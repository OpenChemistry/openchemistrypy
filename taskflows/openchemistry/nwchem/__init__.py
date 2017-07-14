import cumulus
from cumulus.taskflow import TaskFlow
from cumulus.taskflow.cluster import create_girder_client
from cumulus.tasks.job import (download_job_input_folders,
                               upload_job_output_to_folder)
from cumulus.tasks.job import submit_job, monitor_job

from girder.api.rest import getCurrentUser
from girder.constants import AccessType
from girder.utility.model_importer import ModelImporter

from jsonpath_rw import parse
import os
import datetime
import json

def _jsonpath(path, json):
    values = [x.value for x in parse(path).find(json)]
    if len(values) != 1:
        raise Exception('Path did not resolve to single property.')

    return values[0]


class NWChemTaskFlow(TaskFlow):
    """
    {
        "input": {
            "calculation": {
                "_id": <the id of the pending calculation>
            },
        },
        "cluster": {
            "_id": <id of cluster to run on>
        }
    }
    """

    def start(self, *args, **kwargs):
        user = getCurrentUser()
        input_ = kwargs.get('input')
        cluster = kwargs.get('cluster')

        if input_ is None:
            raise Exception('Unable to extract input.')

        if cluster is None:
            raise Exception('Unable to extract cluster.')

        cluster_id = parse('cluster._id').find(kwargs)
        if cluster_id:
            cluster_id = cluster_id[0].value
            model = ModelImporter.model('cluster', 'cumulus')
            cluster = model.load(cluster_id, user=user, level=AccessType.ADMIN)
            cluster = model.filter(cluster, user, passphrase=False)
        else:
            raise Exception('Cluster don\'t contain _id.')

        super(NWChemTaskFlow, self).start(
            setup_input.s(input_, cluster),
            *args, **kwargs)

def _get_oc_folder(client):
    me = client.get('user/me')
    if me is None:
        raise Exception('Unable to get me.')

    login = me['login']
    private_folder_path =    'user/%s/Private' % login
    private_folder = client.resourceLookup(private_folder_path)
    oc_folder_path = '%s/oc' % private_folder_path
    oc_folder = client.resourceLookup(oc_folder_path, test=True)
    if oc_folder is None:
        oc_folder = client.createFolder(private_folder['_id'], 'oc')

    return oc_folder

@cumulus.taskflow.task
def setup_input(task, input_, cluster):
    client = create_girder_client(
        task.taskflow.girder_api_url, task.taskflow.girder_token)

    oc_folder = _get_oc_folder(client)
    run_folder = client.createFolder(oc_folder['_id'],
                                     datetime.datetime.now().strftime("%Y_%m_%d-%H_%M_%S"))
    input_folder = client.createFolder(run_folder['_id'],
                                       'input')


    input_file_path = os.path.join(os.path.dirname(__file__), 'ch3br.nw')
    size = os.path.getsize(input_file_path)
    name = 'oc.nw'
    with open(input_file_path) as fp:
        input_file = client.uploadFile(input_folder['_id'],  fp, name, size,
                                       parentType='folder')

    submit.delay(input_, cluster, run_folder, input_file, input_folder)


def _create_job(task, input_file, input_folder):
    task.taskflow.logger.info('Create NWChem job.')

    body = {
        'name': 'nwchem_run',
        'commands': [
            "mpiexec -n %s nwchem %s" % (
                10,
                input_file['name'])
        ],
        'input': [
            {
              'folderId': input_folder['_id'],
              'path': '.'
            }
        ],
        'output': [],
        'params': {
            'numberOfSlots': 10
        }
    }

    client = create_girder_client(
                task.taskflow.girder_api_url, task.taskflow.girder_token)

    job = client.post('jobs', data=json.dumps(body))
    task.taskflow.set_metadata('jobs', [job])

    return job

@cumulus.taskflow.task
def submit(task, input_, cluster, run_folder, input_file, input_folder):
    job = _create_job(task, input_file, input_folder)

    girder_token = task.taskflow.girder_token
    task.taskflow.set_metadata('cluster', cluster)

    # Now download and submit job to the cluster
    task.taskflow.logger.info('Downloading input files to cluster.')
    download_job_input_folders(cluster, job,
                               girder_token=girder_token, submit=False)
    task.taskflow.logger.info('Downloading complete.')

    task.taskflow.logger.info('Submitting job %s to cluster.' % job['_id'])
    girder_token = task.taskflow.girder_token

    try:
        submit_job(cluster, job, girder_token=girder_token, monitor=False)
    except:
        import traceback
        traceback.print_exc()

    monitor_job.apply_async((cluster, job), {'girder_token': girder_token,
                                             'monitor_interval': 30},
                            link=postprocess.s(run_folder, cluster, job))

@cumulus.taskflow.task
def postprocess(task, _, run_folder, cluster, job):
    task.taskflow.logger.info('Uploading results from cluster')

    client = create_girder_client(
        task.taskflow.girder_api_url, task.taskflow.girder_token)

    output_folder = client.createFolder(run_folder['_id'],
                                       'output')
    # Refresh state of job
    job = client.get('jobs/%s' % job['_id'])
    job['output'] = [{
        'folderId': output_folder['_id'],
        'path': '.'
    }]

    upload_job_output_to_folder(cluster, job, girder_token=task.taskflow.girder_token)

    task.taskflow.logger.info('Upload job output complete.')

    # Call to ingest the files

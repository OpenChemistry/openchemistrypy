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
from io import BytesIO
import jinja2
import tempfile

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

    calculation_id = parse('calculation._id').find(input_)
    if not calculation_id:
        raise Exception('Unable to extract calculation id.')
    calculation_id = calculation_id[0].value

    # Fetch the geometry
    calculation = client.get('calculations/%s' % calculation_id)
    molecule_id = calculation['moleculeId']
    r = client.get('molecules/%s/xyz' % molecule_id, jsonResp=False)
    xyz = r.content
    print(xyz)
    oc_folder = _get_oc_folder(client)
    run_folder = client.createFolder(oc_folder['_id'],
                                     datetime.datetime.now().strftime("%Y_%m_%d-%H_%M_%S"))
    input_folder = client.createFolder(run_folder['_id'],
                                       'input')

    # Generate input file
    params = {
        # For now we always need an energy calculation first
        'energy': True
    }
    calculation_types = parse('properties.calculationTypes').find(calculation)
    if calculation_types:
        calculation_types = calculation_types[0].value

    for calculation_type in calculation_types:
        params[calculation_type] = True

    basis = parse('properties.basis').find(calculation)
    if basis:
        params['basis'] = basis[0].value

    theory = parse('properties.theory').find(calculation)
    if theory:
        params['theory'] = theory[0].value.upper()

    theory = parse('properties.theory').find(calculation)

    template_path = os.path.dirname(__file__)
    jinja2_env = jinja2.Environment(loader=jinja2.FileSystemLoader(template_path),
                             trim_blocks=True)
    with tempfile.TemporaryFile() as fp:
        jinja2_env.get_template('oc.nw.j2').stream(**params).dump(fp, encoding='utf8')
        # Get the size of the file
        size = fp.seek(0, 2)
        fp.seek(0)
        name = 'oc.nw'
        input_file = client.uploadFile(input_folder['_id'],  fp, name, size,
                                       parentType='folder')
    # Upload the xyz file
    size = len(xyz)
    client.uploadFile(input_folder['_id'], BytesIO(xyz), 'geometry.xyz', size,
                      parentType='folder')

    submit.delay(input_, cluster, run_folder, input_file, input_folder)


def _create_job(task, input_file, input_folder):
    task.taskflow.logger.info('Create NWChem job.')

    body = {
        'name': 'nwchem_run',
        'commands': [
            'docker pull openchemistry/nwchem-json:latest',
            'docker run -v $(pwd):/data openchemistry/nwchem-json:latest %s' % (
                input_file['name'])
            #'cp -r /home/test/597b90b6f6571037648d575a/* .'
        ],
        'input': [
            {
              'folderId': input_folder['_id'],
              'path': '.'
            }
        ],
        'output': [],
        'params': {
            'taskFlowId': task.taskflow.id
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
                            link=postprocess.s(run_folder, input_, cluster, job))

@cumulus.taskflow.task
def postprocess(task, _, run_folder, input_, cluster, job):
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

    input_file_name = task.taskflow.get_metadata('inputFileName')
    input_file_name
    # Call to ingest the files
    for item in client.listItem(output_folder['_id']):
        if item['name'].endswith('.json'):
            files = list(client.listFile(item['_id']))
            if len(files) != 1:
                raise Exception('Expecting a single file under item, found: %s' + len(files))

            json_output_file_id = files[0]['_id']
            # Now call endpoint to ingest result
            body = {
                'calculationId': input_['calculation']['_id'],
                'fileId': json_output_file_id,
                'public': True
            }

            client.post('molecules', json=body)

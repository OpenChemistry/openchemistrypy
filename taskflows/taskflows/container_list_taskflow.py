import cumulus
from cumulus.taskflow import TaskFlow
from cumulus.taskflow import logging
from cumulus.taskflow.cluster import create_girder_client
from cumulus.tasks.job import monitor_job, submit_job
from cumulus.tasks.job import (
    download_job_input_folders, upload_job_output_to_folder
)

from girder_client import HttpError
from girder.api.rest import getCurrentUser
from girder.constants import AccessType
from girder.utility.model_importer import ModelImporter

from .utils import (
    get_cori, get_oc_folder, log_and_raise, is_demo, is_nersc, countdown,
    post_image_to_database
)

import datetime
import io
import json
from jsonpath_rw import parse
import os
import re

STATUS_LEVEL = logging.INFO + 5
PYTHON_FILE = 'list.py'
OUTPUT_FILE = 'list.json'

# Only these repositories will be registered automatically
REPOSITORIES_TO_REGISTER = [
    'openchemistry/chemml',
    'openchemistry/cp2k',
    'openchemistry/nwchem',
    'openchemistry/psi4',
    'openchemistry/torchani'
]

class ContainerListTaskFlow(TaskFlow):
    """
    {
        "cluster": {
            "_id": <id of cluster to run on>
        },
        'container': <container technology to be used: docker | singularity | shifter>
    }
    """

    def start(self, *args, **kwargs):
        user = getCurrentUser()
        cluster = kwargs.get('cluster')
        container = kwargs.get('container')

        if cluster is None:
            raise Exception('Unable to extract cluster.')

        if '_id' not in cluster and 'name' not in cluster:
            raise Exception('Unable to extract cluster.')

        if container is None:
            raise Exception('Unable to extract container type.')

        cluster_id = parse('cluster._id').find(kwargs)
        if cluster_id:
            cluster_id = cluster_id[0].value
            model = ModelImporter.model('cluster', 'cumulus')
            cluster = model.load(cluster_id, user=user, level=AccessType.ADMIN)

        super(ContainerListTaskFlow, self).start(
            start.s(user, cluster, container),
            *args, **kwargs)


def _get_job_parameters(task, cluster, container):
    setup_commands = []
    job_parameters = {
        'taskFlowId': task.taskflow.id
    }

    # Override default parameters depending on the cluster we are running on
    if is_nersc(cluster):
        # NERSC specific options
        job_parameters.update({
            'numberOfNodes': 1,
            'queue': 'debug',
            'constraint': 'haswell',
            'account': os.environ.get('OC_ACCOUNT')
        })
    elif is_demo(cluster):
        # DEV/DEMO environment options
        setup_commands = ['source scl_source enable python27']
    else:
        # baremetal options
        pass

    return {
        'container': container,
        'setupCommands': setup_commands,
        'jobParameters': job_parameters
    }


def _create_job(task, cluster, folder, container):
    params = _get_job_parameters(task, cluster, container)
    setup_commands = params['setupCommands']
    job_parameters = params['jobParameters']

    run_command = 'python %s -c %s' % (PYTHON_FILE, container)
    commands = setup_commands + [
        run_command,
        'rm %s' % PYTHON_FILE
    ]

    body = {
        # ensure there are no special characters in the submission script name
        'name': 'desc_%s' % re.sub('[^a-zA-Z0-9]', '_', run_command),
        'commands': commands,
        'input': [
            {
              'folderId': folder['_id'],
              'path': '.'
            }
        ],
        'output': [
            {
              'folderId': folder['_id'],
              'path': '.'
            }
        ],
        'uploadOutput': False,
        'params': job_parameters
    }

    client = create_girder_client(
                task.taskflow.girder_api_url, task.taskflow.girder_token)

    job = client.post('jobs', data=json.dumps(body))

    return job


@cumulus.taskflow.task
def start(task, user, cluster, container):
    client = create_girder_client(
        task.taskflow.girder_api_url, task.taskflow.girder_token)

    if cluster.get('name') == 'cori':
        cluster = get_cori(client)

    if '_id' not in cluster:
        log_and_raise(task, 'Invalid cluster configurations: %s' % cluster)

    oc_folder = get_oc_folder(client)
    root_folder = client.createFolder(oc_folder['_id'],
                                      datetime.datetime.now().strftime("%Y_%m_%d-%H_%M_%f"))
    # temporary folder to save the container in/out
    folder = client.createFolder(root_folder['_id'], 'list_folder')

    # save the list.py script to the job directory
    with open(os.path.join(os.path.dirname(__file__), 'utils/%s' % PYTHON_FILE), 'rb') as f:
        # Get the size of the file
        size = f.seek(0, 2)
        f.seek(0)
        client.uploadFile(folder['_id'], f, PYTHON_FILE, size, parentType='folder')

    job = _create_job(task, cluster, folder, container)

    # Now download list.py script to the cluster
    task.taskflow.logger.info('Preparing job to list the images.')
    download_job_input_folders(cluster, job,
                               girder_token=task.taskflow.girder_token,
                               submit=False)

    task.taskflow.logger.info('Submitting job to list the images.')
    submit_job(cluster, job, girder_token=task.taskflow.girder_token,
               monitor=False)

    monitor_job.apply_async(
        (cluster, job), {'girder_token': task.taskflow.girder_token,
                         'monitor_interval': 10},
        countdown=countdown(cluster),
        link=postprocess_job.s(user, cluster, job, folder, container))


@cumulus.taskflow.task
def postprocess_job(task, _, user, cluster, job, folder, container):
    task.taskflow.logger.info('Finished listing the images')

    task.taskflow.logger.info('Processing the results of the list')
    client = create_girder_client(
        task.taskflow.girder_api_url, task.taskflow.girder_token)

    # Refresh state of job
    job = client.get('jobs/%s' % job['_id'])

    upload_job_output_to_folder(cluster, job, girder_token=task.taskflow.girder_token)

    output_items = list(client.listItem(folder['_id']))
    output_file = None
    for item in output_items:
        if item['name'] == OUTPUT_FILE:
            files = list(client.listFile(item['_id']))
            if len(files) != 1:
                log_and_raise(task, 'Expecting a single file under item, found: %s' % len(files))
            output_file = files[0]
            break

    if output_file is None:
        log_and_raise(task, 'Could not locate output file: %s' % OUTPUT_FILE)

    # Download the output file
    output_io = io.BytesIO()
    client.downloadFile(output_file['_id'], output_io)

    # Remove the folder
    client.delete('folder/%s' % folder['_id'])

    list_json = json.loads(output_io.getvalue().decode('utf-8'))

    for image in list_json:
        repository = image.get('repository')
        if repository not in REPOSITORIES_TO_REGISTER:
            continue

        tag = image.get('tag')
        digest = image.get('digest')
        # Convert size to GB
        size = round(image.get('size', 0) / 1.e9, 2)

        post_image_to_database(client, container, repository, tag, digest,
                               cluster, size)

    task.taskflow.logger.info('Success!')

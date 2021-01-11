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
OUTPUT_FILE = 'pull.json'


class ContainerPullTaskFlow(TaskFlow):
    """
    {
        "cluster": {
            "_id": <id of cluster to run on>
        },
        "image": {
            'repository': <the image repository, e.g. "openchemistry/psi4">
            'tag': <the image tag, e.g. "latest">
        },
        'container': <container technology to be used: docker | singularity>
    }
    """

    def start(self, *args, **kwargs):
        user = getCurrentUser()
        cluster = kwargs.get('cluster')
        image = kwargs.get('image')
        container = kwargs.get('container')

        if cluster is None:
            raise Exception('Unable to extract cluster.')

        if '_id' not in cluster and 'name' not in cluster:
            raise Exception('Unable to extract cluster.')

        if image is None:
            raise Exception('Unable to extract the image name.')

        if container is None:
            raise Exception('Unable to extract container type.')

        cluster_id = parse('cluster._id').find(kwargs)
        if cluster_id:
            cluster_id = cluster_id[0].value
            model = ModelImporter.model('cluster', 'cumulus')
            cluster = model.load(cluster_id, user=user, level=AccessType.ADMIN)

        super(ContainerPullTaskFlow, self).start(
            start.s(user, cluster, image, container),
            *args, **kwargs)


def _get_job_parameters(task, cluster, image, container):
    repository = image.get('repository')
    tag = image.get('tag')
    digest = image.get('digest')
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
        'repository': repository,
        'tag': tag,
        'digest': digest,
        'setupCommands': setup_commands,
        'jobParameters': job_parameters
    }


def _create_job(task, cluster, folder, image, container):
    params = _get_job_parameters(task, cluster, image, container)
    setup_commands = params['setupCommands']
    repository = params['repository']
    tag = params['tag']
    job_parameters = params['jobParameters']

    run_command = 'python pull.py -r %s -t %s -c %s' % (
        repository, tag, container)
    commands = setup_commands + [
        run_command,
        'rm pull.py'
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
def start(task, user, cluster, image, container):
    client = create_girder_client(
        task.taskflow.girder_api_url, task.taskflow.girder_token)

    if cluster.get('name') == 'cori':
        cluster = get_cori(client)

    if '_id' not in cluster:
        log_and_raise(task, 'Invalid cluster configurations: %s' % cluster)

    oc_folder = get_oc_folder(client)
    root_folder = client.createFolder(oc_folder['_id'],
                                      datetime.datetime.now().strftime("%Y_%m_%d-%H_%M_%f"))
    # temporary folder to save the container in/out description
    folder = client.createFolder(root_folder['_id'], 'pull_folder')

    # save the pull.py script to the job directory
    with open(os.path.join(os.path.dirname(__file__), 'utils/pull.py'), 'rb') as f:
        # Get the size of the file
        size = f.seek(0, 2)
        f.seek(0)
        name = 'pull.py'
        client.uploadFile(folder['_id'], f, name, size, parentType='folder')

    job = _create_job(task, cluster, folder, image, container)

    # Now download pull.py script to the cluster
    task.taskflow.logger.info('Preparing job to pull the container.')
    download_job_input_folders(cluster, job,
                               girder_token=task.taskflow.girder_token,
                               submit=False)

    task.taskflow.logger.info('Submitting job to pull the container.')
    submit_job(cluster, job, girder_token=task.taskflow.girder_token,
               monitor=False)

    monitor_job.apply_async(
        (cluster, job), {'girder_token': task.taskflow.girder_token,
                         'monitor_interval': 10},
        countdown=countdown(cluster),
        link=postprocess_job.s(user, cluster, image, job, folder, container))


@cumulus.taskflow.task
def postprocess_job(task, _, user, cluster, image, job, folder, container):
    task.taskflow.logger.info('Finished pulling the container')

    task.taskflow.logger.info('Processing the results of the pull.')
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

    pull_json = json.loads(output_io.getvalue().decode('utf-8'))
    image_uri = pull_json.get('imageUri')
    # Convert size to GB
    size = round(pull_json.get('size', 0) / 1.e9, 2)

    _ensure_image_uri_is_valid(task, container, image_uri)

    repository = image.get('repository')
    tag = image.get('tag')
    digest = _extract_digest(container, image_uri)

    post_image_to_database(client, container, repository, tag, digest,
                           cluster, size)

    task.taskflow.logger.info('Success!')


def _ensure_image_uri_is_valid(task, container, image_uri):
    if not image_uri:
        log_and_raise(task, 'Image uri is empty')

    # Raises an exception if the uri is not valid
    if container == 'singularity':
        if '.sif' not in image_uri:
            log_and_raise(task, 'Invalid image uri: ' + str(image_uri))
    else:
        if ':' not in image_uri:
            log_and_raise(task, 'Invalid image uri: ' + str(image_uri))


def _extract_digest(container, image_uri):
    if container == 'singularity':
        # Get the hashsum from the file path
        return os.path.basename(image_uri).replace('.sif', '')
    else:
        return image_uri.split(':')[1]

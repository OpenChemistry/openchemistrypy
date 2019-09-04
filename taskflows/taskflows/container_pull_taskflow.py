import cumulus
from cumulus.taskflow import TaskFlow
from cumulus.taskflow import logging
from cumulus.taskflow.cluster import create_girder_client
from cumulus.tasks.job import submit_job, monitor_job

from girder.api.rest import getCurrentUser
from girder.constants import AccessType
from girder.utility.model_importer import ModelImporter

from .utils import get_cori, image_to_sif, log_and_raise

import os
import json
from jsonpath_rw import parse
import re

STATUS_LEVEL = logging.INFO + 5


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
            raise Exception('Unable to extract the docker image name.')

        if container is None:
            raise Exception('Unable to extract container name.')

        cluster_id = parse('cluster._id').find(kwargs)
        if cluster_id:
            cluster_id = cluster_id[0].value
            model = ModelImporter.model('cluster', 'cumulus')
            cluster = model.load(cluster_id, user=user, level=AccessType.ADMIN)

        super(ContainerPullTaskFlow, self).start(
            start.s(user, cluster, image, container),
            *args, **kwargs)


def _nersc(cluster):
    return cluster.get('name') in ['cori']


def _demo(cluster):
    return cluster.get('name') == 'demo_cluster'


def _countdown(cluster):
    """
    Returns the number of seconds the monitoring task should be delayed before
    running based on the cluster we are using.
    """
    countdown = 0
    # If we are running at NERSC our job states are cached for 60 seconds,
    # so they are potentially 60 seconds out of date, so we have to wait
    # at least 60 seconds before we can assume that the job is complete if its
    # nolonger in the queue, so we delay our monitoring
    if _nersc(cluster):
        countdown = 65

    return countdown


def _get_job_parameters(task, cluster, image, container):
    repository = image.get('repository')
    tag = image.get('tag')
    digest = image.get('digest')
    setup_commands = []
    job_parameters = {
        'taskFlowId': task.taskflow.id
    }

    # Override default parameters depending on the cluster we are running on
    if _nersc(cluster):
        # NERSC specific options
        job_parameters.update({
            'numberOfNodes': 1,
            'queue': 'debug',
            'constraint': 'haswell',
            'account': os.environ.get('OC_ACCOUNT')
        })
    elif _demo(cluster):
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


def _create_job(task, cluster, image, container):
    params = _get_job_parameters(task, cluster, image, container)
    setup_commands = params['setupCommands']
    repository = params['repository']
    tag = params['tag']
    job_parameters = params['jobParameters']

    image_name = '%s:%s' % (repository, tag)
    if container == 'singularity':
        # Include the path to the singularity dir, and the extension
        image_str = image_to_sif(image_name)

        # Make sure the directory exists where we will put the sif file
        setup_commands.append('mkdir -p ' + os.path.dirname(image_str))

        # Force an overwite of the image if it already exists
        build_options = ['-F']

        docker_uri = 'docker://%s' % image_name
        run_command = 'singularity build ' + ' '.join(build_options)
        run_command += ' %s %s' % (image_str, docker_uri)
    else:
        run_command = '%s pull %s' % (container, image_name)

    commands = setup_commands + [run_command]

    body = {
        # ensure there are no special characters in the submission script name
        'name': 'desc_%s' % re.sub('[^a-zA-Z0-9]', '_', run_command),
        'commands': commands,
        # Even though we have no output, this must still be included
        # for some reason. It needs to be a list.
        'output': [],
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

    job = _create_job(task, cluster, image, container)

    task.taskflow.logger.info('Submitting job to pull the container.')
    submit_job(cluster, job, girder_token=task.taskflow.girder_token,
               monitor=False)

    monitor_job.apply_async(
        (cluster, job), {'girder_token': task.taskflow.girder_token,
                         'monitor_interval': 10},
        countdown=_countdown(cluster),
        link=postprocess_job.s(user, cluster, image, job))


@cumulus.taskflow.task
def postprocess_job(task, _, user, cluster, image, job):
    task.taskflow.logger.info('Finished pulling the container')

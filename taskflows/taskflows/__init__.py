import cumulus
from cumulus.taskflow import TaskFlow
from cumulus.taskflow import logging
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
import tempfile
import re

from .utils import cjson_to_xyz

STATUS_LEVEL = logging.INFO + 5

class OpenChemistryTaskFlow(TaskFlow):
    """
    {
        "input": {
            "calculation": {
                "_id": <the id of the pending calculation>
            },
        },
        "cluster": {
            "_id": <id of cluster to run on>
        },
        "image": {
            'repository': <the image repository, e.g. "openchemistry/psi4">
            'tag': <the image tag, e.g. "latest">
        },
        "runParameters": {
            'container': <the container technology to be used: docker | singularity>,
            'keepScratch': <whether to save the calculation raw output: default False>
        }
    }
    """

    def start(self, *args, **kwargs):
        user = getCurrentUser()
        input_ = kwargs.get('input')
        cluster = kwargs.get('cluster')
        image = kwargs.get('image')
        run_parameters = kwargs.get('runParameters')

        if input_ is None:
            raise Exception('Unable to extract input.')

        if '_id' not in cluster and 'name' not in cluster:
            raise Exception('Unable to extract cluster.')

        if image is None:
            raise Exception('Unable to extract the docker image name.')

        if run_parameters is None:
            run_parameters = {}

        cluster_id = parse('cluster._id').find(kwargs)
        if cluster_id:
            cluster_id = cluster_id[0].value
            model = ModelImporter.model('cluster', 'cumulus')
            cluster = model.load(cluster_id, user=user, level=AccessType.ADMIN)
            cluster = model.filter(cluster, user, passphrase=False)

        super(OpenChemistryTaskFlow, self).start(
            start.s(input_, cluster, image, run_parameters),
            *args, **kwargs)

def _get_cori(client):
    params = {
        'type': 'newt'
    }
    clusters = client.get('clusters', parameters=params)
    for cluster in clusters:
        if cluster['name'] == 'cori':
            return cluster

    # We need to create one
    body = {
        'config': {
            'host': 'cori'
        },
        'name': 'cori',
        'type': 'newt'
    }
    cluster = client.post('clusters', data=json.dumps(body))

    return cluster

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
def start(task, input_, cluster, image, run_parameters):
    """
    The flow is the following:
    - Dry run the container with the -d flag to obtain a description of the input/output formats
    - Convert the cjson input geometry to conform to the container's expected format
    - Run the container
    - Convert the container output format into cjson
    - Ingest the output in the database
    """
    client = create_girder_client(
        task.taskflow.girder_api_url, task.taskflow.girder_token)

    if cluster.get('name') == 'cori':
        cluster = _get_cori(client)

    if '_id' not in cluster:
        _log_and_raise(task, 'Invalid cluster configurations: %s' % cluster)

    oc_folder = _get_oc_folder(client)
    root_folder = client.createFolder(oc_folder['_id'],
                                    datetime.datetime.now().strftime("%Y_%m_%d-%H_%M_%f"))
    # temporary folder to save the container in/out description
    description_folder = client.createFolder(root_folder['_id'],
                                    'description')

    # save the pull.py script to the job directory
    with open(os.path.join(os.path.dirname(__file__), 'utils/pull.py'), 'rb') as f:
        # Get the size of the file
        size = f.seek(0, 2)
        f.seek(0)
        name = 'pull.py'
        input_parameters_file = client.uploadFile(description_folder['_id'],  f, name, size,
                                    parentType='folder')

    job = _create_description_job(task, cluster, description_folder, image, run_parameters)

    # Now download pull.py script to the cluster
    task.taskflow.logger.info('Preparing job to obtain the container description.')
    download_job_input_folders(cluster, job,
                               girder_token=task.taskflow.girder_token, submit=False)
    task.taskflow.logger.info('Submitting job to obtain the container description.')

    submit_job(cluster, job, girder_token=task.taskflow.girder_token, monitor=False)

    monitor_job.apply_async((cluster, job), {'girder_token': task.taskflow.girder_token,
                                             'monitor_interval': 10},
                            link=postprocess_description.s(input_, cluster, image, run_parameters, root_folder, job, description_folder))

def _get_job_parameters(cluster, image, run_parameters):
    container = run_parameters.get('container', 'docker') # docker | singularity
    repository = image.get('repository')
    tag = image.get('tag')
    digest = image.get('digest')
    image_uri = image.get('imageUri')
    host_dir = '$(pwd)' # the host directory being mounted into the container
    guest_dir = '/data' # the directory inside the container pointing to host_dir
    job_dir = '' # relative path from guest_dir to the job directory
    setup_commands = []

    # Override default parameters depending on the cluster we are running on

    if _nersc(cluster):
        # NERSC specific options
        container = 'singularity' # no root access, only singularity is supported
        raise NotImplementedError('Cannot run on NERSC yet')
    elif _demo(cluster):
        # DEV/DEMO environment options
        host_dir = 'dev_job_data'
        job_dir = '{{job._id}}'
        setup_commands = ['source scl_source enable python27']
    else:
        # baremetal options
        pass

    return {
        'container': container,
        'imageUri': image_uri,
        'repository': repository,
        'tag': tag,
        'digest': digest,
        'hostDir': host_dir,
        'guestDir': guest_dir,
        'jobDir': job_dir,
        'setupCommands': setup_commands
    }

def _create_description_job(task, cluster, description_folder, image, run_parameters):
    params = _get_job_parameters(cluster, image, run_parameters)
    container = params['container']
    setup_commands = params['setupCommands']
    repository = params['repository']
    tag = params['tag']

    job_params = {
        'taskFlowId': task.taskflow.id
    }

    output_file = 'description.json'

    commands = setup_commands + [
        'IMAGE_NAME=$(python pull.py -r %s -t %s -c %s | tail -1)' % (repository, tag, container),
        '%s run $IMAGE_NAME -d > %s' % (container, output_file),
        'rm pull.py'
    ]

    body = {
        # ensure there are no special characters in the submission script name
        'name': 'desc_%s' % re.sub('[^a-zA-Z0-9]', '_', repository),
        'commands': commands,
        'input': [
            {
              'folderId': description_folder['_id'],
              'path': '.'
            }
        ],
        'output': [
            {
              'folderId': description_folder['_id'],
              'path': '.'
            }
        ],
        'uploadOutput': False,
        'params': job_params
    }

    client = create_girder_client(
                task.taskflow.girder_api_url, task.taskflow.girder_token)

    job = client.post('jobs', data=json.dumps(body))

    return job

@cumulus.taskflow.task
def postprocess_description(task, _, input_, cluster, image, run_parameters, root_folder, description_job, description_folder):
    task.taskflow.logger.info('Processing the output of the container description job.')

    client = create_girder_client(
        task.taskflow.girder_api_url, task.taskflow.girder_token)

    # Refresh state of job
    description_job = client.get('jobs/%s' % description_job['_id'])

    upload_job_output_to_folder(cluster, description_job, girder_token=task.taskflow.girder_token)

    description_items = list(client.listItem(description_folder['_id']))

    description_file = None
    pull_file = None
    for item in description_items:
        if item['name'] == 'description.json':
            files = list(client.listFile(item['_id']))
            if len(files) != 1:
                _log_std_err(task, client, description_folder)
                _log_and_raise(task, 'Expecting a single file under item, found: %s' % len(files))
            description_file = files[0]

        elif item['name'] == 'pull.json':
            files = list(client.listFile(item['_id']))
            if len(files) != 1:
                _log_std_err(task, client, description_folder)
                _log_and_raise(task, 'Expecting a single file under item, found: %s' % len(files))
            pull_file = files[0]

    if pull_file is None:
        _log_std_err(task, client, description_folder)
        _log_and_raise(task, 'There was an error trying to pull the requested container image')

    if description_file is None:
        _log_std_err(task, client, description_folder)
        _log_and_raise(task, 'The container does not implement correctly the --description flag')

    with tempfile.TemporaryFile() as tf:
        client.downloadFile(pull_file['_id'], tf)
        tf.seek(0)
        container_pull = json.loads(tf.read().decode())

    image = container_pull

    with tempfile.TemporaryFile() as tf:
        client.downloadFile(description_file['_id'], tf)
        tf.seek(0)
        container_description = json.loads(tf.read().decode())

    # Add code name and version to the taskflow metadata
    code = {
        'name': container_description.get('name'),
        'version': container_description.get('version')
    }
    task.taskflow.set_metadata('code', code)

    # remove temporary description folder
    client.delete('folder/%s' % description_folder['_id'])

    setup_input.delay(input_, cluster, image, run_parameters, root_folder, container_description)

@cumulus.taskflow.task
def setup_input(task, input_, cluster, image, run_parameters, root_folder, container_description):
    task.taskflow.logger.info('Setting up the calculation input files.')

    client = create_girder_client(
        task.taskflow.girder_api_url, task.taskflow.girder_token)

    if cluster.get('name') == 'cori':
        cluster = _get_cori(client)

    if '_id' not in cluster:
        _log_and_raise(task, 'Invalid cluster configurations: %s' % cluster)

    calculation_id = parse('calculation._id').find(input_)
    if not calculation_id:
        _log_and_raise(task, 'Unable to extract calculation id.')

    calculation_id = calculation_id[0].value
    calculation = client.get('calculations/%s' % calculation_id)
    molecule_id = calculation['moleculeId']

    input_parameters = calculation.get('input', {}).get('parameters', {})

    # Fetch the starting geometry
    input_geometry = calculation.get('input', {}).get('geometry', None)
    if input_geometry is None:
        r = client.get('molecules/%s/cjson' % molecule_id, jsonResp=False)
        cjson = r.json()
    else:
        # TODO: implement the path where a specific input geometry exists
        raise NotImplementedError('Running a calculation with a specific geometry is not implemented yet.')

    input_format = container_description['input']['format']
    output_format = container_description['output']['format']

    # The folder where the input geometry and input parameters are
    input_folder = client.createFolder(root_folder['_id'], 'input')
    # The folder where the converted output will be at the end of the job
    output_folder = client.createFolder(root_folder['_id'], 'output')
    # The folder where the raw input/output files of the specific code are stored
    scratch_folder = client.createFolder(root_folder['_id'], 'scratch')
    # The folder where the cluster stdout and stderr is saved
    run_folder = client.createFolder(root_folder['_id'], 'run')

    # Save the input parameters to file
    with tempfile.TemporaryFile() as fp:
        fp.write(json.dumps(input_parameters).encode())
        # Get the size of the file
        size = fp.seek(0, 2)
        fp.seek(0)
        name = 'input_parameters.json'
        input_parameters_file = client.uploadFile(input_folder['_id'],  fp, name, size,
                                    parentType='folder')

    # Save the input geometry to file
    with tempfile.TemporaryFile() as fp:
        content = _convert_geometry(task, cjson, input_format)
        fp.write(content.encode())
        # Get the size of the file
        size = fp.seek(0, 2)
        fp.seek(0)
        name = 'geometry.%s' % input_format
        input_geometry_file = client.uploadFile(input_folder['_id'],  fp, name, size,
                                    parentType='folder')

    submit_calculation.delay(input_, cluster, image, run_parameters, root_folder, container_description, input_folder, output_folder, scratch_folder, run_folder)

def _convert_geometry(task, cjson, input_format):
    if input_format.lower() == 'xyz':
        return cjson_to_xyz(cjson)
    elif input_format.lower() == 'cjson':
        return json.dumps(cjson)
    else:
        _log_and_raise(task, 'The container is requesting an unsupported geometry format %s' % input_format)

def _create_job(task, cluster, image, run_parameters, container_description, input_folder, output_folder, scratch_folder, run_folder):
    params = _get_job_parameters(cluster, image, run_parameters)
    container = params['container']
    image_uri = params['imageUri']
    repository = params['repository']
    digest = params['digest']
    host_dir = params['hostDir']
    guest_dir = params['guestDir']
    job_dir = params['jobDir']

    task.taskflow.logger.info('Creating %s job' % repository)

    input_format = container_description['input']['format']
    output_format = container_description['output']['format']

    input_dir = os.path.join(guest_dir, job_dir, 'input')
    output_dir = os.path.join(guest_dir, job_dir, 'output')
    scratch_dir = os.path.join(guest_dir, job_dir, 'scratch')

    geometry_filename = os.path.join(input_dir, 'geometry.%s' % input_format)
    parameters_filename = os.path.join(input_dir, 'input_parameters.json')
    output_filename = os.path.join(output_dir, 'output.%s' % output_format)

    output = [
        {
            'folderId': output_folder['_id'],
            'path': './output'
        },
        {
            'folderId': run_folder['_id'],
            'path': '.',
            'exclude': [
                'input',
                'output',
                'scratch'
            ]
        }
    ]

    keep_scratch = run_parameters.get('keepScratch', False)
    if keep_scratch:
        output.append({
            'folderId': scratch_folder['_id'],
            'path': './scratch'
        })

    commands = [
        'mkdir output',
        'mkdir scratch'
    ]

    if container == 'docker':
        mount_option = '-v %s:%s' % (host_dir, guest_dir)
        # In the docker case we need to ensure the image has been pull on this
        # node, as the images are not shared across the nodes.
        commands.append('docker pull %s' % image_uri)
    else:
        mount_option = ''

    commands.append('%s run %s %s -g %s -p %s -o %s -s %s' % (
            container, mount_option, image_uri,
            geometry_filename, parameters_filename,
            output_filename, scratch_dir
        ))

    body = {
        # ensure there are no special characters in the submission script name
        'name': 'run_%s' % re.sub('[^a-zA-Z0-9]', '_', repository),
        'commands': commands,
        'input': [
            {
              'folderId': input_folder['_id'],
              'path': './input'
            }
        ],
        'output': output,
        'uploadOutput': False,
        'params': {
            'taskFlowId': task.taskflow.id
        }
    }

    client = create_girder_client(
                task.taskflow.girder_api_url, task.taskflow.girder_token)

    job = client.post('jobs', data=json.dumps(body))
    task.taskflow.set_metadata('jobs', [job])

    return job

def _nersc(cluster):
    return cluster.get('name') in ['cori']

def _demo(cluster):
    return cluster.get('name') == 'demo_cluster'

def _log_and_raise(task, msg):
    _log_error(task, msg)
    raise Exception(msg)

def _log_error(task, msg):
    task.taskflow.logger.error(msg)

def _log_std_err(task, client, run_folder):
    errors = _get_std_err(client, run_folder)
    for e in errors:
        _log_error(task, e)

def _get_std_err(client, run_folder):
    error_regex = re.compile(r'^.*\.e\d*$', re.IGNORECASE)
    output_items = list(client.listItem(run_folder['_id']))
    errors = []
    for item in output_items:
        if error_regex.match(item['name']):
            files = list(client.listFile(item['_id']))
            if len(files) != 1:
                continue
            with tempfile.TemporaryFile() as tf:
                client.downloadFile(files[0]['_id'], tf)
                tf.seek(0)
                contents = tf.read().decode()
                errors.append(contents)
    return errors

@cumulus.taskflow.task
def submit_calculation(task, input_, cluster, image, run_parameters, root_folder, container_description, input_folder, output_folder, scratch_folder, run_folder):
    job = _create_job(task, cluster, image, run_parameters, container_description, input_folder, output_folder, scratch_folder, run_folder)

    girder_token = task.taskflow.girder_token
    task.taskflow.set_metadata('cluster', cluster)

    # Now download and submit job to the cluster
    task.taskflow.logger.info('Uploading the input files to the cluster.')
    download_job_input_folders(cluster, job,
                               girder_token=girder_token, submit=False)

    task.taskflow.logger.info('Submitting the calculation job %s to the queue.' % job['_id'])

    submit_job(cluster, job, girder_token=girder_token, monitor=False)

    monitor_job.apply_async((cluster, job), {'girder_token': girder_token,
                                             'monitor_interval': 10},
                            link=postprocess_job.s(input_, cluster, image, run_parameters, root_folder, container_description, input_folder, output_folder, scratch_folder, run_folder, job))

@cumulus.taskflow.task
def postprocess_job(task, _, input_, cluster, image, run_parameters, root_folder, container_description, input_folder, output_folder, scratch_folder, run_folder, job):
    task.taskflow.logger.info('Processing the results of the calculation.')
    client = create_girder_client(
        task.taskflow.girder_api_url, task.taskflow.girder_token)

    # Refresh state of job
    job = client.get('jobs/%s' % job['_id'])

    upload_job_output_to_folder(cluster, job, girder_token=task.taskflow.girder_token)

    # remove temporary input folder folder, this data is attached to the calculation model
    client.delete('folder/%s' % input_folder['_id'])

    # clean up the scratch folder
    keep_scratch = run_parameters.get('keepScratch', False)
    if keep_scratch:
        scratch_folder_id = scratch_folder['_id']
    else:
        client.delete('folder/%s' % scratch_folder['_id'])
        scratch_folder_id = None

    # ingest the output of the calculation
    output_format = container_description['output']['format']
    output_file = None
    output_items = list(client.listItem(output_folder['_id']))
    for item in output_items:
        if item['name'] == 'output.%s' % output_format:
            files = list(client.listFile(item['_id']))
            if len(files) != 1:
                _log_std_err(task, client, run_folder)
                _log_and_raise(task, 'Expecting a single file under item, found: %s' % len(files))
            output_file = files[0]
            break

    if output_file is None:
        # Log the job stderr
        _log_std_err(task, client, run_folder)
        _log_and_raise(task, 'The calculation did not produce any output file.')

    # remove the run folder, only useful to access the stdout and stderr after the job is done
    client.delete('folder/%s' % run_folder['_id'])

    # Now call endpoint to ingest result
    params = {
        'detectBonds': True
    }

    task.taskflow.logger.info('Uploading the results of the calculation to the database.')

    body = {
        'fileId': output_file['_id'],
        'format': output_format,
        'public': True,
        'image': image, # image now also has a digest field, add it to the calculation
        'scratchFolderId': scratch_folder_id
    }

    client.put('calculations/%s' % input_['calculation']['_id'], parameters=params, json=body)

    task.taskflow.logger.log(STATUS_LEVEL, 'Done!')

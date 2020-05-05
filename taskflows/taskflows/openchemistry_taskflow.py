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

from .utils import (
    digest_to_sif, get_cori, get_oc_folder, log_and_raise, log_std_err,
    is_demo, is_nersc, countdown
)

from jsonpath_rw import parse
import os
import datetime
import json
import tempfile
import re

STATUS_LEVEL = logging.INFO + 5

class OpenChemistryTaskFlow(TaskFlow):
    """
    {
        "input": {
            "calculations": [
                <id of pending calculation #1>
                <id of pending calculation #2>
                ...
            ],
        },
        "cluster": {
            "_id": <id of cluster to run on>
        },
        "image": {
            'repository': <the image repository, e.g. "openchemistry/psi4">
            'tag': <the image tag, e.g. "latest">
        },
        "runParameters": {
            'container': <the container technology to be used: docker | singularity | shifter>,
            'keepScratch': <whether to save the raw output of the calculations: default False>
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
            start.s(input_, user, cluster, image, run_parameters),
            *args, **kwargs)

@cumulus.taskflow.task
def start(task, input_, user, cluster, image, run_parameters):
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
        cluster = get_cori(client)

    if '_id' not in cluster:
        log_and_raise(task, 'Invalid cluster configurations: %s' % cluster)

    oc_folder = get_oc_folder(client)
    root_folder = client.createFolder(oc_folder['_id'],
                                    datetime.datetime.now().strftime("%Y_%m_%d-%H_%M_%f"))
    # temporary folder to save the container in/out description
    description_folder = client.createFolder(root_folder['_id'],
                                    'description')

    job = _create_description_job(task, cluster, description_folder, image, run_parameters)

    task.taskflow.logger.info('Preparing job to obtain the container description.')
    download_job_input_folders(cluster, job,
                               girder_token=task.taskflow.girder_token, submit=False)
    task.taskflow.logger.info('Submitting job to obtain the container description.')

    submit_job(cluster, job, girder_token=task.taskflow.girder_token, monitor=False)

    monitor_job.apply_async((cluster, job), {'girder_token': task.taskflow.girder_token,
                                             'monitor_interval': 10},
                                             countdown=countdown(cluster),
                            link=postprocess_description.s(input_, user, cluster, image, run_parameters, root_folder, job, description_folder))

def _get_job_parameters(task, cluster, image, run_parameters):
    container = run_parameters.get('container', 'docker') # docker | singularity
    repository = image.get('repository')
    tag = image.get('tag')
    digest = image.get('digest')
    host_dir = '$(pwd)' # the host directory being mounted into the container
    guest_dir = '/data' # the directory inside the container pointing to host_dir
    job_dir = '' # relative path from guest_dir to the job directory
    setup_commands = []
    job_parameters = {
        'taskFlowId': task.taskflow.id
    }

    if digest is None:
        digest = _get_digest(task, repository, tag)

    # Override default parameters depending on the cluster we are running on

    if is_nersc(cluster):
        # NERSC specific options
        container = 'shifter' # no root access, only singularity is supported
        job_parameters.update({
            'numberOfNodes': 1,
            'queue': 'debug',
            'constraint': 'haswell',
            'account': os.environ.get('OC_ACCOUNT')
        })
    elif is_demo(cluster):
        # DEV/DEMO environment options
        host_dir = 'dev_job_data'
        job_dir = '{{job._id}}'
        setup_commands = ['source scl_source enable python27']
    else:
        # baremetal options
        pass

    return {
        'container': container,
        'repository': repository,
        'tag': tag,
        'digest': digest,
        'hostDir': host_dir,
        'guestDir': guest_dir,
        'jobDir': job_dir,
        'setupCommands': setup_commands,
        'jobParameters': job_parameters
    }


def _create_description_job(task, cluster, description_folder, image, run_parameters):
    params = _get_job_parameters(task, cluster, image, run_parameters)
    container = params['container']
    setup_commands = params['setupCommands']
    repository = params['repository']
    tag = params['tag']
    digest = params['digest']
    job_parameters = params['jobParameters'];

    # Make sure the image exists on the server before we go further
    _ensure_image_on_server(task, repository, tag, digest, container)

    output_file = 'description.json'

    run_command = '%s run $IMAGE_NAME' % container
    # Shifter has a pretty different sytax so special case it.
    if container == 'shifter':
        run_command = 'shifter --image=$IMAGE_NAME --entrypoint --'

    image_name = '%s:%s' % (repository, tag)
    if container == 'singularity':
        image_name = digest_to_sif(digest)

    commands = setup_commands + [
        'IMAGE_NAME=%s' % image_name,
        '%s -d > %s' % (run_command, output_file)
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
        'params': job_parameters
    }

    client = create_girder_client(
                task.taskflow.girder_api_url, task.taskflow.girder_token)

    job = client.post('jobs', data=json.dumps(body))

    return job

@cumulus.taskflow.task
def postprocess_description(task, _, input_, user, cluster, image, run_parameters, root_folder, description_job, description_folder):
    task.taskflow.logger.info('Processing the output of the container description job.')

    client = create_girder_client(
        task.taskflow.girder_api_url, task.taskflow.girder_token)

    # Refresh state of job
    description_job = client.get('jobs/%s' % description_job['_id'])

    upload_job_output_to_folder(cluster, description_job, girder_token=task.taskflow.girder_token)

    description_items = list(client.listItem(description_folder['_id']))

    description_file = None
    for item in description_items:
        if item['name'] == 'description.json':
            files = list(client.listFile(item['_id']))
            if len(files) != 1:
                log_std_err(task, client, description_folder)
                log_and_raise(task, 'Expecting a single file under item, found: %s' % len(files))
            description_file = files[0]

    if description_file is None:
        log_std_err(task, client, description_folder)
        log_and_raise(task, 'The container does not implement correctly the --description flag')

    with client.session() as session:
        # If we have a NEWT session id we need set as a cookie so the redirect
        # to the NEWT API works ( is authenticated ).
        newt_session_id = parse('newt.sessionId').find(user)
        if newt_session_id:
            newt_session_id = newt_session_id[0].value
            session.cookies.set('newt_sessionid', newt_session_id)

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
        cluster = get_cori(client)

    if '_id' not in cluster:
        log_and_raise(task, 'Invalid cluster configurations: %s' % cluster)

    calculation_ids = parse('calculations').find(input_)
    if not calculation_ids:
        log_and_raise(task, 'Unable to extract calculation ids.')

    calculation_ids = calculation_ids[0].value
    calculations = [client.get('calculations/%s' % x) for x in calculation_ids]
    molecule_ids = [x['moleculeId'] for x in calculations]
    geometry_ids = [x.get('geometryId') for x in calculations]

    input_parameters = [x.get('input', {}).get('parameters', {})
                        for x in calculations]

    # For now, we only allow multiple calculations if all of the input
    # parameters are the same. Raise an error if they differ.
    if not all(x == input_parameters[0] for x in input_parameters):
        msg = ('For running multiple calculations, all input parameters must '
               'currently be identical')
        log_and_raise(task, msg)

    input_parameters = input_parameters[0]

    input_format = container_description['input']['format']

    # Fetch the starting geometries
    geometry_data = []
    for molecule_id, geometry_id in zip(molecule_ids, geometry_ids):

        path = 'molecules/%s/' % molecule_id
        if geometry_id:
            # It is preferred to use the geometry if we have it
            path += 'geometries/%s/' % geometry_id

        path += '%s' % input_format

        r = client.get(path, jsonResp=False)

        if r.status_code != 200:
            raise Exception('Failed to get molecule in format: ' + input_format)

        if input_format == 'cjson':
            geometry_data.append(json.dumps(r.json()))
        else:
            geometry_data.append(r.content.decode('utf-8'))

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
        input_parameters_file = client.uploadFile(input_folder['_id'],  fp,
                                                  name, size,
                                                  parentType='folder')

    # Save the input geometries to files
    for i, data in enumerate(geometry_data):
        with tempfile.TemporaryFile() as fp:
            fp.write(data.encode())
            # Get the size of the file
            size = fp.seek(0, 2)
            fp.seek(0)
            name = 'geometry_' + str(i + 1) + '.%s' % input_format
            input_geometry_file = client.uploadFile(input_folder['_id'], fp,
                                                    name, size,
                                                    parentType='folder')

    submit_calculation.delay(input_, cluster, image, run_parameters, root_folder, container_description, input_folder, output_folder, scratch_folder, run_folder)

def _create_job(task, input_, cluster, image, run_parameters, container_description, input_folder, output_folder, scratch_folder, run_folder):
    params = _get_job_parameters(task, cluster, image, run_parameters)
    container = params['container']
    repository = params['repository']
    digest = params['digest']
    host_dir = params['hostDir']
    guest_dir = params['guestDir']
    job_dir = params['jobDir']
    job_parameters = params['jobParameters'];

    task.taskflow.logger.info('Creating %s job' % repository)

    input_format = container_description['input']['format']
    output_format = container_description['output']['format']

    input_dir = os.path.join(guest_dir, job_dir, 'input')
    output_dir = os.path.join(guest_dir, job_dir, 'output')
    scratch_dir = os.path.join(guest_dir, job_dir, 'scratch')

    parameters_filename = os.path.join(input_dir, 'input_parameters.json')

    calculation_ids = parse('calculations').find(input_)[0].value

    geometry_filenames = []
    output_filenames = []
    for i in range(len(calculation_ids)):
        geometry_filenames.append(os.path.join(input_dir, 'geometry_' + str(i + 1) + '.%s' % input_format))
        output_filenames.append(os.path.join(output_dir, 'output_' + str(i + 1) + '.%s' % output_format))

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

    # Each contain has a different bind arg
    bind_args = {
        'docker': '-v',
        'singularity': '-B',
        'shifter': '-V'
    }

    mount_option = '%s %s:%s' % (bind_args[container], host_dir, guest_dir)

    container_args = '-p %s -s %s' % (
        parameters_filename, scratch_dir
    )

    for g, o in zip(geometry_filenames, output_filenames):
        container_args += ' -g %s -o %s' % (g, o)

    image_str = image.get('repository') + ':' + image.get('tag')

    if container == 'singularity':
        # Include the path to the singularity dir, and the extension
        image_str = digest_to_sif(digest)

    if container != 'shifter':
        commands.append('%s run %s %s %s' % (
            container, mount_option, image_str, container_args
        ))
    # Shifters syntax is pretty different so special case it
    else:
        commands.append('shifter %s --image=%s --entrypoint -- %s'  % (
            mount_option, image_str, container_args
        ))

    if is_nersc(cluster):
        # NERSC specific options
        job_parameters.update({
            'numberOfNodes': 1,
            'queue': 'debug',
            'constraint': 'haswell',
            'account': os.environ.get('OC_ACCOUNT')
        })

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
        'params': job_parameters
    }

    client = create_girder_client(
                task.taskflow.girder_api_url, task.taskflow.girder_token)

    job = client.post('jobs', data=json.dumps(body))
    task.taskflow.set_metadata('jobs', [job])

    return job

@cumulus.taskflow.task
def submit_calculation(task, input_, cluster, image, run_parameters, root_folder, container_description, input_folder, output_folder, scratch_folder, run_folder):
    job = _create_job(task, input_, cluster, image, run_parameters, container_description, input_folder, output_folder, scratch_folder, run_folder)

    girder_token = task.taskflow.girder_token
    task.taskflow.set_metadata('cluster', cluster)

    # Now download and submit job to the cluster
    task.taskflow.logger.info('Uploading the input files to the cluster.')
    download_job_input_folders(cluster, job,
                               girder_token=girder_token, submit=False)

    task.taskflow.logger.info('Submitting job %s to the queue.' % job['_id'])

    submit_job(cluster, job, girder_token=girder_token, monitor=False)

    task.taskflow.logger.info('Submitted job %s to cluster.' % job['_id'])

    monitor_job.apply_async((cluster, job), {'girder_token': girder_token,
                                             'monitor_interval': 10},
                                             countdown=countdown(cluster),
                            link=postprocess_job.s(input_, cluster, image, run_parameters, root_folder, container_description, input_folder, output_folder, scratch_folder, run_folder, job))

@cumulus.taskflow.task
def postprocess_job(task, _, input_, cluster, image, run_parameters, root_folder, container_description, input_folder, output_folder, scratch_folder, run_folder, job):
    task.taskflow.logger.info('Processing the results of the calculation.')
    client = create_girder_client(
        task.taskflow.girder_api_url, task.taskflow.girder_token)

    # Refresh state of job
    job = client.get('jobs/%s' % job['_id'])

    upload_job_output_to_folder(cluster, job, girder_token=task.taskflow.girder_token)

    # remove temporary input folder, this data is attached to the calculation model
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
    output_files = []
    output_items = list(client.listItem(output_folder['_id']))
    for i in range(len(input_['calculations'])):
        output_file = None
        for item in output_items:
            if item['name'] == 'output_' + str(i + 1) + '.%s' % output_format:
                files = list(client.listFile(item['_id']))
                if len(files) != 1:
                    log_std_err(task, client, run_folder)
                    log_and_raise(task, 'Expecting a single file under item, found: %s' % len(files))
                output_file = files[0]
                break

        if output_file is None:
            # Log the job stderr
            log_std_err(task, client, run_folder)
            log_and_raise(task, 'The calculation did not produce any output file.')

        output_files.append(output_file)

    # remove the run folder, only useful to access the stdout and stderr after the job is done
    client.delete('folder/%s' % run_folder['_id'])

    # Now call endpoint to ingest result
    params = {
        'detectBonds': True
    }

    task.taskflow.logger.info('Uploading the results of the calculation to the database.')

    code = task.taskflow.get_metadata('code')
    if isinstance(code, dict):
        # Get the contents of "code" to set it below
        code = code.get('code')

    for i, output_file in enumerate(output_files):
        body = {
            'fileId': output_file['_id'],
            'format': output_format,
            'public': True,
            'image': image, # image now also has a digest field, add it to the calculation
            'scratchFolderId': scratch_folder_id,
            'code': code
        }

        client.put('calculations/%s' % input_['calculations'][i], parameters=params, json=body)

    task.taskflow.logger.log(STATUS_LEVEL, 'Done!')


def _ensure_image_on_server(task, repository, tag, digest, container='docker'):
    client = create_girder_client(
        task.taskflow.girder_api_url, task.taskflow.girder_token)
    params = {
      'repository': repository,
      'tag': tag,
      'digest': digest
    }
    r = client.get('images', params)
    images = r['results']
    if len(images) < 1:
        log_and_raise(task, 'Image not found on the server.')

    if container not in images[0]:
        msg = 'Image does not have container type: ' + container
        log_and_raise(task, msg)


def _get_digest(task, repository, tag):
    client = create_girder_client(
        task.taskflow.girder_api_url, task.taskflow.girder_token)
    params = {
      'repository': repository,
      'tag': tag
    }
    r = client.get('images', params)
    images = r['results']
    if len(images) < 1:
        log_and_raise(task, 'Image not found on the server.')

    # The digest should definitely be here
    return images[0]['digest']

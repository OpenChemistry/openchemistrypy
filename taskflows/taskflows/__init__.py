from bson.objectid import ObjectId
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
import tempfile
import re

from .utils import cjson_to_xyz

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

def _fetch_best_geometry(client, molecule_id):
    # Fetch our best geometry
    params = {
        'moleculeId': molecule_id,
        'sortByTheory': True,
        'limit': 1,
        'calculationType': 'optimization',
        'pending': False
    }

    calculations = client.get('calculations', parameters=params)

    if len(calculations) < 1:
        return None

    return calculations[0]

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
        raise Exception('Invalid cluster configurations: %s' % cluster)

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
    task.taskflow.logger.info('Downloading description input files to cluster.')
    download_job_input_folders(cluster, job,
                               girder_token=task.taskflow.girder_token, submit=False)
    task.taskflow.logger.info('Downloading complete.')

    submit_job(cluster, job, girder_token=task.taskflow.girder_token, monitor=False)

    monitor_job.apply_async((cluster, job), {'girder_token': task.taskflow.girder_token,
                                             'monitor_interval': 10},
                            link=postprocess_description.s(input_, cluster, image, run_parameters, root_folder, job, description_folder))

def _create_description_job(task, cluster, description_folder, image, run_parameters):
    container = run_parameters.get('container', 'docker')
    setup_commands = []

    if _nersc(cluster):
        container = 'singularity'
        raise NotImplementedError('Cannot run docker containers on NERSC')
    elif _demo(cluster):
        setup_commands = ['source scl_source enable python27']

    params = {
        'taskFlowId': task.taskflow.id
    }

    output_file = 'description.json'

    repository = image.get('repository')
    tag = image.get('tag')
    image_name = ":".join([repository, tag])

    commands = setup_commands + [
        'IMAGE_NAME=$(python pull.py -r %s -t %s -c %s | tail -1)' % (repository, tag, container),
        '%s run $IMAGE_NAME -d > %s' % (container, output_file),
        'rm pull.py'
    ]

    body = {
        # ensure there are no special characters in the submission script name
        'name': 'desc_%s' % re.sub('[^a-zA-Z0-9]', '_', image_name),
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
        'params': params
    }

    client = create_girder_client(
                task.taskflow.girder_api_url, task.taskflow.girder_token)

    job = client.post('jobs', data=json.dumps(body))

    return job

@cumulus.taskflow.task
def postprocess_description(task, _, input_, cluster, image, run_parameters, root_folder, description_job, description_folder):
    task.taskflow.logger.info('Processing description job output.')

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
                raise Exception('Expecting a single file under item, found: %s' + len(files))
            description_file = files[0]

        elif item['name'] == 'pull.json':
            files = list(client.listFile(item['_id']))
            if len(files) != 1:
                raise Exception('Expecting a single file under item, found: %s' + len(files))
            pull_file = files[0]

    if pull_file is None:
        raise Exception('There was an error trying to pull the requested container image')

    if description_file is None:
        raise Exception('The container does not implement correctly the --description flag')

    with tempfile.TemporaryFile() as tf:
        client.downloadFile(pull_file['_id'], tf)
        tf.seek(0)
        container_pull = json.loads(tf.read().decode())

    image = container_pull

    with tempfile.TemporaryFile() as tf:
        client.downloadFile(description_file['_id'], tf)
        tf.seek(0)
        container_description = json.loads(tf.read().decode())

    # remove temporary description folder
    client.delete('folder/%s' % description_folder['_id'])

    setup_input.delay(input_, cluster, image, run_parameters, root_folder, container_description)

@cumulus.taskflow.task
def setup_input(task, input_, cluster, image, run_parameters, root_folder, container_description):
    task.taskflow.logger.info('Setting up calculation input.')

    client = create_girder_client(
        task.taskflow.girder_api_url, task.taskflow.girder_token)

    if cluster.get('name') == 'cori':
        cluster = _get_cori(client)

    if '_id' not in cluster:
        raise Exception('Invalid cluster configurations: %s' % cluster)

    calculation_id = parse('calculation._id').find(input_)
    if not calculation_id:
        raise Exception('Unable to extract calculation id.')

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
        content = _convert_geometry(cjson, input_format)
        fp.write(content.encode())
        # Get the size of the file
        size = fp.seek(0, 2)
        fp.seek(0)
        name = 'geometry.%s' % input_format
        input_geometry_file = client.uploadFile(input_folder['_id'],  fp, name, size,
                                    parentType='folder')

    submit_calculation.delay(input_, cluster, image, run_parameters, root_folder, container_description, input_folder, output_folder, scratch_folder)

def _convert_geometry(cjson, input_format):
    if input_format.lower() == 'xyz':
        return cjson_to_xyz(cjson)
    elif input_format.lower() == 'cjson':
        return json.dumps(cjson)
    else:
        raise Exception('The container is requesting an unsupported geometry format %s') % input_format

def _create_job_ec2(task, cluster, image, run_parameters, container_description, input_folder, output_folder, scratch_folder):
    return _create_job_demo(task, cluster, image, run_parameters, container_description, input_folder, output_folder, scratch_folder)

def _create_job_demo(task, cluster, image, run_parameters, container_description, input_folder, output_folder, scratch_folder):
    container = run_parameters.get('container', 'docker')
    image_uri = image.get('imageUri')
    repository = image.get('repository')
    digest = image.get('digest')
    image_name = "@".join([repository, digest])

    task.taskflow.logger.info('Create %s job' % image_name)

    local_dir = 'dev_job_data'
    mount_dir = '/data'

    input_format = container_description['input']['format']
    output_format = container_description['output']['format']

    input_dir = os.path.join(mount_dir, '{{job._id}}', 'input')
    output_dir = os.path.join(mount_dir, '{{job._id}}', 'output')
    scratch_dir = os.path.join(mount_dir, '{{job._id}}', 'scratch')

    geometry_filename = os.path.join(input_dir, 'geometry.%s' % input_format)
    parameters_filename = os.path.join(input_dir, 'input_parameters.json')
    output_filename = os.path.join(output_dir, 'output.%s' % output_format)

    output = [
        {
            'folderId': output_folder['_id'],
            'path': './output'
        }
    ]

    keep_scratch = run_parameters.get('keepScratch', False)
    if keep_scratch:
        output.append({
            'folderId': scratch_folder['_id'],
            'path': './scratch'
        })

    if container == 'docker':
        mount_option = '-v %s:%s' % (local_dir, mount_dir)
    else:
        mount_option = ''

    body = {
        # ensure there are no special characters in the submission script name
        'name': 'run_%s' % re.sub('[^a-zA-Z0-9]', '_', image_name),
        'commands': [
            'mkdir output',
            'mkdir scratch',
            '%s run %s %s -g %s -p %s -o %s -s %s' % (
                container, mount_option, image_uri,
                geometry_filename, parameters_filename,
                output_filename, scratch_dir
            )
        ],
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

def _create_job(task, cluster, image, run_parameters, container_description, input_folder, output_folder, scratch_folder):
    if _nersc(cluster):
        raise NotImplementedError('Cannot run docker containers on NERSC')
    elif _demo(cluster):
        return _create_job_demo(task, cluster, image, run_parameters, container_description, input_folder, output_folder, scratch_folder)
    else:
        return _create_job_ec2(task, cluster, image, run_parameters, container_description, input_folder, output_folder, scratch_folder)

@cumulus.taskflow.task
def submit_calculation(task, input_, cluster, image, run_parameters, root_folder, container_description, input_folder, output_folder, scratch_folder):
    job = _create_job(task, cluster, image, run_parameters, container_description, input_folder, output_folder, scratch_folder)

    girder_token = task.taskflow.girder_token
    task.taskflow.set_metadata('cluster', cluster)

    # Now download and submit job to the cluster
    task.taskflow.logger.info('Downloading input files to cluster.')
    download_job_input_folders(cluster, job,
                               girder_token=girder_token, submit=False)
    task.taskflow.logger.info('Downloading complete.')

    task.taskflow.logger.info('Submitting job %s to cluster.' % job['_id'])

    submit_job(cluster, job, girder_token=girder_token, monitor=False)

    monitor_job.apply_async((cluster, job), {'girder_token': girder_token,
                                             'monitor_interval': 10},
                            link=postprocess_job.s(input_, cluster, image, run_parameters, root_folder, container_description, input_folder, output_folder, scratch_folder, job))

@cumulus.taskflow.task
def postprocess_job(task, _, input_, cluster, image, run_parameters, root_folder, container_description, input_folder, output_folder, scratch_folder, job):
    task.taskflow.logger.info('Processing the results of the job.')
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
                raise Exception('Expecting a single file under item, found: %s' + len(files))
            output_file = files[0]
            break

    if output_file is None:
        raise Exception('The calculation did not produce any output file.')

    # Now call endpoint to ingest result
    body = {
        'fileId': output_file['_id'],
        'format': output_format,
        'public': True,
        'image': image, # image now also has a digest field, add it to the calculation
        'scratchFolderId': scratch_folder_id
    }
    client.put('calculations/%s' % input_['calculation']['_id'], json=body)

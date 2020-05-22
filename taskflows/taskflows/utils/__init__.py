import json
import os
import re

from girder_client import HttpError

from . import avogadro


def sif_dir():
    return '$HOME/.oc/singularity'


def digest_to_sif(digest):
    return os.path.join(sif_dir(), digest + '.sif')


def image_to_sif(image_str):
    # Let's remove special characters from the image string
    name = '%s.sif' % re.sub('[^a-zA-Z0-9]', '_', image_str)
    return os.path.join(sif_dir(), name)


def cjson_to_xyz(cjson):
    xyz = avogadro.convert_str(json.dumps(cjson), 'cjson', 'xyz')
    return xyz


def get_cori(client):
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


def log_and_raise(task, msg):
    log_error(task, msg)
    raise Exception(msg)


def log_error(task, msg):
    task.taskflow.logger.error(msg)


def log_std_err(task, client, run_folder):
    errors = get_std_err(client, run_folder)
    for e in errors:
        log_error(task, e)


def get_std_err(client, run_folder):
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


def get_oc_folder(client):
    me = client.get('user/me')
    if me is None:
        raise Exception('Unable to get me.')

    login = me['login']
    private_folder_path = 'user/%s/Private' % login
    private_folder = client.resourceLookup(private_folder_path)
    oc_folder_path = '%s/oc' % private_folder_path
    # girder_client.resourceLookup(...) no longer has a test parameter
    # so we just assume that if resourceLookup(...) raises a HttpError
    # then the resource doesn't exist.
    try:
        oc_folder = client.resourceLookup(oc_folder_path)
    except HttpError:
        oc_folder = client.createFolder(private_folder['_id'], 'oc')

    return oc_folder


def is_nersc(cluster):
    return cluster.get('name') in ['cori']


def is_demo(cluster):
    return cluster.get('name') == 'demo_cluster'


def countdown(cluster):
    """
    Returns the number of seconds the monitoring task should be delayed before
    running based on the cluster we are using.
    """
    countdown = 0
    # If we are running at NERSC our job states are cached for 60 seconds,
    # so they are potentially 60 seconds out of date, so we have to wait
    # at least 60 seconds before we can assume that the job is complete if its
    # nolonger in the queue, so we delay our monitoring
    if is_nersc(cluster):
        countdown = 65

    return countdown


def post_image_to_database(client, container, repository, tag, digest, cluster,
                           size):
    body = {
        'type': container,
        'repository': repository,
        'tag': tag,
        'digest': digest,
        'clusterId': cluster['_id'],
        'size': size
    }

    try:
        client.post('images', body)
    except HttpError as e:
        # Just ignore the error if the image already exists
        if e.status != 409:
            raise

import json
import os
import re

from . import avogadro

def ensure_singularity_dir():
    home = os.path.expanduser('~')
    singularity_dir = os.path.join(home, '.oc', 'singularity')

    try:
        os.makedirs(singularity_dir)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(singularity_dir):
            pass
        else:
            raise

    return singularity_dir


def image_to_sif(image_str):
    sif_dir = ensure_singularity_dir()

    # Let's remove special characters from the image string
    name = '%s.sif' % re.sub('[^a-zA-Z0-9]', '_', image_str)
    return os.path.join(sif_dir, name)


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
    errors = _get_std_err(client, run_folder)
    for e in errors:
        _log_error(task, e)


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

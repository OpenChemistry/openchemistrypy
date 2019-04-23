import os
import requests
import re
import json
import hashlib

import avogadro

from jsonpath_rw import parse
from IPython.lib import kernel

def fetch_or_create_queue(girder_client):
    params = {'name': 'oc_queue'}
    queue = girder_client.get('queues', parameters=params)

    if (len(queue) > 0):
        queue = queue[0]
    else:
        params = {'name': 'oc_queue', 'maxRunning': 5}
        queue = girder_client.post('queues', parameters=params)

    return queue

def lookup_file(girder_client, jupyterhub_url):
    """
    Utility function to lookup the Girder file that the current notebook is running
    from.
    """
    connection_file_path = kernel.get_connection_file()
    me = girder_client.get('user/me')
    login = me['login']
    kernel_id = re.search('kernel-(.*).json', connection_file_path).group(1)

    # Authenticate with jupyterhub so we can get the cookie
    r = requests.post('%s/hub/login' % jupyterhub_url, data={'Girder-Token': girder_client.token}, allow_redirects=False)
    r.raise_for_status()
    cookies = r.cookies

    url = "%s/user/%s/api/sessions" % (jupyterhub_url, login)
    r = requests.get(url, cookies=cookies)
    r.raise_for_status()


    sessions = r.json()
    matching = [s for s in sessions if s['kernel']['id'] == kernel_id]
    path = matching[0]['path']
    name = os.path.basename(path)

    # Logout
    url = "%s/hub/logout" % jupyterhub_url
    r = requests.get(url, cookies=cookies)


    return girder_client.resourceLookup('user/%s/Private/oc/notebooks/%s/%s' % (login, path, name))

def calculate_mo(cjson, mo):
    if isinstance(mo, str):
        mo = mo.lower()
        if mo.lower() in ['homo', 'lumo']:
            # Electron count might be saved in several places...
            path_expressions = [
                'orbitals.electronCount',
                'basisSet.electronCount',
                'properties.electronCount'
            ]
            matches = []
            for expr in path_expressions:
                matches.extend(parse(expr).find(cjson))
            if len(matches) > 0:
                electron_count = matches[0].value
            else:
                raise Exception('Unable to access electronCount')

            # The index of the first orbital is 0, so homo needs to be
            # electron_count // 2 - 1
            if mo.lower() == 'homo':
                mo = int(electron_count / 2) - 1
            elif mo.lower() == 'lumo':
                mo = int(electron_count / 2)
        else:
            raise ValueError('Unsupported mo: %s' % mo)

    mol = avogadro.core.Molecule()
    conv = avogadro.io.FileFormatManager()
    conv.read_string(mol, json.dumps(cjson), 'cjson')
    # Do some scaling of our spacing based on the size of the molecule.
    atom_count = mol.atom_count()
    spacing = 0.30
    if atom_count > 50:
        spacing = 0.5
    elif atom_count > 30:
        spacing = 0.4
    elif atom_count > 10:
        spacing = 0.33
    cube = mol.add_cube()
    # Hard wiring spacing/padding for now, this could be exposed in future too.
    cube.set_limits(mol, spacing, 4)
    gaussian = avogadro.core.GaussianSetTools(mol)
    gaussian.calculate_molecular_orbital(cube, mo)

    return json.loads(conv.write_string(mol, "cjson"))['cube']

def hash_object(obj):
    return hashlib.sha512(json.dumps(obj, sort_keys=True).encode()).hexdigest()

def camel_to_space(s):
    s = re.sub(r"""
        (            # start the group
            # alternative 1
        (?<=[a-z])  # current position is preceded by a lower char
                    # (positive lookbehind: does not consume any char)
        [A-Z]       # an upper char
                    #
        |   # or
            # alternative 2
        (?<!\A)     # current position is not at the beginning of the string
                    # (negative lookbehind: does not consume any char)
        [A-Z]       # an upper char
        (?=[a-z])   # matches if next char is a lower char
                    # lookahead assertion: does not consume any char
        )           # end the group""",
    r' \1', s, flags=re.VERBOSE)
    return s[0:1].upper() + s[1:]

def parse_image_name(image_name):
    split = image_name.split(":")
    if len(split) > 2:
        raise ValueError('Invalid Docker image name provided')
    elif len(split) == 1:
        repository = split[0]
        tag = 'latest'
    else:
        repository, tag = split

    return repository, tag

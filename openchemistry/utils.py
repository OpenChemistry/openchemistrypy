import os
import requests
import re

from IPython.lib import kernel


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


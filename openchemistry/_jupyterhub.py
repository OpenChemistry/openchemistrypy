import os

from ._girder import GirderClient
from ._singleton import Singleton
from ._utils import lookup_file

@Singleton
class JupyterHub(object):

    def __init__(self):
        url = os.environ.get('OC_JUPYTERHUB_URL')
        self.url = url
        self.file = None
        
        if self.url is not None and GirderClient().client is not None:
            self.file = lookup_file(GirderClient().client, self.url)

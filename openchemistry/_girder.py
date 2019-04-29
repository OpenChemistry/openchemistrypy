import os
from girder_client import GirderClient as GC

from ._utils import lookup_file

class GirderClient(object):

    class __GirderClient(object):
        def __init__(self):
            girder_host = os.environ.get('GIRDER_HOST')
            girder_port = os.environ.get('GIRDER_PORT')
            girder_scheme = os.environ.get('GIRDER_SCHEME', 'http')
            girder_api_root = os.environ.get('GIRDER_API_ROOT', '/api/v1')
            girder_api_key = os.environ.get('GIRDER_API_KEY')
            girder_token = os.environ.get('GIRDER_TOKEN')
            # First check if we have a public url set
            girder_api_url = os.environ.get('GIRDER_PUBLIC_API_URL')
            if girder_api_url is None:
                girder_api_url = '%s://%s%s/%s' % (
                    girder_scheme, girder_host, ':%s' % girder_port if girder_port else '',
                    girder_api_root )
            app_base_url = os.environ.get('APP_BASE_URL')
            cluster_id = os.environ.get('CLUSTER_ID')
            jupyterhub_url = os.environ.get('OC_JUPYTERHUB_URL')

            self.file = None
            self.client = None
            self.host = None
            self.api_url = girder_api_url
            self.app_url = app_base_url
            self.cluster_id = cluster_id

            if girder_host:
                self.client = GC(host=girder_host, port=girder_port,
                                            scheme=girder_scheme, apiRoot=girder_api_root)
                self.host = girder_host

                if girder_api_key is not None:
                    self.client.authenticate(apiKey=girder_api_key)
                elif girder_token is not None:
                    self.client.token = girder_token

                if jupyterhub_url is not None:
                    self.file = lookup_file(self.client, jupyterhub_url)

        def __getattr__(self, name):
            return getattr(self.client, name)

    instance = None

    def __init__(self):
        if GirderClient.instance is None:
            GirderClient.instance = GirderClient.__GirderClient()

    def __getattr__(self, name):
        return getattr(GirderClient.instance, name)

import os
from girder_client import GirderClient as GC

from ._singleton import Singleton
from ._utils import get_oc_token_obj

@Singleton
class GirderClient(object):
    def __init__(self):
        token_obj = get_oc_token_obj()
        url = token_obj.get('apiUrl')
        api_key = token_obj.get('apiKey')

        url = os.environ.get('OC_API_URL', url)
        internal_url = os.environ.get('OC_INTERNAL_API_URL', url)
        api_key = os.environ.get('OC_API_KEY', api_key)
        token = os.environ.get('GIRDER_TOKEN')

        self.client = None
        self.url = url
        self.internal_url = internal_url

        if internal_url is not None:
            self.client = GC(apiUrl=internal_url)

            if api_key is not None:
                self.client.authenticate(apiKey=api_key)
            elif token is not None:
                self.client.token = token

    def __getattr__(self, name):
        return getattr(self.client, name)

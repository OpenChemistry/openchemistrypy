import os

from ._singleton import Singleton
from ._utils import get_oc_token_obj

@Singleton
class Application(object):

    def __init__(self):
        token_obj = get_oc_token_obj()
        url = token_obj.get('appUrl')
        url = os.environ.get('OC_APP_URL', url)
        self.url = url

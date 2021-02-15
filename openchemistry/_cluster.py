import os

from ._singleton import Singleton

@Singleton
class Cluster(object):

    def __init__(self):
        self.id = os.environ.get('OC_CLUSTER_ID')

import json
import urllib
import urllib.parse
from jsonpath_rw import parse

from ._girder import GirderClient
from ._jupyterhub import JupyterHub
from ._utils import (
    parse_image_name, md_table, AttributeInterceptor
)

class Model(object):
    '''

    '''
    def __init__(self):
        pass

def _fetch_model(image_name, input_parameters):
    repository, tag = parse_image_name(image_name)
    input_params_quoted = urllib.parse.quote(json.dumps(input_parameters))
    parameters = {
        'inputParameters': input_params_quoted,
        'imageName': '%s:%s' % (repository, tag)
    }

    res = GirderClient().get('trainings', parameters)
    if 'results' not in res or len(res['results']) < 1:
        return None

    return res['results'][0]

def _fetch_taskflow_status(taskflow_id):
    r = GirderClient().get('taskflows/%s/status' % taskflow_id)

    return r['status']

def _create_pending_model(image_name, input_parameters):
    repository, tag = parse_image_name(image_name)

    notebooks = []
    if JupyterHub().file is not None:
        notebooks.append(JupyterHub().file['_id'])

    body = {
        'public': True,
        'properties': {
            'pending': True
        },
        'input': {
            'parameters': input_parameters,
        },
        'image': {
            'repository': repository,
            'tag': tag
        },
        'notebooks': notebooks
    }

    model = GirderClient().post('trainings', json=body)

    return model

def _submit_model(cluster_id, model_id, image_name, run_parameters):
    if run_parameters is None:
        run_parameters = {}

    repository, tag = parse_image_name(image_name)

    body = {}
    body['taskFlowBody'] = {
        'taskFlowClass': 'taskflows.OpenChemistryTaskFlow',
        'meta': {
            'trainingIds': [model_id],
            'image': {
                'repository': repository,
                'tag': tag
            }
        }
    }

    body['taskFlowInput'] = {
        'input': {
            'type': 'training',
            'trainings': [model_id]
        },
        'image': {
            'repository': repository,
            'tag': tag
        },
        'runParameters': run_parameters
    }

    if cluster_id:
        body['taskFlowInput']['cluster'] = {'_id': cluster_id}

    # This returns the taskflow id
    return GirderClient().post('launch_taskflow/launch', json=body)

def _model_result(model):
    pending = parse('properties.pending').find(model)
    if pending:
        pending = pending[0].value

    result = ModelResult(model['_id'], model.get('properties'))

    if pending:
        taskflow_id = parse('properties.taskFlowId').find(model)
        if taskflow_id:
            taskflow_id = taskflow_id[0].value
        else:
            taskflow_id = None

        result = PendingModelResultWrapper(result, taskflow_id)

    return result

class Properties(object):

    def __init__(self, properties):
        self._properties = properties

    def show(self, **kwargs):
        properties = self._properties
        try:
            from IPython.display import Markdown
            table = md_table(properties, 'Model Properties', 'Name', 'Value')
            return Markdown(table)
        except ImportError:
            # Outside notebook print CJSON
            print(properties)

    def data(self):
        return self._properties


class ModelResult(object):

    def __init__(self, _id, properties=None):
        self._id = _id
        if properties is None:
            properties = {}

        self._properties = properties
        self._visualizations = {}

    @property
    def properties(self):
        viz = self._visualizations.get('properties')

        if viz is None:
            viz = Properties(self._properties)
            self._visualizations['properties'] = viz

        return viz


class PendingModelResultWrapper(AttributeInterceptor):
    def __init__(self, model, taskflow_id=None):
        try:
            from ._notebook import CalculationMonitor
            if taskflow_id is None:
                taskflow_id = model._properties['taskFlowId']

            table = CalculationMonitor({
                'taskFlowIds': [taskflow_id],
                'girderToken': GirderClient().token,
                'girderApiUrl': GirderClient().url
            })
        except ImportError:
            # Outside notebook just print message
            table = 'Pending calculations .... '

        # Only intercept when the taskflow is not complete
        def intercept():
            return _fetch_taskflow_status(taskflow_id) != 'complete'

        super(PendingModelResultWrapper, self).__init__(model, table, intercept)

import os
import inspect
from jsonpath_rw import parse

from ._girder import GirderClient
from ._molecule import Molecule
from ._data import MoleculeProvider, CalculationProvider
from ._utils import fetch_or_create_queue, hash_object, parse_image_name

class GirderMolecule(Molecule):
    '''
    Derived version that allows calculations to be initiated on using Girder
    '''
    def __init__(self, _id, cjson=None):
        if cjson is None:
            cjson = GirderClient().get('molecules/%s/cjson' % _id)
        super(GirderMolecule, self).__init__(MoleculeProvider(cjson, _id))
        self._id = _id

    def calculate(self, image_name, input_parameters, input_geometry=None, run_parameters=None, force=False):
        molecule_id = self._id
        calculation = _fetch_or_submit_calculation(molecule_id, image_name, input_parameters, input_geometry, run_parameters, force)
        pending = parse('properties.pending').find(calculation)
        if pending:
            pending = pending[0].value

        taskflow_id = parse('properties.taskFlowId').find(calculation)
        if taskflow_id:
            taskflow_id = taskflow_id[0].value
        else:
            taskflow_id = None

        calculation = CalculationResult(calculation['_id'], calculation['properties'], molecule_id)

        if pending:
            calculation = PendingCalculationResultWrapper(calculation, taskflow_id)

        return calculation

    def energy(self, image_name, input_parameters, input_geometry=None, run_parameters=None, force=False):
        params = {'task': 'energy'}
        params.update(input_parameters)
        return self.calculate(image_name, params, input_geometry, run_parameters, force)

    def optimize(self, image_name, input_parameters, input_geometry=None, run_parameters=None, force=False):
        params = {'task': 'optimize'}
        params.update(input_parameters)
        return self.calculate(image_name, params, input_geometry, run_parameters, force)

    def frequencies(self, image_name, input_parameters, input_geometry=None, run_parameters=None, force=False):
        params = {'task': 'frequencies'}
        params.update(input_parameters)
        return self.calculate(image_name, params, input_geometry, run_parameters, force)

    def set_name(self, name):
        body = {
            'name': name
        }
        GirderClient().patch('molecules/%s' % self._id, json=body)

class CalculationResult(Molecule):

    def __init__(self, _id=None, properties=None, molecule_id=None):
        super(CalculationResult, self).__init__(CalculationProvider(_id, molecule_id))
        self._id = _id
        self._properties = properties
        self._molecule_id = molecule_id

    def data(self):
        return self._provider.cjson

    @property
    def frequencies(self):
        import warnings
        warnings.warn("Use the 'vibrations' property to display normal modes")
        return self.vibrations

    def delete(self):
        return _delete_calculation(self._id)

class AttributeInterceptor(object):
    def __init__(self, wrapped, value, intercept_func=lambda : True):
        self._wrapped = wrapped
        self._value = value
        self._intercept_func = intercept_func

    def unwrap(self):
        return self._wrapped

    def __getattr__(self, name):
        # Use object's implementation to get attributes, otherwise
        # we will get recursion
        _wrapped = object.__getattribute__(self, '_wrapped')
        _value = object.__getattribute__(self, '_value')
        _intercept_func = object.__getattribute__(self, '_intercept_func')

        attr = object.__getattribute__(_wrapped, name)
        if _intercept_func():
            if inspect.ismethod(attr):
                def pending(*args, **kwargs):
                    return _value
                return pending
            else:
                return AttributeInterceptor(attr, _value, _intercept_func)
        else:
            return attr

class PendingCalculationResultWrapper(AttributeInterceptor):
    def __init__(self, calculation, taskflow_id=None):
        try:
            from ._notebook import CalculationMonitor
            if taskflow_id is None:
                taskflow_id = calculation._properties['taskFlowId']

            table = CalculationMonitor({
                'taskFlowIds': [taskflow_id],
                'girderToken': GirderClient().token,
                'girderApiUrl': GirderClient().api_url
            })
        except ImportError:
            # Outside notebook just print message
            table = 'Pending calculations .... '

        # Only intercept when the taskflow is not complete
        def intercept():
            return _fetch_taskflow_status(taskflow_id) != 'complete'

        super(PendingCalculationResultWrapper, self).__init__(calculation,
                                                              table, intercept)

def _fetch_calculation(molecule_id, image_name, input_parameters, input_geometry=None):
    repository, tag = parse_image_name(image_name)
    parameters = {
        'moleculeId': molecule_id,
        'inputParametersHash': hash_object(input_parameters),
        'imageName': '%s:%s' % (repository, tag)
    }

    if input_geometry:
        parameters['inputGeometryHash'] = hash_object(input_geometry)

    calculations = GirderClient().get('calculations', parameters)

    if len(calculations) < 1:
        return None

    return calculations[0]

def _nersc():
    return os.environ.get('OC_SITE') == 'NERSC'

def _submit_calculation(cluster_id, pending_calculation_id, image_name, run_parameters):
    if cluster_id is None and not _nersc():
        # Try to get demo cluster
        params = {
            'type': 'trad'
        }
        clusters = GirderClient().get('clusters', params)

        if len(clusters) > 0:
            cluster_id = clusters[0]['_id']
        else:
            raise Exception('Unable to submit calculation, no cluster configured.')

    if run_parameters is None:
        run_parameters = {}

    repository, tag = parse_image_name(image_name)

    # Create the taskflow
    queue = fetch_or_create_queue(GirderClient())

    body = {
        'taskFlowClass': 'taskflows.OpenChemistryTaskFlow',
        'meta': {
            'calculationId': pending_calculation_id,
            'image': {
                'repository': repository,
                'tag': tag
            }
        }
    }

    taskflow = GirderClient().post('taskflows', json=body)

    # Start the taskflow
    body = {
        'input': {
            'calculation': {
                '_id': pending_calculation_id
            }
        },
        'image': {
            'repository': repository,
            'tag': tag
        },
        'runParameters': run_parameters
    }

    if cluster_id is not None:
        body['cluster'] = {
            '_id': cluster_id
        }
    elif _nersc():
        body['cluster'] = {
            'name': 'cori'
        }

    GirderClient().put('queues/%s/add/%s' % (queue['_id'], taskflow['_id']), json=body)
    GirderClient().put('queues/%s/pop' % queue['_id'], parameters={'multi': True})

    return taskflow['_id']

def _fetch_taskflow_status(taskflow_id):
    r = GirderClient().get('taskflows/%s/status' % taskflow_id)

    return r['status']

def _create_pending_calculation(molecule_id, image_name, input_parameters, input_geometry=None):
    repository, tag = parse_image_name(image_name)

    notebooks = []
    if GirderClient().file is not None:
        notebooks.append(GirderClient().file['_id'])

    body = {
        'moleculeId': molecule_id,
        'cjson': None,
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

    if input_geometry is not None:
        body['input']['geometry'] = input_geometry

    calculation = GirderClient().post('calculations', json=body)

    return calculation

def _delete_calculation(calculation_id):
    GirderClient().delete('calculations/%s' % calculation_id)

def _fetch_or_submit_calculation(molecule_id, image_name, input_parameters, input_geometry=None, run_parameters=None, force=False):
    calculation = _fetch_calculation(molecule_id, image_name, input_parameters, input_geometry)
    taskflow_id = None

    if calculation is None or force:
        calculation = _create_pending_calculation(molecule_id, image_name, input_parameters, input_geometry)
        taskflow_id = _submit_calculation(GirderClient().cluster_id, calculation['_id'], image_name, run_parameters)
        # Patch calculation to include taskflow id
        props = calculation['properties']
        props['taskFlowId'] = taskflow_id
        calculation = GirderClient().put('calculations/%s/properties' % calculation['_id'], json=props)
    else:
        # If we already have a calculation tag it with this notebooks id
        notebooks = calculation.setdefault('notebooks', [])
        if GirderClient().file is not None:
            notebooks.append(GirderClient().file['_id'])

        body = {
            'notebooks': notebooks
        }
        GirderClient().patch('calculations/%s/notebooks' % calculation['_id'],
                            json=body)

    return calculation

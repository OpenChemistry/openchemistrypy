import os
import inspect
import json
from jsonpath_rw import parse
import urllib
import urllib.parse

from girder_client import HttpError

from ._girder import GirderClient
from ._molecule import Molecule
from ._data import MoleculeProvider, CalculationProvider
from ._utils import (
    fetch_or_create_queue, hash_object, parse_image_name, mol_has_3d_coords
)

class GirderMolecule(Molecule):
    '''
    Derived version that allows calculations to be initiated on using Girder
    '''
    def __init__(self, _id, cjson=None):
        if cjson is None:
            try:
                cjson = GirderClient().get('molecules/%s/cjson' % _id)
            except HttpError as ex:
                if ex.status == 404:
                    # The molecule does not have 3D coordinates
                    cjson = None
                else:
                    raise

        super(GirderMolecule, self).__init__(MoleculeProvider(cjson, _id))
        self._id = _id

    def calculate(self, image_name, input_parameters, geometry_id=None, run_parameters=None, force=False):
        molecule_id = self._id
        calculations = _fetch_or_submit_calculations([molecule_id], image_name,
                                                     input_parameters,
                                                     [geometry_id],
                                                     run_parameters, force)
        if calculations:
            return _calculation_result(calculations[0], molecule_id)

    def energy(self, image_name, input_parameters, geometry_id=None, run_parameters=None, force=False):
        params = {'task': 'energy'}
        params.update(input_parameters)
        return self.calculate(image_name, params, geometry_id, run_parameters, force)

    def optimize(self, image_name, input_parameters, geometry_id=None, run_parameters=None, force=False):
        params = {'task': 'optimize'}
        params.update(input_parameters)
        return self.calculate(image_name, params, geometry_id, run_parameters, force)

    def frequencies(self, image_name, input_parameters, geometry_id=None, run_parameters=None, force=False):
        params = {'task': 'frequencies'}
        params.update(input_parameters)
        return self.calculate(image_name, params, geometry_id, run_parameters, force)

    def set_name(self, name):
        body = {
            'name': name
        }
        GirderClient().patch('molecules/%s' % self._id, json=body)

    def add_geometry(self, cjson):
        GirderClient().post('molecules/%s/geometries' % self._id, json=cjson)

class CalculationResult(Molecule):

    def __init__(self, _id=None, properties=None, molecule_id=None):
        super(CalculationResult, self).__init__(CalculationProvider(_id, molecule_id))
        self._id = _id
        self._properties = properties
        self._molecule_id = molecule_id
        self._optimized_geometry_id = None

    def data(self):
        return self._provider.cjson

    @property
    def optimized_geometry_id(self):
        if not self._optimized_geometry_id:
            # Try to get it...
            result = GirderClient().get('calculations/%s' % self._id)
            if 'optimizedGeometryId' in result:
                self._optimized_geometry_id = result['optimizedGeometryId']
            else:
                print('None')

        return self._optimized_geometry_id

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

def _fetch_calculation(molecule_id, image_name, input_parameters, geometry_id=None):
    repository, tag = parse_image_name(image_name)
    input_params_quoted = urllib.parse.quote(json.dumps(input_parameters))
    parameters = {
        'moleculeId': molecule_id,
        'inputParameters': input_params_quoted,
        'imageName': '%s:%s' % (repository, tag)
    }

    if geometry_id:
        parameters['geometryId'] = geometry_id

    res = GirderClient().get('calculations', parameters)
    if 'results' not in res or len(res['results']) < 1:
        return None

    return res['results'][0]

def _nersc():
    return os.environ.get('OC_SITE') == 'NERSC'

def _submit_calculations(cluster_id, pending_calculation_ids, image_name,
                         run_parameters):
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
            'calculationIds': pending_calculation_ids,
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
            'calculations': pending_calculation_ids
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

def _create_pending_calculation(molecule_id, image_name, input_parameters, geometry_id=None):
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

    if geometry_id is not None:
        body['geometryId'] = geometry_id

    calculation = GirderClient().post('calculations', json=body)

    return calculation

def _delete_calculation(calculation_id):
    GirderClient().delete('calculations/%s' % calculation_id)

def _fetch_or_submit_calculations(molecule_ids, image_name, input_parameters,
                                  geometry_ids=None, run_parameters=None,
                                  force=False):

    try:
        _check_required_coords(molecule_ids, image_name)
    except Exception as e:
        print(str(e))
        return []

    if geometry_ids is None:
        geometry_ids = [None] * len(molecule_ids)

    calculations = []
    pending_calculations = []
    for molecule_id, geometry_id in zip(molecule_ids, geometry_ids):
        calculation = _fetch_calculation(molecule_id, image_name,
                                         input_parameters, geometry_id)
        if calculation is None or force:
            calculation = _create_pending_calculation(molecule_id, image_name,
                                                      input_parameters,
                                                      geometry_id)
            pending_calculations.append(calculation)
        else:
            # If we already have a calculation tag it with this notebooks id
            notebooks = calculation.setdefault('notebooks', [])
            if GirderClient().file is not None:
                notebooks.append(GirderClient().file['_id'])

            body = {
                'notebooks': notebooks
            }
            GirderClient().patch('calculations/%s/notebooks' %
                                 calculation['_id'],
                                 json=body)

        calculations.append(calculation)

    if len(pending_calculations) != 0:
        calc_ids = [x['_id'] for x in pending_calculations]
        taskflow_id = _submit_calculations(GirderClient().cluster_id, calc_ids,
                                           image_name, run_parameters)

        for i, calculation in enumerate(pending_calculations):
            # Patch calculation to include taskflow id
            props = calculation['properties']
            props['taskFlowId'] = taskflow_id
            pending_calculations[i] = GirderClient().put(
                'calculations/%s/properties' % calculation['_id'], json=props)
            index = calculations.index(calculation)
            calculations[index] = pending_calculations[i]

    return calculations

def _calculation_result(calculation, molecule_id):
    pending = parse('properties.pending').find(calculation)
    if pending:
        pending = pending[0].value

    result = CalculationResult(calculation['_id'], calculation['properties'],
                               molecule_id)

    if pending:
        taskflow_id = parse('properties.taskFlowId').find(calculation)
        if taskflow_id:
            taskflow_id = taskflow_id[0].value
        else:
            taskflow_id = None

        result = PendingCalculationResultWrapper(result, taskflow_id)

    return result

def _3d_coords_required(image_name):
    # We will have a list of programs that require 3D coordinates
    require_3d_coords_list = [
        'psi4',
        'nwchem'
    ]

    for program in require_3d_coords_list:
        if program in image_name:
            return True

    return False

def _mol_has_3d_coords(mol_id):
    mol = GirderClient().get('molecules/%s' % mol_id)
    return mol_has_3d_coords(mol)

def _generate_3d_coords(mol_id):
    GirderClient().post('molecules/%s/3d' % mol_id)

def _check_required_coords(mol_ids, image_name):
    raise_exception = False
    if _3d_coords_required(image_name):
        for mol_id in mol_ids:
            if not _mol_has_3d_coords(mol_id):
                _generate_3d_coords(mol_id)
                raise_exception = True

    if raise_exception:
        msg = 'Generating 3D coordinates, please re-run the calculation soon'
        raise Exception(msg)

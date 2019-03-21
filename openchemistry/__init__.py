from jinja2 import Environment, BaseLoader
from girder_client import GirderClient, HttpError
import re
import os
import urllib.parse
import json
import inspect
import collections
from abc import ABC, abstractmethod
from jsonpath_rw import parse

from .utils import lookup_file, calculate_mo, hash_object, camel_to_space
import avogadro

from .io.psi4 import Psi4Reader
from .io.nwchemJson import NWChemJsonReader
from .io.cjson import CjsonReader

girder_host = os.environ.get('GIRDER_HOST')
girder_port = os.environ.get('GIRDER_PORT')
girder_scheme = os.environ.get('GIRDER_SCHEME', 'http')
girder_api_root = os.environ.get('GIRDER_API_ROOT', '/api/v1')
girder_api_key = os.environ.get('GIRDER_API_KEY')
girder_token = os.environ.get('GIRDER_TOKEN')
girder_api_url = '%s://%s%s/%s' % (
    girder_scheme, girder_host, ':%s' % girder_port if girder_port else '',
    girder_api_root )
app_base_url = os.environ.get('APP_BASE_URL')
cluster_id = os.environ.get('CLUSTER_ID')
jupyterhub_url = os.environ.get('OC_JUPYTERHUB_URL')

if girder_host:
    girder_client = GirderClient(host=girder_host, port=girder_port,
                                 scheme=girder_scheme, apiRoot=girder_api_root)

    if girder_api_key is not None:
        girder_client.authenticate(apiKey=girder_api_key)
    elif girder_token is not None:
        girder_client.token = girder_token

    girder_file = None
    if jupyterhub_url is not None:
        girder_file = lookup_file(girder_client, jupyterhub_url)

def _fetch_calculation(molecule_id, image_name, input_parameters, input_geometry=None):
    repository, tag = parse_image_name(image_name)
    parameters = {
        'moleculeId': molecule_id,
        'inputParametersHash': hash_object(input_parameters),
        'imageName': '%s:%s' % (repository, tag)
    }

    if input_geometry:
        parameters['inputGeometryHash'] = hash_object(input_geometry)

    calculations = girder_client.get('calculations', parameters)

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
        clusters = girder_client.get('clusters', params)

        if len(clusters) > 0:
            cluster_id = clusters[0]['_id']
        else:
            raise Exception('Unable to submit calculation, no cluster configured.')

    if run_parameters is None:
        run_parameters = {}

    repository, tag = parse_image_name(image_name)

    # Create the taskflow
    queue = _fetch_or_create_queue()

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

    taskflow = girder_client.post('taskflows', json=body)

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

    girder_client.put('queues/%s/add/%s' % (queue['_id'], taskflow['_id']), json=body)
    girder_client.put('queues/%s/pop' % queue['_id'], parameters={'multi': True})

    return taskflow['_id']

def _fetch_taskflow_status(taskflow_id):
    r = girder_client.get('taskflows/%s/status' % taskflow_id)

    return r['status']

def _create_pending_calculation(molecule_id, image_name, input_parameters, input_geometry=None):
    repository, tag = parse_image_name(image_name)

    notebooks = []
    if girder_file is not None:
        notebooks.append(girder_file['_id'])

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

    calculation = girder_client.post('calculations', json=body)

    return calculation

def _delete_calculation(calculation_id):
    girder_client.delete('calculations/%s' % calculation_id)

def _fetch_or_submit_calculation(molecule_id, image_name, input_parameters, input_geometry=None, run_parameters=None, force=False):
    global cluster_id

    calculation = _fetch_calculation(molecule_id, image_name, input_parameters, input_geometry)
    taskflow_id = None

    if calculation is None or force:
        calculation = _create_pending_calculation(molecule_id, image_name, input_parameters, input_geometry)
        taskflow_id = _submit_calculation(cluster_id, calculation['_id'], image_name, run_parameters)
        # Patch calculation to include taskflow id
        props = calculation['properties']
        props['taskFlowId'] = taskflow_id
        calculation = girder_client.put('calculations/%s/properties' % calculation['_id'], json=props)
    else:
        # If we already have a calculation tag it with this notebooks id
        notebooks = calculation.setdefault('notebooks', [])
        if girder_file is not None:
            notebooks.append(girder_file['_id'])

        body = {
            'notebooks': notebooks
        }
        girder_client.patch('calculations/%s/notebooks' % calculation['_id'],
                            json=body)

    return calculation

def _fetch_or_create_queue():
    params = {'name': 'oc_queue'}
    queue = girder_client.get('queues', parameters=params)

    if (len(queue) > 0):
        queue = queue[0]
    else:
        params = {'name': 'oc_queue', 'maxRunning': 5}
        queue = girder_client.post('queues', parameters=params)

    return queue

class Molecule(object):
    def __init__(self, provider):
        self._provider = provider
        self._visualizations = {
            'structure': None,
            'orbitals': None,
            'properties': None,
            'vibrations': None
        }

    @property
    def structure(self):
        if self._visualizations['structure'] is None:
            self._visualizations['structure'] = Structure(self._provider)
        return self._visualizations['structure']

    @property
    def orbitals(self):
        if self._visualizations['orbitals'] is None:
            self._visualizations['orbitals'] = Orbitals(self._provider)
        return self._visualizations['orbitals']

    @property
    def properties(self):
        if self._visualizations['properties'] is None:
            self._visualizations['properties'] = Properties(self._provider)
        return self._visualizations['properties']

    @property
    def vibrations(self):
        if self._visualizations['vibrations'] is None:
            self._visualizations['vibrations'] = Vibrations(self._provider)
        return self._visualizations['vibrations']

class GirderMolecule(Molecule):
    '''
    Derived version that allows calculations to be initiated on using Girder
    '''
    def __init__(self, _id, cjson=None):
        if cjson is None:
            cjson = girder_client.get('molecules/%s/cjson' % _id)
        super(GirderMolecule, self).__init__(CjsonProvider(cjson))
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

class CalculationResult(Molecule):

    def __init__(self, _id=None, properties=None, molecule_id=None):
        super(CalculationResult, self).__init__(CalculationProvider(_id, molecule_id))
        self._id = _id
        self._properties = properties
        self._molecule_id = molecule_id

    @property
    def frequencies(self):
        import warnings
        warnings.warn("Use the 'vibrations' property to display normal modes")
        return self.vibrations

    def delete(self):
        return _delete_calculation(self._id)

class DataProvider(ABC):
    @property
    @abstractmethod
    def cjson(self):
        pass

    @property
    @abstractmethod
    def vibrations(self):
        pass

    @abstractmethod
    def load_orbital(self, mo):
        pass

    @property
    @abstractmethod
    def url(self):
        pass

class CachedDataProvider(DataProvider):
    MAX_CACHED = 5

    def __init__(self):
        self._cached_volumes = collections.OrderedDict()

    def _get_cached_volume(self, mo):
        if mo in self._cached_volumes:
            return self._cached_volumes[mo]
        return None

    def _set_cached_volume(self, mo, cube):
        keys = self._cached_volumes.keys()
        if len(keys) >= self.MAX_CACHED:
            del self._cached_volumes[next(iter(keys))]
        self._cached_volumes[mo] = cube

class CjsonProvider(CachedDataProvider):
    def __init__(self, cjson):
        super(CjsonProvider, self).__init__()
        self._cjson_ = cjson

    @property
    def cjson(self):
        return self._cjson_

    @property
    def vibrations(self):
        if 'vibrations' in self.cjson:
            return self.cjson['vibrations']
        else:
            return {'modes': [], 'intensities': [], 'frequencies': []}

    def load_orbital(self, mo):
        cube = super(CjsonProvider, self)._get_cached_volume(mo)
        if cube is None:
            cube = calculate_mo(self.cjson, mo)
            super(CjsonProvider, self)._set_cached_volume(mo, cube)
        cjson = self.cjson
        cjson['cube'] = cube

    @property
    def url(self):
        return None

class CalculationProvider(CachedDataProvider):
    def __init__(self, calculation_id, molecule_id):
        super(CalculationProvider, self).__init__()
        self._id = calculation_id
        self._molecule_id = molecule_id
        self._cjson_ = None
        self._vibrational_modes_ = None

    @property
    def cjson(self):
        if self._cjson_ is None:
            self._cjson_ = girder_client.get('calculations/%s/cjson' % self._id)

        return self._cjson_

    @property
    def vibrations(self):
        if self._vibrational_modes_ is None:
            self._vibrational_modes_ = girder_client.get('calculations/%s/vibrationalmodes' % self._id)

        return self._vibrational_modes_

    def load_orbital(self, mo):
        cube = super(CalculationProvider, self)._get_cached_volume(mo)
        if cube is None:
            cube = girder_client.get('calculations/%s/cube/%s' % (self._id, mo))['cube']
            super(CalculationProvider, self)._set_cached_volume(mo, cube)
        cjson = self.cjson
        cjson['cube'] = cube

    @property
    def url(self):
        return '%s/calculations/%s' % (app_base_url.rstrip('/'), self._id)

class AvogadroProvider(CjsonProvider):
    def __init__(self, molecule):
        self._molecule = molecule
        self._cjson = None

    @property
    def cjson(self):
        if self._cjson is None:
            conv = avogadro.io.FileFormatManager()
            cjson_str = conv.write_string(self._molecule, 'cjson')
            self._cjson = json.loads(cjson_str)
        return self._cjson

class Visualization(ABC):
    def __init__(self, provider):
        self._provider = provider
        self._params = {}

    @abstractmethod
    def show(self, viewer='moljs', spectrum=False, volume=False, isosurface=False, menu=True, mo=None, iso=None, transfer_function=None, mode=-1, play=False, alt=None):
        self._params = {
            'moleculeRenderer': viewer,
            'showSpectrum': spectrum,
            'showVolume': volume,
            'showIsoSurface': isosurface,
            'showMenu': menu,
            'mo': mo,
            'isoValue': iso,
            'mode': mode,
            'play': play,
            **self._transfer_function_to_params(transfer_function)
        }
        try:
            from .notebook import CJSON
            return CJSON(self._provider.cjson, **self._params)
        except ImportError:
            # Outside notebook print CJSON
            if alt is None:
                print(self._provider.cjson)
            else:
                print(alt)

    def url(self):
        url = self._provider.url
        if url is None:
            raise ValueError('The current structure is not coming from a calculation')

        if self._params:
            url = '%s?%s' % (url, urllib.parse.urlencode(self._params))
        try:
            from IPython.display import Markdown
            return Markdown('[%s](%s)' % (url, url))
        except ImportError:
            # Outside notebook just print the url
            print(url)

    def _transfer_function_to_params(self, transfer_function):
        '''
        transfer_function = {
            "colormap": {
                "colors": [[r0, g0, b0], [r1, g1, b1], ...],
                "points": [p0, p1, ...]
            },
            "opacitymap": {
                "opacities": [alpha0, alpha1, ...],
                "points": [p0, p1, ...]
            }
        }
        '''
        params = {}

        if transfer_function is None or not isinstance(transfer_function, dict):
            return params

        colormap = transfer_function.get('colormap')
        if colormap is not None:
            colors = colormap.get('colors')
            points = colormap.get('points')
            if colors is not None:
                params['colors'] = colors
                params['activeMapName'] = 'Custom'
            if points is not None:
                params['colorsX'] = points

        opacitymap = transfer_function.get('opacitymap')
        if opacitymap is not None:
            opacities = opacitymap.get('opacities')
            points = opacitymap.get('points')
            if opacities is not None:
                params['opacities'] = opacities
            if points is not None:
                params['opacitiesX'] = points

        return params

class Structure(Visualization):

    def show(self, viewer='moljs', menu=True, **kwargs):
        return super(Structure, self).show(viewer=viewer, menu=menu, **kwargs)

class Vibrations(Visualization):

    def show(self, viewer='moljs', spectrum=True, menu=True, mode=-1, play=True, **kwargs):
        return super(Vibrations, self).show(viewer=viewer, spectrum=spectrum, menu=menu, mode=mode, play=play, **kwargs)

    def table(self):
        vibrations = self._provider.vibrations
        try:
            from IPython.display import Markdown
            table = self._md_table(vibrations)
            return Markdown(table)
        except ImportError:
            # Outside notebook print CJSON
            print(vibrations)

    def _md_table(self, vibrations):
        import math
        table = '''### Normal Modes
| # | Frequency | Intensity |
|------|-------|-------|'''
        frequencies = vibrations.get('frequencies', [])
        intensities = vibrations.get('intensities', [])

        n = len(frequencies)
        if len(intensities) != n:
            intensities = None

        for i, freq in enumerate(frequencies):
            try:
                freq = float(freq)
            except ValueError:
                freq = math.nan

            intensity = math.nan if intensities is None else intensities[i]
            try:
                intensity = float(intensity)
            except ValueError:
                intensity = math.nan

            table += '\n| %s | %.2f | %.2f |' % (
                i,
                freq,
                intensity
            )

        return table

class Orbitals(Visualization):

    def show(self, viewer='moljs', volume=False, isosurface=True, menu=True, mo='homo', iso=0.05, transfer_function=None, **kwargs):
        self._provider.load_orbital(mo)
        return super(Orbitals, self).show(viewer=viewer, volume=volume, isosurface=isosurface, menu=menu, mo=mo, iso=iso, transfer_function=transfer_function)

class Properties(Visualization):

    def show(self, **kwargs):
        cjson = self._provider.cjson
        properties = cjson.get('properties', {})
        try:
            from IPython.display import Markdown
            table = self._md_table(properties)
            return Markdown(table)
        except ImportError:
            # Outside notebook print CJSON
            print(properties)

    def _md_table(self, properties):
        import math
        table = '''### Calculated Properties
| Name | Value |
|------|-------|'''

        for prop, value in properties.items():
            try:
                value = float(value)
            except ValueError:
                value = math.nan
            table += '\n| %s | %.2f |' % (
                camel_to_space(prop),
                value
            )

        return table

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
            from .notebook import CalculationMonitor
            if taskflow_id is None:
                taskflow_id = calculation._properties['taskFlowId']

            table = CalculationMonitor({
                'taskFlowIds': [taskflow_id],
                'girderToken': girder_client.token,
                'girderApiUrl': girder_api_url
            })
        except ImportError:
            # Outside notebook just print message
            table = 'Pending calculations .... '

        # Only intercept when the taskflow is not complete
        def intercept():
            return _fetch_taskflow_status(taskflow_id) != 'complete'

        super(PendingCalculationResultWrapper, self).__init__(calculation,
                                                              table, intercept)

class Reaction(object):
    def __init__(self, equation):
        self._equation = equation

        (self._reactants, self._products) = equation.split('=>')
        self._reactants = [x.strip() for x in self._reactants.split('+')]
        self._products = [x.strip() for x in self._products.split('+')]

    @property
    def reactants(self):
        return self._reactants

    @property
    def products(self):
        return self._products

    @property
    def equation(self):
        return '%s => %s' % (' + '.join(self.reactants), ' + '.join(self.products))

_inchi_key_regex = re.compile("^([0-9A-Z\-]+)$")

def _is_inchi_key(identifier):
    return len(identifier) == 27 and identifier[25] == '-' and \
        _inchi_key_regex.match(identifier)

def _find_using_cactus(identifier):
    params = {
        'cactus': identifier

    }
    molecule = girder_client.get('molecules/search', parameters=params)

    # Just pick the first
    if len(molecule) > 0:
        molecule = molecule[0]
        return GirderMolecule(molecule['_id'], molecule['cjson'])
    else:
        return None

def import_structure(smiles=None, inchi=None, cjson=None):

    params = {}
    if smiles:
        params['smiles'] = smiles
    elif inchi:
        params['inchi'] = inchi
    elif cjson:
        params['cjson'] = cjson
    else:
        raise Exception('SMILES, InChI, or CJson must be provided')

    molecule = girder_client.post('molecules', json=params)

    if not molecule:
        raise Exception('Molecule could not be imported with params', params)

    return GirderMolecule(molecule['_id'], molecule['cjson'])

def find_structure(identifier, image_name=None, input_parameters=None, input_geometry=None):
    is_calc_query = (image_name is not None and input_parameters is not None)

    # InChiKey?
    if _is_inchi_key(identifier):
        try:
            molecule = girder_client.get('molecules/inchikey/%s' % identifier)

            # Are we search for a specific calculation?
            if is_calc_query:
                # Look for optimization calculation
                cal = _fetch_calculation(molecule['_id'], image_name, input_parameters, input_geometry)

                if cal is not None:
                    # TODO We should probably pass in the full calculation
                    # so we don't have to fetch it again.
                    return CalculationResult(cal['_id'])
                else:
                    return None
            else:
                return GirderMolecule(molecule['_id'], molecule['cjson'])
        except HttpError as ex:
            if ex.status == 404:
                # Use cactus to try a lookup the structure
                molecule = _find_using_cactus(identifier)
            else:
                raise

    # If we have been provided basis, theory or functional and we haven't found
    # a calculation, then we are done.
    if is_calc_query:
        return None

    # Try cactus
    molecule = _find_using_cactus(identifier)

    if not molecule:
        raise Exception('No molecules found matching identifier: \'%s\'' % identifier)

    return molecule

def setup_reaction(equation):
    return Reaction(equation)

def compose_equation(equation, **vars):
    equation = Environment(loader=BaseLoader()).from_string(equation)

    return equation.render(**vars)

def load(data):
    if isinstance(data, dict):
        provider = CjsonProvider(data)
    elif isinstance(data, avogadro.core.Molecule):
        provider = AvogadroProvider(data)
    else:
        raise TypeError("Load accepts either a cjson dict, or an avogadro.core.Molecule")
    return Molecule(provider)

def monitor(results):
    taskflow_ids = []

    for result in results:
        if hasattr(result, '_properties'):
            props = result._properties
            if isinstance(props, AttributeInterceptor):
                props = props.unwrap()
            if isinstance(props, dict):
                taskflow_id = props.get('taskFlowId')
                if taskflow_id is not None:
                    taskflow_ids.append(taskflow_id)

    return _calculation_monitor(taskflow_ids)

def queue():
    if girder_host is None:
        import warnings
        warnings.warn("Cannot displaying pending calculations, the notebook is not running in a Girder environment")
        return

    queue = _fetch_or_create_queue()
    running = []
    pending = []

    for taskflow_id, status in queue['taskflows'].items():
        if status == 'pending':
            pending.append(taskflow_id)
        elif status == 'running':
            running.append(taskflow_id)

    taskflow_ids = running + pending

    return _calculation_monitor(taskflow_ids)

def _calculation_monitor(taskflow_ids):
    try:
        from .notebook import CalculationMonitor
        table = CalculationMonitor({
            'taskFlowIds': taskflow_ids,
            'girderToken': girder_client.token,
            'girderApiUrl': girder_api_url
        })
    except ImportError:
        # Outside notebook just print message
        table = 'Pending calculations .... '

    return table

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


def find_spectra(identifier, stype='IR', source='NIST'):
    """Find spectra in source database

    Parameters
    ----------
    identifier : str
        Inchi string.
    stype : str
        Type of spectrum to query.
    source : str
        Database to query. Supported are: 'NIST'
    """
    source = source.lower()

    params = {
            'inchi' : identifier,
            'spectrum_type' : stype,
            'source' : source
    }

    spectra = girder_client.get('experiments', parameters=params)

    return spectra

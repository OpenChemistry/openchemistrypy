from jinja2 import Environment, BaseLoader
from girder_client import GirderClient, HttpError
import re
import os
import urllib.parse
import json
import inspect
from jsonpath_rw import parse

from .utils import lookup_file
from avogadro import io, core

girder_host = os.environ.get('GIRDER_HOST')
girder_port = os.environ.get('GIRDER_PORT')
girder_scheme = os.environ.get('GIRDER_SCHEME', 'http')
girder_api_root = os.environ.get('GIRDER_API_ROOT', '/api/v1')
girder_api_key = os.environ.get('GIRDER_API_KEY')
girder_token = os.environ.get('GIRDER_TOKEN')
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

    girder_file = lookup_file(girder_client, jupyterhub_url)

# TODO Need to use basis and theory
def _fetch_calculation(molecule_id, type_=None, basis=None, theory=None, functional=None, code='nwchem'):
    parameters = {
        'moleculeId': molecule_id,
        'sortByTheory': True
    }

    if type_ is not None:
        parameters['calculationType'] = type_

    if functional is not None:
        parameters['functional'] = functional

    if theory is not None:
        parameters['theory'] = theory

    if basis is not None:
        parameters['basis'] = basis

    if code is not None:
        parameters['code'] = code

    calculations = girder_client.get('calculations', parameters)

    if len(calculations) < 1:
        return None

    # Pick the "best"
    return calculations[0]

def _nersc():
    return os.environ.get('OC_SITE') == 'NERSC'

def _submit_calculation(cluster_id, pending_calculation_id, optimize, calculation_types=None, code='nwchem'):
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

    code_params = {
        'nwchem': {
            'label': 'NWChem (version 27327)',
            'class': 'openchemistry.nwchem.NWChemTaskFlow'
        },
        'psi4': {
            'label': 'PSI4 (version 1.2.1)',
            'class': 'openchemistry.psi4.Psi4TaskFlow'
        }
    }

    if code not in code_params:
        raise Exception('Unable to submit calculation with the %s code' % code)

    code_label = code_params[code]['label']
    taskflow_class = code_params[code]['class']

    # Create the taskflow
    body = {
        'taskFlowClass': taskflow_class,
        'meta': {
            'code': code_label
        }
    }
    if calculation_types is not None:
        body['meta']['type'] = calculation_types

    taskflow = girder_client.post('taskflows', json=body)
    # Start the taskflow
    body = {
        'input': {
            'calculation': {
                '_id': pending_calculation_id
            },
            'optimize': optimize
        }
    }

    if cluster_id is not None:
        body['cluster'] = {
            '_id': cluster_id
        }
    elif _nersc():
        body['cluster'] = {
            'name': 'cori'
        }

    girder_client.put('taskflows/%s/start' % taskflow['_id'], json=body)

    # Set the pending calculation id in the meta data
    body = {
        'meta.calculationId': pending_calculation_id
    }
    girder_client.patch('taskflows/%s' % taskflow['_id'], json=body)

    return taskflow['_id']

def _fetch_taskflow_status(taskflow_id):
    r = girder_client.get('taskflows/%s/status' % taskflow_id)

    return r['status']

def _create_pending_calculation(molecule_id, type_, basis, theory, functional=None,
                                input_geometry=None, code='nwchem'):
    if not isinstance(type_, list):
        type_ = [type_]

    body = {
        'moleculeId': molecule_id,
        'cjson': None,
        'public': True,
        'properties': {
            'calculationTypes': type_,
            'basisSet': {
                'name': basis.lower()
            },
            'theory': theory.lower(),
            'pending': True,
            'code': code
        },
        'notebooks': [girder_file['_id']]
    }

    if input_geometry is not None:
        body['properties']['input'] = {
            'calculationId': input_geometry
        }

    if functional is not None:
        body['properties']['functional'] = functional.lower()

    calculation = girder_client.post('calculations', json=body)

    return calculation

def _fetch_or_submit_calculation(molecule_id, type_, basis, theory, functional=None, optimize=False,
                                 input_geometry=None, code='nwchem'):
    global cluster_id
    # If a functional has been provided default theory to dft
    if theory is None and functional is not None:
        theory = 'dft'

    calculation = _fetch_calculation(molecule_id, type_, basis, theory, functional, code=code)
    taskflow_id = None

    if calculation is None:
        calculation = _create_pending_calculation(molecule_id, type_, basis,
                                                  theory, functional, input_geometry=input_geometry, code=code)
        calculation_types = parse('properties.calculationTypes').find(calculation)[0].value
        taskflow_id = _submit_calculation(cluster_id, calculation['_id'], optimize, calculation_types, code=code)
        # Patch calculation to include taskflow id
        props = calculation['properties']
        props['taskFlowId'] = taskflow_id
        calculation = girder_client.put('calculations/%s/properties' % calculation['_id'], json=props)
    else:
        # If we all ready have a calculation tag it with this notebooks id
        body = {
            'notebooks': [girder_file['_id']]
        }
        girder_client.patch('calculations/%s/notebooks' % calculation['_id'],
                            json=body)

    return calculation

def _optimize(molecule_id, basis=None, theory=None, functional=None, input_geometry=None, code='nwchem'):
    type_ = 'optimization'
    calculation =  _fetch_or_submit_calculation(molecule_id, type_, basis, theory,
                                                functional, input_geometry=input_geometry, code=code)
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

def _frequencies(molecule_id,  optimize=False, basis=None, theory=None,
                 functional=None, input_geometry=None, code='nwchem'):
    type_ = 'vibrational'
    calculation = _fetch_or_submit_calculation(molecule_id, type_, basis, theory,
                                               functional, optimize, input_geometry, code=code)
    pending = parse('properties.pending').find(calculation)
    if pending:
        pending = pending[0].value

    taskflow_id = parse('properties.taskFlowId').find(calculation)
    if taskflow_id:
        taskflow_id = taskflow_id[0].value
    else:
        taskflow_id = None
    calculation = FrequenciesCalculationResult(calculation['_id'], calculation['properties'], molecule_id)

    if pending:
        calculation = PendingCalculationResultWrapper(calculation, taskflow_id)

    return calculation

def _energy(molecule_id, optimize=False, basis=None, theory=None, functional=None, input_geometry=None, code='nwchem'):
    type_ = 'energy'
    calculation = _fetch_or_submit_calculation(molecule_id, type_, basis, theory,
                                               functional, optimize, input_geometry, code=code)
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

class Molecule(object):
    def __init__(self, cjson):
        self._cjson = cjson

    @property
    def structure(self):
        return Structure(cjson=self._cjson)

    @property
    def orbitals(self):
        return Orbitals(self._cjson)


class GirderMolecule(Molecule):
    '''
    Derived version that allows calculations to be initiated on using Girder
    '''
    def __init__(self, _id, cjson=None):
        self._id = _id
        self._cjson = cjson

    def optimize(self, basis=None, theory=None, functional=None, code='nwchem'):
        return _optimize(self._id, basis, theory, functional, code=code)

    def frequencies(self, optimize=False, basis=None, theory=None, functional=None, code='nwchem'):
        return _frequencies(self._id, optimize, basis, theory, functional, code=code)

    def energy(self, optimize=False, basis=None, theory=None, functional=None, code='nwchem'):
        return _energy(self._id, optimize, basis, theory, functional, code=code)


class Structure(object):

    def __init__(self, calculation_result=None, cjson=None):
        self._calculation_result = calculation_result
        self._cjson = cjson

    def show(self, style='ball-stick'):

        try:
            from .notebook import CJSON
            if self._calculation_result:
                return CJSON(self._calculation_result._cjson, vibrational=False)
            else:
                return CJSON(self._cjson, vibrational=False)
        except ImportError:
            # Outside notebook print CJSON
            print(self._calculation_result._cjson)

    def url(self, style='ball-stick'):
        url = '%s/calculations/%s' % (app_base_url.rstrip('/'), self._calculation_result._id)
        try:
            from IPython.display import Markdown
            return Markdown('[%s](%s)' % (url, url))
        except ImportError:
            # Outside notebook just print the url
            print(url)


class Frequencies(object):

    def __init__(self, calculation_result):
        self._calculation_result = calculation_result

    def show(self, mode=None, animate_modes=False, spectrum=True):
        try:
            from .notebook import CJSON
            return CJSON(self._calculation_result._cjson, structure=animate_modes,
                         animate_mode=mode)
        except ImportError:
            # Outside notebook print CJSON
            print(self.table)

    def table(self):
        return self._calculation_result._vibrational_modes

class Orbitals(object):

    def __init__(self, cjson):
        self._cjson = cjson

    # Default implementation calculate mo locally
    def _calculate_mo(self, mo):
        if isinstance(mo, str):
            mo = mo.lower()
            if mo.lower() in ['homo', 'lumo']:
                # Electron count might be saved in several places...
                path_expressions = [
                    'orbitals.electronCount',
                    'basisSet.electronCount',
                    'properties.electronCount'
                ]
                matches = []
                for expr in path_expressions:
                    matches.extend(parse(expr).find(self._cjson))
                if len(matches) > 0:
                    electron_count = matches[0].value
                else:
                    raise Exception('Unable to access electronCount')

                # The index of the first orbital is 0, so homo needs to be
                # electron_count // 2 - 1
                if mo.lower() == 'homo':
                    mo = int(electron_count / 2) - 1
                elif mo.lower() == 'lumo':
                    mo = int(electron_count / 2)
            else:
                raise ValueError('Unsupported mo: %s' % mo)

        mol = core.Molecule()
        conv = io.FileFormatManager()
        conv.readString(mol, json.dumps(self._cjson), 'cjson')
        # Do some scaling of our spacing based on the size of the molecule.
        atomCount = mol.atomCount()
        spacing = 0.30
        if atomCount > 50:
            spacing = 0.5
        elif atomCount > 30:
            spacing = 0.4
        elif atomCount > 10:
            spacing = 0.33
        cube = mol.addCube()
        # Hard wiring spacing/padding for now, this could be exposed in future too.
        cube.setLimits(mol, spacing, 4)
        gaussian = core.GaussianSetTools(mol)
        gaussian.calculateMolecularOrbital(cube, mo)

        return json.loads(conv.writeString(mol, "cjson"))['cube']


    def show(self, mo='homo', iso=None):
        try:
            from .notebook import CJSON

            cjson_copy = self._cjson.copy()
            cjson_copy['cube'] = self._calculate_mo(mo)

            extra = {}
            if iso:
                extra['iso_value'] = iso

            return CJSON(cjson_copy, vibrational=False, mo=mo, **extra)
        except ImportError:
            # Outside notebook print CJSON
            print(self._cjson)


class CalculationResultOrbitals(Orbitals):

    def __init__(self, calculation_result):
        self._calculation_result = calculation_result
        super(CalculationResultOrbitals, self).__init__(calculation_result._cjson)

    def show(self, mo='homo', iso=None):
        # Save parameter to use in url
        self._last_mo = mo
        self._last_iso = iso

        return super(CalculationResultOrbitals, self).show(mo, iso)

    def url(self):
        url = '%s/calculations/%s' % (app_base_url.rstrip('/'), self._calculation_result._id)

        params = { }

        if self._last_mo is not None:
            params['mo'] = self._last_mo

        if self._last_iso is not None:
            params['iso'] = self._last_iso

        if params:
            url = '%s?%s' % (url, urllib.parse.urlencode(params))

        try:
            from IPython.display import Markdown
            return Markdown('[%s](%s)' % (url, url))
        except ImportError:
            # Outside notebook just print the url
            print(url)

    def _calculate_mo(self, mo):
        return self._calculation_result._cube(mo)['cube']


class CalculationResult(object):

    def __init__(self, _id=None, properties=None, molecule_id=None):
        self._id = _id
        self._cjson_ = None
        self._vibrational_modes_ = None
        self._orbitals = None
        self._molecule_id = molecule_id
        self.properties = properties

    @property
    def _cjson(self):
        if self._cjson_ is None:
            self._cjson_ = girder_client.get('calculations/%s/cjson' % self._id)

        return self._cjson_

    @property
    def _vibrational_modes(self):
        if self._vibrational_modes_ is None:
            self._vibrational_modes_ = girder_client.get('calculations/%s/vibrationalmodes' % self._id)

        return self._vibrational_modes_

    def _cube(self, mo):
        return girder_client.get('calculations/%s/cube/%s' % (self._id, mo))

    @property
    def structure(self):
        return Structure(self)

    @property
    def orbitals(self):
        if self._orbitals is None:
            self._orbitals = CalculationResultOrbitals(self)

        return self._orbitals

    def optimize(self, basis=None, theory=None, functional=None):
        return _optimize(self._molecule_id, basis, theory, functional, self._id)

    def frequencies(self, optimize=False, basis=None, theory=None, functional=None):
        return _frequencies(self._molecule_id, optimize, basis, theory,
                            functional, self._id)

    def energy(self, optimize=False, basis=None, theory=None, functional=None):
        return _energy(self._molecule_id, optimize, basis, theory, functional,
                       self._id)


class FrequenciesCalculationResult(CalculationResult):
    def __init__(self, _id=None, properties=None, molecule_id=None):
        super(FrequenciesCalculationResult, self).__init__(_id, properties,
                                                           molecule_id)

    @property
    def frequencies(self):
        return Frequencies(self)

class AttributeInterceptor(object):
    def __init__(self, wrapped, value, intercept_func=lambda : True):
        self._wrapped = wrapped
        self._value = value
        self._intercept_func = intercept_func


    def __getattribute__(self, name):
        # Use object's implementation to get attributes, otherwise
        # we will get recursion
        _wrapped = object.__getattribute__(self, '_wrapped')
        _value = object.__getattribute__(self, '_value')
        intercept_func = object.__getattribute__(self, '_intercept_func')

        if intercept_func() and hasattr(_wrapped, name):
            attr = object.__getattribute__(_wrapped, name)
            if inspect.ismethod(attr):
                def pending(*args, **kwargs):
                    return _value
                return pending
            else:
                return AttributeInterceptor(attr, _value, intercept_func)
        else:
            return object.__getattribute__(_wrapped, name)

class PendingCalculationResultWrapper(AttributeInterceptor):
    def __init__(self, calculation, taskflow_id=None):
        try:
            from .notebook import CalculationMonitor
            if taskflow_id is None:
                taskflow_id = calculation.properties['taskFlowId']

            table = CalculationMonitor({
                'taskFlowIds': [taskflow_id],
                'girderToken': girder_client.token
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

    def _fetch_free_energy(self, formula, basis=None, theory=None, functional=None):
        """
        :return A tuple containing the total energy and zero point energy.
        """

        # First fetch the molecule using the formula
        params = {
            'formula': formula
        }
        mol = girder_client.get('molecules/search', parameters=params)

        if len(mol) < 1:
            raise Exception('No molecules found for formula \'%s\'' % formula)

        # TODO Might we get more than one molecule with the same formula?

        # Now fetch the calculations, TODO what types should we select
        calculation = _fetch_or_submit_calculation(mol[0]['_id'], ['vibrational',
                                                                   'energy'],
                                                   basis, theory, functional)

        pending = parse('properties.pending').find(calculation)
        if pending:
            pending = pending[0].value

        if pending:
            taskflow_id = parse('properties.taskFlowId').find(calculation)
            taskflow_id = taskflow_id[0].value
            return CalculationResult(calculation['_id'], calculation['properties'])

        calcs = parse('properties.calculations').find(calculation)
        if not calcs:
            raise Exception('No calculations found for \'%s\'' % formula)

        calcs = calcs[0].value

        # TODO for now just select the first, which calculations should we
        # favor? For now just search for the first that has both energies
        selected_calc = None
        for calc in calcs:
            if 'totalEnergy' in calc and 'zeroPointEnergyCorrection' in calc:
                selected_calc = calc
                break

        return (selected_calc['totalEnergy'], selected_calc['zeroPointEnergyCorrection'])

    def free_energy(self, basis=None, theory=None, functional=None):

        def _sum(formulas):
            pending_calculations = []
            energy = 0
            for formula in formulas:
                free_energy = self._fetch_free_energy(formula, basis, theory, functional)

                if isinstance(free_energy, CalculationResult):
                    pending_calculations.append(free_energy)
                else:
                    (total_energy, zero_point_energy) = free_energy
                    energy += total_energy['value'] + zero_point_energy['value']

            if len(pending_calculations) == 0:
                return energy
            else:
                return pending_calculations

        reactants_energy_total = _sum(self.reactants)
        products_energy_total = _sum(self.products)

        if isinstance(reactants_energy_total, list) or isinstance(products_energy_total, list):
            pending_calculations = []
            if isinstance(reactants_energy_total, list):
                pending_calculations += reactants_energy_total

            if isinstance(products_energy_total, list):
                pending_calculations += products_energy_total

            return pending_calculations

        free_energy = products_energy_total - reactants_energy_total
        # Convert to kJ/mol
        free_energy = free_energy * 2625.5

        return free_energy

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

def find_structure(identifier, basis=None, theory=None, functional=None, code='nwchem'):
    is_calc_query = (basis is not None or theory is not None
                     or functional is not None)

    # InChiKey?
    if _is_inchi_key(identifier):
        try:
            molecule = girder_client.get('molecules/inchikey/%s' % identifier)

            # Are we search for a specific calculation?
            if is_calc_query:
                # Look for optimization calculation
                cal = _fetch_calculation(molecule['_id'], 'optimization',
                                         basis, theory, functional, code=code)

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

def show_free_energies(reactions, basis=None, theory=None, functional=None):
    free_energy_chart_data = {
        'freeEnergy': [],
        'reaction': []
    }

    pending_calculations = []
    for reaction in reactions:
        equation = reaction.equation
        free_energy = reaction.free_energy(basis, theory, functional)

        if isinstance(free_energy, list):
            pending_calculations += free_energy

        free_energy_chart_data['reaction'].append(equation)
        free_energy_chart_data['freeEnergy'].append(free_energy)

    if pending_calculations:
        taskflow_ids = [ cal.properties['taskFlowId'] for cal in pending_calculations]
        # Remove duplicates
        taskflow_ids = list(set(taskflow_ids))

        try:
            from .notebook import CalculationMonitor
            table = CalculationMonitor({
                    'taskFlowIds': taskflow_ids,
                    'girderToken': girder_client.token
                })
        except ImportError:
            # Outside notebook just print message
            table = 'Pending calculations .... '

        return table;

    try:
        from .notebook import FreeEnergy

        return FreeEnergy(free_energy_chart_data)
    except ImportError:
        # Outside notebook print the data
        print(free_energy_chart_data)

def load(cjson):
    return Molecule(cjson)


from jinja2 import Environment, BaseLoader
from girder_client import GirderClient, HttpError
import re
import os
import urllib.parse
import inspect
from jsonpath_rw import parse

girder_host = os.environ.get('GIRDER_HOST')
girder_port = os.environ.get('GIRDER_PORT')
girder_api_key = os.environ.get('GIRDER_API_KEY')
app_base_url = os.environ.get('APP_BASE_URL')
cluster_id = os.environ.get('CLUSTER_ID')

if girder_host:
    girder_client = GirderClient(host=girder_host, port=girder_port,
                                 scheme='http')
    girder_client.authenticate(apiKey=girder_api_key)

# TODO Need to use basis and theory
def _fetch_calculation(molecule_id, type_=None, theory=None, basis=None):
    parameters = {
        'moleculeId': molecule_id,
    }

    if type_ is not None:
        parameters['calculationType'] = type_

    calculations = girder_client.get('calculations', parameters)

    if len(calculations) < 1:
        return None

    # For now just pick the first
    return calculations[0]

def _submit_calculation(cluster_id, pending_calculation_id):
    if cluster_id is None:
        raise Exception('Unable to submit calculation, no cluster configured.')

    # Create the taskflow
    body = {
        'taskFlowClass': 'openchemistry.nwchem.NWChemTaskFlow'
    }

    taskflow = girder_client.post('taskflows', json=body)
    # Start the taskflow
    body = {
        'cluster': {
            '_id': cluster_id
        },
        'input': {
            'calculation': {
                '_id': pending_calculation_id
            }
        }
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

def _create_pending_calculation(molecule_id, type, basis, theory):
    body = {
        'moleculeId': molecule_id,
        'cjson': None,
        'public': True,
        'properties': {
            'calculationTypes': [type],
            'basis': basis,
            'theory': theory,
            'pending': True
        }
    }

    calculation = girder_client.post('calculations', json=body)

    return calculation

def _fetch_or_submit_calculation(molecule_id, type_, basis, theory):
    global cluster_id
    calculation = _fetch_calculation(molecule_id, type_, basis, theory)
    taskflow_id = None

    if calculation is None:
        calculation = _create_pending_calculation(molecule_id, type_, basis,
                                                  theory)
        taskflow_id = _submit_calculation(cluster_id, calculation['_id'])
        # Patch calculation to include taskflow id
        props = calculation['properties']
        props['taskFlowId'] = taskflow_id
        calculation = girder_client.put('calculations/%s/properties' % calculation['_id'], json=props)

    return calculation

class Molecule(object):
    def __init__(self, _id, cjson=None):
        self._id = _id
        self._cjson = cjson

    def optimize(self, basis=None, theory=None):
        type_ = 'optimization'
        calculation =  _fetch_or_submit_calculation(self._id, type_, basis, theory)
        pending = parse('properties.pending').find(calculation)
        if pending:
            pending = pending[0].value

        taskflow_id = parse('properties.taskFlowId').find(calculation)
        taskflow_id = taskflow_id[0].value
        calculation = CalculationResult(calculation['_id'], calculation['properties'])

        if pending:
            calculation = PendingCalculationResultWrapper(calculation, taskflow_id)

        return calculation


    def frequencies(self, basis=None, theory=None):
        type_ = 'vibrational'
        calculation = _fetch_or_submit_calculation(self._id, type_, basis, theory)
        pending = parse('properties.pending').find(calculation)
        if pending:
            pending = pending[0].value

        taskflow_id = parse('properties.taskFlowId').find(calculation)
        taskflow_id = taskflow_id[0].value
        calculation = FrequenciesCalculationResult(calculation['_id'], calculation['properties'])

        if pending:
            calculation = PendingCalculationResultWrapper(calculation, taskflow_id)

        return calculation

    def energy(self, basis=None, theory=None):
        return CalculationResult()

    def optimize_frequencies(self, basis=None, theory=None):
        return FrequenciesCalculationResult()

    @property
    def structure(self):
        return Structure(cjson=self._cjson)

class Structure(object):

    def __init__(self, calculation_result=None, cjson=None):
        self._calculation_result = calculation_result
        self._cjson = cjson

    def show(self, style='ball-stick'):

        try:
            from jupyterlab_cjson import CJSON
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
            from jupyterlab_cjson import CJSON
            return CJSON(self._calculation_result._cjson, structure=animate_modes,
                         animate_mode=mode)
        except ImportError:
            # Outside notebook print CJSON
            print(self.table)

    @property
    def table(self):
        return self._calculation_result._vibrational_modes

class Orbitals(object):

    def __init__(self, calculation_result):
        self._calculation_result = calculation_result

    def show(self, mo='homo', iso=None):
        try:
            from jupyterlab_cjson import CJSON

            cjson_copy = self._calculation_result._cjson.copy()
            cjson_copy['cube'] = self._calculation_result._cube(mo)['cube']

            extra = {}
            if iso:
                extra['iso_surfaces'] = [{
                    'value': iso,
                    'color': 'blue',
                    'opacity': 0.9,
                }, {
                    'value': -iso,
                    'color': 'red',
                    'opacity': 0.9
                }];

            #self._calculation_result._cube(mo)

            # Save parameter to use in url
            self._last_mo = mo
            self._last_iso = iso

            return CJSON(cjson_copy, vibrational=False, mo=mo,
                         calculation_id=self._calculation_result._id, **extra)
        except ImportError:
            # Outside notebook print CJSON
            print(self._calculation_result._cjson)

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

class CalculationResult(object):

    def __init__(self, _id=None, properties=None):
        self._id = _id
        self._cjson_ = None
        self._vibrational_modes_ = None
        self._orbitals = None
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
            self._orbitals = Orbitals(self)

        return self._orbitals

class FrequenciesCalculationResult(CalculationResult):
    def __init__(self, _id=None, properties=None):
        super(FrequenciesCalculationResult, self).__init__(_id, properties)

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
            from jupyterlab_cjson import CalculationMonitor
            if taskflow_id is None:
                taskflow_id = calculation.properties['taskFlowId']

            print('TaskFlow id %s' % taskflow_id)

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

    def _fetch_free_energy(self, formula, basis=None, theory=None):
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
        cjson = _fetch_calculation(mol[0]['_id'])

        calcs = parse('properties.calculations').find(cjson)
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

    def free_energy(self, basis=None, theory=None):

        def _sum(formulas):
            energy = 0
            for formula in formulas:
                (total_energy, zero_point_energy) = self._fetch_free_energy(formula)
                energy += total_energy['value'] + zero_point_energy['value']

            return energy

        reactants_energy_total = _sum(self.reactants)
        products_energy_total = _sum(self.products)

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
        return Molecule(molecule['_id'], molecule['cjson'])
    else:
        return None

def find_structure(identifier):

    # InChiKey?
    if _is_inchi_key(identifier):
        try:
            molecule = girder_client.get('molecules/inchikey/%s' % identifier)

            return Molecule(molecule['_id'], molecule['cjson'])
        except HttpError as ex:
            if ex.status == 404:
                # Use cactus to try a lookup the structure
                molecule = _find_using_cactus(identifier)
            else:
                raise
    else:
        molecule = _find_using_cactus(identifier)


    if not molecule:
        raise Exception('No molecules found matching identifier: \'%s\'' % identifier)

    return molecule


def setup_reaction(equation):
    return Reaction(equation)

def compose_equation(equation, **vars):
    equation = Environment(loader=BaseLoader()).from_string(equation)

    return equation.render(**vars)

def show_free_energies(reactions, basis=None, theory=None):
    free_energy_chart_data = {
        'freeEnergy': [],
        'reaction': []
    }

    for reaction in reactions:
        equation = reaction.equation
        free_energy = reaction.free_energy(basis, theory)

        free_energy_chart_data['reaction'].append(equation)
        free_energy_chart_data['freeEnergy'].append(free_energy)

    try:
        from jupyterlab_cjson import FreeEnergy

        return FreeEnergy(free_energy_chart_data)
    except ImportError:
        # Outside notebook print the data
        print(free_energy_chart_data)

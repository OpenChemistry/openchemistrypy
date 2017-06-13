from jinja2 import Environment, BaseLoader
from girder_client import GirderClient, HttpError
import re
import os
from jsonpath_rw import parse

girder_host = os.environ['GIRDER_HOST']
girder_port = os.environ['GIRDER_PORT']
girder_api_key = os.environ['GIRDER_API_KEY']
#app_base_url = os.environ['APP_BASE_URL']


girder_client = GirderClient(host=girder_host, port=girder_port,
                              scheme='http')

girder_client.authenticate(apiKey=girder_api_key)

# TODO Need to add basis and theory
def _fetch_calculation(molecule_id, type=None):
    parameters = {
        'moleculeId': molecule_id,
    }

    if type is not None:
        parameters['calculationType'] = type

    calculations = girder_client.get('calculations', parameters)


    if len(calculations) < 1:
        # TODO Start the appropriate calculation :-)
        return None

    # For now just pick the first
    return calculations[0]


class Molecule(object):
    def __init__(self, _id):
        self._id = _id

    def optimize(self, basis=None, theory=None):
        calculation = _fetch_calculation(self._id, type='optimization')

        return CalculationResult(calculation['_id'])

    def frequencies(self, basis=None, theory=None):
        calculation = _fetch_calculation(self._id, type='vibrational')

        return FrequenciesCalculationResult(calculation['_id'])

    def energy(self, basis=None, theory=None):
        return CalculationResult()

    def optimize_frequencies(self, basis=None, theory=None):
        return FrequenciesCalculationResult()


class Structure(object):

    def __init__(self, calculation_result):
        self._calculation_result = calculation_result

    def show(self, style='ball-stick'):

        try:
            from jupyterlab_cjson import CJSON
            return CJSON(self._calculation_result._cjson, vibrational=False)
        except ImportError:
            # Outside notebook print CJSON
            print(self._calculation_result._cjson)

    def url(self, style='ball-stick'):
        '%s/molecules/%s' % (app_base_url.rstrip('/'), self._calculation_result._id)


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

            self._calculation_result._cube(mo)

            return CJSON(cjson_copy, vibrational=False, **extra)
        except ImportError:
            # Outside notebook print CJSON
            print(self._calculation_result._cjson)


class CalculationResult(object):

    def __init__(self, _id):
        self._id = _id
        self._cjson_ = None
        self._vibrational_modes_ = None

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
        return Orbitals(self)

class FrequenciesCalculationResult(CalculationResult):
    def __init__(self, _id):
        super(FrequenciesCalculationResult, self).__init__(_id)

    @property
    def frequencies(self):
        return Frequencies(self)

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

def find_structure(identifier):

    # InChiKey?
    if _is_inchi_key(identifier):
        try:
            molecule = girder_client.get('molecules/inchikey/%s' % identifier)
        except HttpError as ex:
            if ex.status == 404:
                return None
            else:
                raise
    else:
        raise Exception('Identifier not supported.')

    return Molecule(molecule['_id'])

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

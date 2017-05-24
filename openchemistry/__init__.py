from jinja2 import Environment, BaseLoader
from girder_client import GirderClient, HttpError
import re
import os

girder_host = os.environ['GIRDER_HOST']
girder_port = os.environ['GIRDER_PORT']
girder_api_key = os.environ['GIRDER_API_KEY']


girder_client = GirderClient(host=girder_host, port=girder_port,
                              scheme='http')

girder_client.authenticate(apiKey=girder_api_key)


class Molecule(object):
    def __init__(self, _id):
        self._id = _id

    def optimize(self, basis=None, theory=None):

        parameters = {
            'moleculeId': self._id
        }


        calculations = girder_client.get('calculations', parameters)

        if len(calculations) < 1:
            # TODO Start the appropriate calculation :-)
            return None

        # For now just pick the first
        calculation = calculations[0]

        return CalculationResult(calculation['_id'])

    def frequencies(self, basis=None, theory=None):
        return FrequenciesCalculationResult()

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

class Frequencies(object):
    def show(self, animate_mode=None, animate_modes=False, spectrum=True):
        pass

    @property
    def table(self):
        return {
            'intensities': [],
            'frequency': [],
            'mode': []
        }

class Orbitals(object):
    def show(self, mo='homo'):
        pass

class CalculationResult(object):

    def __init__(self, _id):
        self._id = _id
        self._cjson_ = None

    @property
    def _cjson(self):
        if self._cjson_ is None:
            self._cjson_ = girder_client.get('calculations/%s/cjson' % self._id)

        return self._cjson_

    @property
    def structure(self):
        return Structure(self)

    @property
    def orbitals(self):
        return Orbitals()

class FrequenciesCalculationResult(CalculationResult):
    @property
    def frequencies(self):
        return Frequencies()

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

    def free_energy(self, basis=None, theory=None):
        return [1, 2, 3]

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
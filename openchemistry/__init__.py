from jinja2 import Environment, BaseLoader

class Molecule(object):
    def optimize(self, basis=None, theory=None):
        return CalculationResult()

    def frequencies(self, basis=None, theory=None):
        return FrequenciesCalculationResult()

    def energy(self, basis=None, theory=None):
        return CalculationResult()

    def optimize_frequencies(self, basis=None, theory=None):
        return FrequenciesCalculationResult()


class Structure(object):
    def show(self, style='ball-stick'):
        pass

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
    def show(self):
        pass

class CalculationResult(object):
    @property
    def structure(self):
        return Structure()

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


def find_structure(identifier):
    return Molecule()

def setup_reaction(equation):
    return Reaction(equation)

def compose_equation(equation, **vars):
    equation = Environment(loader=BaseLoader()).from_string(equation)

    return equation.render(**vars)
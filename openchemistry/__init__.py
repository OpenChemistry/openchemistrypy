from jinja2 import Environment, BaseLoader

class Molecule(object):
    def optimize(self, basis, theory):
        return CalculationResult()

class Structure(object):
    def show(self, style):
        pass

class VibrationalModes(object):
    def show(self):
        pass

class Orbitals(object):
    def show(self):
        pass

class CalculationResult(object):
    @property
    def structure(self):
        return Structure()

    @property
    def vibrational_modes(self):
        return VibrationalModes()

    @property
    def orbitals(self):
        return Orbitals()

    def frequencies(self):
        return self

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

    def free_energy(self, basis, theory):
        return [1, 2, 3]


def get_structure(identifier):
    return Molecule()

def setup_reaction(equation):
    return Reaction(equation)

def compose_equation(equation, **vars):
    equation = Environment(loader=BaseLoader()).from_string(equation)

    return equation.render(**vars)
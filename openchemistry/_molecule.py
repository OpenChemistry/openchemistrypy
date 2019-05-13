from ._visualization import Structure, Orbitals, Properties, Vibrations

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

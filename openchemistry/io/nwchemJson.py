import json
from avogadro.core import Molecule
from avogadro.io import FileFormatManager
from jsonpath_rw import parse
from .base import BaseReader
from .constants import HARTREE_TO_J_MOL

class NWChemJsonReader(BaseReader):

    def read(self):
        str_data = self._file.read()
        mol = Molecule()
        conv = FileFormatManager()
        conv.read_string(mol, str_data, 'json')
        cjson_str = conv.write_string(mol, 'cjson')
        cjson = json.loads(cjson_str)
        # Copy some calculated properties
        data = json.loads(str_data)
        energy = parse('simulation.calculations[0].calculationResults.totalEnergy.value').find(data)
        if len(energy) == 1:
            energy = energy[0].value
            cjson.setdefault('properties', {})['totalEnergy'] = energy * HARTREE_TO_J_MOL
        return cjson

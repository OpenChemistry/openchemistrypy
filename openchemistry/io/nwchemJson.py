import json
from avogadro.core import Molecule
from avogadro.io import FileFormatManager
from .base import BaseReader

class NWChemJsonReader(BaseReader):

    def read(self):
        str_data = self._file.read()
        mol = Molecule()
        conv = FileFormatManager()
        conv.read_string(mol, str_data, 'json')
        cjson_str = conv.write_string(mol, 'cjson')
        cjson = json.loads(cjson_str)
        return cjson

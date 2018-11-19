import json
from avogadro.core import Molecule
from avogadro.io import FileFormatManager
from .base import BaseReader

class NWChemJsonReader(BaseReader):

    @staticmethod
    def read(f):
        str_data = f.read()
        mol = Molecule()
        conv = FileFormatManager()
        conv.readString(mol, str_data, 'json')
        cjson_str = conv.writeString(mol, 'cjson')
        cjson = json.loads(cjson_str)
        return cjson

from avogadro.core import Molecule
from avogadro.io import FileFormatManager

def convert_str(str_data, in_format, out_format):
    mol = Molecule()
    conv = FileFormatManager()
    conv.readString(mol, str_data, in_format)
    return conv.writeString(mol, out_format)

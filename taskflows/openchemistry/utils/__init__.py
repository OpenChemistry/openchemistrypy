import json
from . import avogadro
from . import openbabel

def cjson_to_xyz(cjson):
    xyz = avogadro.convert_str(json.dumps(cjson), 'cjson', 'xyz')
    # remove the first two lines in the xyz file
    # (i.e. number of atom and optional comment)
    xyz = xyz.split('\n')[2:]
    xyz = '\n  '.join(xyz)
    return xyz

def cjson_to_smiles(cjson):
    cml = avogadro.convert_str(cjson, 'cjson', 'cml')
    smiles, mime = openbabel.convert_str(cml, 'cml', 'smiles')
    return smiles

import json
from . import avogadro

def cjson_to_xyz(cjson):
    xyz = avogadro.convert_str(json.dumps(cjson), 'cjson', 'xyz')
    # remove the first two lines in the xyz file
    # (i.e. number of atom and optional comment)
    xyz = xyz.split('\n')[2:]
    xyz = '\n  '.join(xyz)
    return xyz

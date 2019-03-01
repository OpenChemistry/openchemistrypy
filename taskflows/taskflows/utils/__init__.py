import json
from . import avogadro

def cjson_to_xyz(cjson):
    xyz = avogadro.convert_str(json.dumps(cjson), 'cjson', 'xyz')
    return xyz

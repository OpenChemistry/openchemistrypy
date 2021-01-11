import json
import os

from .base import BaseReader
from .constants import EV_TO_J_MOL
from .utils import _cleanup_cclib_cjson

class Cp2kReader(BaseReader):

    def read(self):
        import cclib

        cjson = {}

        project_name = None
        energy = None

        with open(self._file, 'r') as rf:
            for line in rf:
                line = line.strip()

                if line.startswith('GLOBAL| Project name'):
                    project_name = line.split()[-1]

                if line.startswith('ENERGY|'):
                    # This might occur multiple times in the file.
                    # Each will be over-written by the last.
                    energy = float(line.split()[-1]) * EV_TO_J_MOL

        if project_name:
            dir_name = os.path.dirname(self._file)
            geometry_file = dir_name + '/' + project_name + '-pos-1.xyz'
            if os.path.exists(geometry_file):
                data = cclib.io.ccread(geometry_file)
                cjson = json.loads(cclib.ccwrite(data, outputtype='cjson',))
                # Cleanup original cjson
                _cleanup_cclib_cjson(cjson)

        if energy:
            cjson.setdefault('properties', {})['totalEnergy'] = energy

        return cjson

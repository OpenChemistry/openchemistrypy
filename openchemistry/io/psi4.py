import json

from .utils import _cclib_to_cjson_basis, _cclib_to_cjson_mocoeffs, _cclib_to_cjson_vibdisps
from .base import BaseReader
from .constants import EV_TO_J_MOL

class Psi4Reader(BaseReader):

    def read(self):
        import cclib

        data = cclib.io.ccread(self._file)
        cjson = json.loads(cclib.ccwrite(data, outputtype='cjson',))

        # The cjson produced by cclib is not directly usable in our platform
        # Basis, moCoefficients, and normal modes need to be further converted

        # Cleanup original cjson
        if 'orbitals' in cjson['atoms']:
            del cjson['atoms']['orbitals']
        if 'properties' in cjson:
            del cjson['properties']
        if 'vibrations' in cjson:
            del cjson['vibrations']
        if 'optimization' in cjson:
            del cjson['optimization']
        if 'diagram' in cjson:
            del cjson['diagram']
        if 'inchi' in cjson:
            del cjson['inchi']
        if 'inchikey' in cjson:
            del cjson['inchikey']
        if 'smiles' in cjson:
            del cjson['smiles']

        # Convert basis set info
        if hasattr(data, 'gbasis'):
            basis = _cclib_to_cjson_basis(data.gbasis)
            cjson['basisSet'] = basis

        # Convert mo coefficients
        if hasattr(data, 'mocoeffs'):
            mocoeffs = _cclib_to_cjson_mocoeffs(data.mocoeffs)
            cjson.setdefault('orbitals', {})['moCoefficients'] = mocoeffs
        
        # Convert mo energies
        if hasattr(data, 'moenergies'):
            moenergies = list(data.moenergies[-1])
            cjson.setdefault('orbitals', {})['energies'] = moenergies

        if hasattr(data, 'nelectrons'):
            cjson.setdefault('orbitals', {})['electronCount'] = int(data.nelectrons)
        
        if hasattr(data, 'homos') and hasattr(data, 'nmo'):
            homos = data.homos
            nmo = data.nmo
            if len(homos) == 1:
                occupations = [2 if i <= homos[0] else 0 for i in range(nmo)]
                cjson.setdefault('orbitals', {})['occupations'] = occupations

        # Convert normal modes
        if hasattr(data, 'vibfreqs'):
            vibfreqs = list(data.vibfreqs)
            cjson.setdefault('vibrations', {})['frequencies'] = vibfreqs

        if hasattr(data, 'vibdisps'):
            vibdisps = _cclib_to_cjson_vibdisps(data.vibdisps)
            cjson.setdefault('vibrations', {})['eigenVectors'] = vibdisps

        # Add a placeholder intensities array
        if 'vibrations' in cjson and 'frequencies' in cjson['vibrations']:
            if 'intensities'  not in cjson['vibrations']:
                cjson['vibrations']['intensities'] = [1 for i in range(len(cjson['vibrations']['frequencies']))]
            if 'modes'  not in cjson['vibrations']:
                cjson['vibrations']['modes'] = [i + 1 for i in range(len(cjson['vibrations']['frequencies']))]


        # Convert calculation metadata
        if hasattr(data, 'metadata'):
            metadata = data.metadata
            if 'basis_set' in metadata:
                cjson.setdefault('metadata', {})['basisSet'] = metadata['basis_set'].lower()
            if 'functional' in metadata:
                cjson.setdefault('metadata', {})['functional'] = metadata['functional'].lower()
            if 'methods' in metadata and len(metadata['methods']) > 0:
                cjson.setdefault('metadata', {})['theory'] = metadata['methods'][0].lower()

        # Add calculated properties
        if hasattr(data, 'scfenergies'):
            if len(data.scfenergies) > 0:
                energy = data.scfenergies[-1] * EV_TO_J_MOL
                cjson.setdefault('properties', {})['totalEnergy'] = energy

        return cjson

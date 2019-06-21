from .utils import (
    _cclib_to_cjson_basis,
    _cclib_to_cjson_vibdisps,
)
from .base import BaseReader
from .constants import EV_TO_J_MOL


class OrcaReader(BaseReader):
    """A class to parse orca output files and dump a chemical json file"""

    def read(self):
        """Read orca output file"""
        import cclib
        from pybel import readfile

        molecule = next(readfile("orca", self._file))

        atom_numbers = []
        coordinates = []

        for atom in molecule:
            atom_number = atom.atomicnum
            atom_numbers.append(atom_number)
            for coord in atom.coords:
                coordinates.append(coord)

        molecular_mass = molecule.exactmass

        cjson = {
            "chemical json": 0,
            "atoms": {
                "elements": {"number": atom_numbers},
                "coords": {"3d": coordinates},
            },
            "properties": {"molecular mass": molecular_mass},
        }

        data = cclib.io.ccread(self._file)

        # Add calculated properties
        if hasattr(data, "scfenergies"):
            if len(data.scfenergies) > 0:
                energy = data.scfenergies[-1] * EV_TO_J_MOL
                cjson.setdefault("properties", {})["totalEnergy"] = energy

        if hasattr(data, "gbasis"):
            basis = _cclib_to_cjson_basis(data.gbasis)
            cjson["basisSet"] = basis

        if hasattr(data, "vibfreqs"):
            vibfreqs = list(data.vibfreqs)
            cjson.setdefault("vibrations", {})["frequencies"] = vibfreqs

        if hasattr(data, "vibdisps"):
            vibdisps = _cclib_to_cjson_vibdisps(data.vibdisps)
            cjson.setdefault("vibrations", {})["eigenVectors"] = vibdisps

        # Add a placeholder intensities array
        if "vibrations" in cjson and "frequencies" in cjson["vibrations"]:
            if "intensities" not in cjson["vibrations"]:
                cjson["vibrations"]["intensities"] = [
                    1 for i in range(len(cjson["vibrations"]["frequencies"]))
                ]
            if "modes" not in cjson["vibrations"]:
                cjson["vibrations"]["modes"] = [
                    i + 1 for i in range(len(cjson["vibrations"]["frequencies"]))
                ]

        return cjson

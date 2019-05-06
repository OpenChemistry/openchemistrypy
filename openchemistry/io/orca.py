import json
import openchemistry as oc
from avogadro.core import Molecule
from avogadro.io import FileFormatManager
from jsonpath_rw import parse
from .base import BaseReader
from .constants import EV_TO_J_MOL
from .utils import _cclib_to_cjson_basis


def OrcaReader(BaseReader):
    """A class to parse orca output files and dump a chemical json file"""

    def read(self, output_file):
        """Read orca output file"""
        import openbabel
        import cclib
        from pybel import readfile

        molecule = next(readfile('orca', output_file))
        obconv = openbabel.OBConversion()

        # Generation of SMILES string
        obconv.SetOutFormat(str("smi"))
        obconv.AddOption(str("a"), openbabel.OBConversion.OUTOPTIONS)
        smiles = obconv.WriteString(molecule.OBMol).split()[0]

        # Generation of Inchi
        obconv.SetOutFormat(str("inchi"))
        obconv.AddOption(str("a"), openbabel.OBConversion.OUTOPTIONS)
        inchi_text = obconv.WriteString(molecule.OBMol).split()[0]

        # Generation of Inchi Key
        obconv.SetOutFormat(str("inchikey"))
        obconv.AddOption(str("a"), openbabel.OBConversion.OUTOPTIONS)
        inchi_key = obconv.WriteString(molecule.OBMol).split()[0]

        # Molecular formula
        molecule_formula = molecule.formula

        # Number of atoms
        atomCount = len(molecule.atoms)

        atom_numbers = []
        coordinates = []

        for atom in molecule:
            atom_number = atom.atomicnum
            atom_numbers.append(atom_number)
            for coord in atom.coords:
                coordinates.append(coord)

        # Heavy atoms are those different from Hydrogen
        remove_atom_number = 1
        heavyAtomCount = len(list(filter((remove_atom_number).__ne__,
                                         atom_numbers)))
        molecular_mass = molecule.exactmass

        cjson = {
            "chemical json": 0,
            "name": molecule_formula,
            "inchi": inchi_text,
            "formula": molecule_formula,
            "atoms": {
                "elements": {
                    "number": atom_numbers
                },
                "coords": {
                    "3d": coordinates
                }
                     },
            "properties": {
                "molecular mass": molecular_mass
                          }
            }

        energy = get_energy(output_file)

        cjson['properties']['totalEnergy'] = energy

        data = cclib.io.ccread(output_file)

        if hasattr(data, 'gbasis'):
            basis = oc.io.utils._cclib_to_cjson_basis(data.gbasis)
            cjson['basisSet'] = basis

        if hasattr(data, 'vibfreqs'):
            vibfreqs = list(data.vibfreqs)
            cjson.setdefault('vibrations', {})['frequencies'] = vibfreqs

        if hasattr(data, 'vibdisps'):
            vibdisps = _cclib_to_cjson_vibdisps(data.vibdisps)
            cjson.setdefault('vibrations', {})['eigenVectors'] = vibdisps

        # Add a placeholder intensities array
        if 'vibrations' in cjson and 'frequencies' in cjson['vibrations']:
            if 'intensities' not in cjson['vibrations']:
                cjson['vibrations']['intensities'] = \
                    [1 for i in range(len(cjson['vibrations']['frequencies']))]
            if 'modes' not in cjson['vibrations']:
                cjson['vibrations']['modes'] = \
                    [i + 1 for i in
                     range(len(cjson['vibrations']['frequencies']))]
        return cjson


def get_energy(output_file):
    """Get energy from orca output"""

    output_file = open(output_file, 'r')
    output_file = output_file.readlines()

    energies = []

    for line in output_file:
        if 'Total Energy' in line:
            energies.append(line.split())

    energy = float(energies[-1][-2]) * EV_TO_J_MOL

    return energy

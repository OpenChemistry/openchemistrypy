import os
import jinja2
from taskflows import OpenChemistryTaskFlow
from taskflows.utils import cjson_to_xyz

class Psi4TaskFlow(OpenChemistryTaskFlow):

    @property
    def code_label(self):
        return 'psi4'

    @property
    def docker_image(self):
        return 'openchemistry/psi4:latest'

    def input_generator(self, params, cjson, tmp_file):
        xyz_structure = cjson_to_xyz(cjson)

        optimization = params.get('optimization', None)
        vibrational = params.get('vibrational', None)
        charge = params.get('charge', 0)
        multiplicity = params.get('multiplicity', 1)
        theory = params.get('theory', 'scf')
        functional = params.get('functional', 'b3lyp')
        basis = params.get('basis', 'cc-pvdz')

        if optimization is not None:
            task = 'optimize'
        elif vibrational is not None:
            task = 'frequency'
        else:
            task = 'energy'

        if theory.lower() == 'dft':
            _theory = functional
            reference = 'ks'
        else:
            _theory = theory
            reference = 'hf'

        if multiplicity == 1:
            reference = 'r' + reference
        else:
            reference = 'u' + reference

        context = {
            'task': task,
            'theory': _theory,
            'reference': reference,
            'charge': charge,
            'multiplicity': multiplicity,
            'basis': basis
        }

        template_path = os.path.dirname(__file__)
        jinja2_env = jinja2.Environment(loader=jinja2.FileSystemLoader(template_path),
                                        trim_blocks=True)
        jinja2_env.get_template('oc.psi4.j2').stream(**context, xyz_structure=xyz_structure).dump(tmp_file, encoding='utf8')

    def select_output_files(self, filenames):
        do_copy = [False] * len(filenames)
        for i, file in enumerate(filenames):
            if file.endswith('.out'):
                do_copy[i] = True
        return do_copy

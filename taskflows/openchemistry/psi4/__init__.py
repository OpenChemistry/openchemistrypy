import os
import jinja2
from openchemistry import OpenChemistryTaskFlow

class Psi4TaskFlow(OpenChemistryTaskFlow):

    @property
    def code_label(self):
        return 'psi4'

    def input_generator(self, params, xyz_structure, tmp_file):
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

    def ec2_job_commands(self, input_name):
        return [
            'docker pull openchemistry/psi4:latest',
            'docker run --rm -v $(pwd):/data openchemistry/psi4:latest %s' % (
                input_name)
        ]

    def demo_job_commands(self, input_name):
        return [
            'docker pull openchemistry/psi4:latest',
            'docker run --rm -w $(pwd) -v dev_job_data:/data openchemistry/psi4:latest %s' % (
                input_name)
        ]

    def nersc_job_commands(self, input_name):
        raise NotImplementedError('PSI4 has not been configured to run on NERSC yet.')

import os
import jinja2
from openchemistry import OpenChemistryTaskFlow

class NWChemTaskFlow(OpenChemistryTaskFlow):

    input_name = 'oc.nw'
    job_name = 'nwchem_run'
    logger_name = 'Create NWChem job.'

    @staticmethod
    def input_generator(params, tmp_file):
        template_path = os.path.dirname(__file__)
        jinja2_env = jinja2.Environment(loader=jinja2.FileSystemLoader(template_path),
                                        trim_blocks=True)
        jinja2_env.get_template('oc.nw.j2').stream(**params).dump(tmp_file, encoding='utf8')

    @staticmethod
    def copy_output_files(filenames):
        do_copy = [False] * len(filenames)
        for i, file in enumerate(filenames):
            if file.endswith('.json'):
                do_copy[i] = True
        return do_copy

    @staticmethod
    def ec2_job_commands(input_name):
        return [
            'docker pull openchemistry/nwchem-json:latest',
            'docker run -v $(pwd):/data openchemistry/nwchem-json:latest %s' % (
                input_name)
        ]

    @staticmethod
    def demo_job_commands(input_name):
        return [
            'docker pull openchemistry/nwchem-json:latest',
            'docker run --rm -w $(pwd) -v dev_job_data:/data openchemistry/nwchem-json:latest %s' % (
                input_name)
        ]

    @staticmethod
    def nersc_job_commands(input_name):
        return [
            '/usr/bin/srun -N 1  -n 32 %s %s' % (os.environ.get('OC_NWCHEM_PATH', 'nwchem'), input_name)
        ]

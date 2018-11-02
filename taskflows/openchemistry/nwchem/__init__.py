import os
import jinja2
from openchemistry import OpenChemistryTaskFlow

class NWChemTaskFlow(OpenChemistryTaskFlow):

    @property
    def code_label(self):
        return 'nwchem'

    def input_generator(self, params, xyz_structure, tmp_file):
        template_path = os.path.dirname(__file__)
        jinja2_env = jinja2.Environment(loader=jinja2.FileSystemLoader(template_path),
                                        trim_blocks=True)
        jinja2_env.get_template('oc.nw.j2').stream(**params, xyz_structure=xyz_structure).dump(tmp_file, encoding='utf8')

    def select_output_files(self, filenames):
        do_copy = [False] * len(filenames)
        for i, file in enumerate(filenames):
            if file.endswith('.json'):
                do_copy[i] = True
        return do_copy

    def ec2_job_commands(self, input_name):
        return [
            'docker pull openchemistry/nwchem-json:latest',
            'docker run --rm -v $(pwd):/data openchemistry/nwchem-json:latest %s' % (
                input_name)
        ]

    def demo_job_commands(self, input_name):
        return [
            'docker pull openchemistry/nwchem-json:latest',
            'docker run --rm -w $(pwd) -v dev_job_data:/data openchemistry/nwchem-json:latest %s' % (
                input_name)
        ]

    def nersc_job_commands(self, input_name):
        return [
            '/usr/bin/srun -N 1  -n 32 %s %s' % (os.environ.get('OC_NWCHEM_PATH', 'nwchem'), input_name)
        ]

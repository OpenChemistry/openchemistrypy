import os
import jinja2
from taskflows import OpenChemistryTaskFlow
from taskflows.utils import cjson_to_xyz

class NWChemTaskFlow(OpenChemistryTaskFlow):

    @property
    def code_label(self):
        return 'nwchem'

    @property
    def docker_image(self):
        return 'openchemistry/nwchem-json:latest'

    def input_generator(self, params, cjson, tmp_file):
        xyz_structure = cjson_to_xyz(cjson)
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

    def demo_job_commands(self, input_name):
        mount_dir = '/data'
        return [
            'docker pull %s' % self.docker_image,
            'docker run -w %s -v dev_job_data:%s %s %s' % (
                os.path.join(mount_dir, '{{job._id}}'),
                mount_dir,
                self.docker_image,
                input_name
            )
        ]

    def nersc_job_commands(self, input_name):
        return [
            '/usr/bin/srun -N 1  -n 32 %s %s' % (os.environ.get('OC_NWCHEM_PATH', 'nwchem'), input_name)
        ]

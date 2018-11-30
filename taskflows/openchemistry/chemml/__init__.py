import os
import json
from openchemistry import OpenChemistryTaskFlow

class ChemmlTaskFlow(OpenChemistryTaskFlow):

    @property
    def code_label(self):
        return 'chemml'

    def input_generator(self, params, cjson, tmp_file):
        json.dump(cjson, tmp_file)

    def select_output_files(self, filenames):
        do_copy = [False] * len(filenames)
        for i, file in enumerate(filenames):
            if file.endswith('.out'):
                do_copy[i] = True
        return do_copy

    def ec2_job_commands(self, input_name):
        mount_dir = '/data/'
        return [
            'docker pull openchemistry/chemml:latest',
            'docker run --rm -v dev_job_data:%s openchemistry/chemml:latest %s' % (
                mount_dir, mount_dir + input_name)
        ]

    def demo_job_commands(self, input_name):
        mount_dir = '/data/'
        return [
            'docker pull openchemistry/chemml:latest',
            'docker run --rm -v dev_job_data:%s openchemistry/chemml:latest %s' % (
                mount_dir, mount_dir + input_name)
        ]

    def nersc_job_commands(self, input_name):
        raise NotImplementedError('ChemMl has not been configured to run on NERSC yet.')

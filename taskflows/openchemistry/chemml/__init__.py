import os
from openchemistry import OpenChemistryTaskFlow
from openchemistry.utils import cjson_to_smiles

class ChemmlTaskFlow(OpenChemistryTaskFlow):

    @property
    def code_label(self):
        return 'chemml'

    def input_generator(self, params, cjson, tmp_file):
        smiles = cjson_to_smiles(cjson)
        tmp_file.write(smiles)

    def select_output_files(self, filenames):
        do_copy = [False] * len(filenames)
        for i, file in enumerate(filenames):
            if file.endswith('.out'):
                do_copy[i] = True
        return do_copy

    def ec2_job_commands(self, input_name):
        return [
            'docker pull openchemistry/chemml:latest',
            'docker run --rm -v $(pwd):/data openchemistry/chemml:latest %s' % (
                input_name)
        ]

    def demo_job_commands(self, input_name):
        return [
            'docker pull openchemistry/chemml:latest',
            'docker run --rm -w $(pwd) -v dev_job_data:/data openchemistry/chemml:latest %s' % (
                input_name)
        ]

    def nersc_job_commands(self, input_name):
        raise NotImplementedError('ChemMl has not been configured to run on NERSC yet.')

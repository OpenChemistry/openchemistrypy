import os
import json

from taskflows import OpenChemistryTaskFlow

class ChemmlTaskFlow(OpenChemistryTaskFlow):

    @property
    def code_label(self):
        return 'chemml'

    @property
    def docker_image(self):
        return 'openchemistry/chemml:latest'

    def input_generator(self, params, cjson, tmp_file):
        tmp_file.write(json.dumps(cjson).encode())

    def select_output_files(self, filenames):
        do_copy = [False] * len(filenames)
        for i, file in enumerate(filenames):
            if file.endswith('.out'):
                do_copy[i] = True
        return do_copy

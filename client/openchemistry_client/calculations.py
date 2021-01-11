import click
import json

import openchemistry as oc
from openchemistry._calculation import CalculationResult
from openchemistry._calculation import GirderMolecule
from openchemistry._calculation import PendingCalculationResultWrapper


_common_help = 'Some kind of help for calculations'


_short_help = 'Perform operations on calculations'


@click.group('calculations', short_help=_short_help, help='%s\n\n%s' % (
    _short_help,
    _common_help))
@click.pass_obj
def calculations(gc):
    pass


@calculations.command('ls')
@click.option('--limit', default=25, help='The results limit')
@click.pass_obj
def _list(gc, limit):
    params = {'limit': limit}
    calcs = gc.get('/calculations', parameters=params)
    print(_format_calculations(calcs))


@calculations.command('submit')
@click.argument('input_file')
@click.pass_obj
def _submit(gc, input_file):

    # Make sure openchemistry is using our girder client...
    _patch_oc_girder_client(gc)

    with open(input_file, 'r') as rf:
        input_dict = json.load(rf)

    molecules = [GirderMolecule(input_dict['molecule_id'])]
    image_name = input_dict['image_name']
    input_parameters = input_dict['input_parameters']
    result = oc.run_calculations(molecules, image_name, input_parameters)[0]

    if isinstance(result, PendingCalculationResultWrapper):
        print('Calculation running:', result.unwrap()._id)
    elif isinstance(result, CalculationResult):
        print('Calculation already performed:', result._id)
    else:
        print('Unknown type returned from oc.run_calculations():',
              type(result))


@calculations.command('download')
@click.argument('calculation_id')
@click.option('-o', '--output-file', default='oc_calculation.json',
              help='The file to place the output cjson into.')
@click.pass_obj
def _download(gc, calculation_id, output_file):
    calcs = gc.get('/calculations/%s' % calculation_id)
    if 'properties' in calcs and calcs['properties'].get('pending', False):
        print('Calculation is currently running')
        return

    print('Downloading calculation:', calculation_id)
    cjson = calcs.get('cjson', {})

    print('Writing cjson to output file:', output_file)
    with open(output_file, 'w') as wf:
        json.dump(cjson, wf)


def _format_calculations(calcs):
    ret = 'matches: ' + str(calcs['matches']) + '\n'
    ret += 'limit: ' + str(calcs['limit']) + '\n'
    ret += 'offset: ' + str(calcs['offset']) + '\n'
    ret += 'results:\n'
    for res in calcs['results']:
        ret += _format_calculation(res) + '\n'

    return ret


def _format_calculation(calc):
    return json.dumps(calc, indent=4)


def _patch_oc_girder_client(gc):
    # Make sure the OC girder client is using our client...
    oc._girder.GirderClient()
    setattr(oc._girder.GirderClient.__dict__['instance'], 'client', gc)

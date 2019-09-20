import click
import json
import os


_common_help = 'Some kind of help for molecules'


_short_help = 'Perform operations on molecules'


@click.group('molecules', short_help=_short_help, help='%s\n\n%s' % (
    _short_help,
    _common_help))
@click.pass_obj
def molecules(gc):
    pass


@molecules.command('ls')
@click.option('--limit', default=25, help='The results limit')
@click.pass_obj
def _list(gc, limit):
    params = {'limit': limit}
    molecules = gc.get('/molecules', parameters=params)
    print(_format_molecules(molecules))


@molecules.command('upload')
@click.argument('filename')
@click.option('--format', default=None, help='The format of the file')
@click.pass_obj
def _upload(gc, filename, format):

    if format is None:
        format = os.path.splitext(filename)[1][1:].lower()

    with open(filename, 'r') as rf:
        contents = rf.read()

    body = {
      format: contents
    }

    mol = gc.post('/molecules', json=body)
    print('Molecule uploaded:')
    print(_format_molecule(mol))


def _format_molecules(molecules):
    ret = 'matches: ' + str(molecules['matches']) + '\n'
    ret += 'limit: ' + str(molecules['limit']) + '\n'
    ret += 'offset: ' + str(molecules['offset']) + '\n'
    ret += 'results:\n'
    for res in molecules['results']:
        ret += _format_molecule(res) + '\n'

    return ret


def _format_molecule(mol):
    return json.dumps(mol, indent=4)

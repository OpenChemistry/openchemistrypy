
import click
import json


_common_help = 'Some kind of help for molecules'


_short_help = 'Perform operations on molecules'


@click.group('molecules', short_help=_short_help, help='%s\n\n%s' % (
    _short_help,
    _common_help.replace('LOCAL_FOLDER', 'LOCAL_FOLDER (default: ".")')))
@click.pass_obj
def molecules(gc):
    pass


@molecules.command('list')
@click.option('--limit', default=25, help='The results limit')
@click.pass_obj
def _list(gc, limit):
    params = {'limit': limit}
    molecules = gc.get('/molecules', parameters=params)
    print(_format_molecules(molecules))


def _format_molecules(molecules):
    ret = 'matches: ' + str(molecules['matches']) + '\n'
    ret += 'limit: ' + str(molecules['limit']) + '\n'
    ret += 'offset: ' + str(molecules['offset']) + '\n'
    ret += 'results:\n'
    for res in molecules['results']:
        ret += json.dumps(res, indent=4) + '\n'

    return ret

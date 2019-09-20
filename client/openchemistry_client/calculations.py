import click
import json


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

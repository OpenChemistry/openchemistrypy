import click
from girder_client import GirderClient

from . import calculations
from . import molecules

VERSION = '0.0.1'


@click.group()
@click.option('--api-url', default=None,
              help='RESTful API URL '
                   '(e.g https://girder.example.com:443/%s)' %
                   GirderClient.DEFAULT_API_ROOT)
@click.option('--api-key', envvar='OPENCHEMISTRY_API_KEY', default=None,
              help='[default: OPENCHEMISTRY_API_KEY env. variable]')
@click.version_option(version=VERSION,
                      prog_name='Openchemistry command line interface')
@click.pass_context
def main(ctx, api_key, api_url):
    """Openchemistry Client

    The client can be used to fetch molecules, add molecules, etc.
    """
    gc = GirderClient(apiUrl=api_url)

    if api_key is not None:
        gc.authenticate(apiKey=api_key)

    ctx.obj = gc


main.add_command(molecules.molecules)
main.add_command(calculations.calculations)


if __name__ == '__main__':
    main()

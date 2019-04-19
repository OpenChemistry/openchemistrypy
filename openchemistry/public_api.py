from jinja2 import Environment, BaseLoader
from girder_client import HttpError
import re
import avogadro

from .girder import GirderClient
from .molecule import Molecule
from .calculation import GirderMolecule, CalculationResult, AttributeInterceptor
from .data_provider import CjsonProvider, AvogadroProvider
from .utils import fetch_or_create_queue

_inchi_key_regex = re.compile("^([0-9A-Z\-]+)$")

def _is_inchi_key(identifier):
    return len(identifier) == 27 and identifier[25] == '-' and \
        _inchi_key_regex.match(identifier)

def _find_using_cactus(identifier):
    params = {
        'cactus': identifier

    }
    molecule = GirderClient().get('molecules/search', parameters=params)

    # Just pick the first
    if len(molecule) > 0:
        molecule = molecule[0]
        return GirderMolecule(molecule['_id'], molecule['cjson'])
    else:
        return None

def _calculation_monitor(taskflow_ids):
    try:
        from .notebook import CalculationMonitor
        table = CalculationMonitor({
            'taskFlowIds': taskflow_ids,
            'girderToken': GirderClient().token,
            'girderApiUrl': GirderClient().api_url
        })
    except ImportError:
        # Outside notebook just print message
        table = 'Pending calculations .... '

    return table

def import_structure(smiles=None, inchi=None, cjson=None):

    # If the smiles begins with 'InChI=', then it is actually an inchi instead
    if smiles and smiles.startswith('InChI='):
        inchi = smiles
        smiles = None

    params = {}
    if smiles:
        params['smiles'] = smiles
    elif inchi:
        params['inchi'] = inchi
    elif cjson:
        params['cjson'] = cjson
    else:
        raise Exception('SMILES, InChI, or CJson must be provided')

    molecule = GirderClient().post('molecules', json=params)

    if not molecule:
        raise Exception('Molecule could not be imported with params', params)

    return GirderMolecule(molecule['_id'], molecule['cjson'])

def find_structure_by_inchi_or_smiles(inchi=None, smiles=None):
    params = {}
    if inchi:
        params['inchi'] = inchi
    elif smiles:
        params['smiles'] = smiles

    if not params:
       raise Exception('Either inchi or smiles must be set')

    molecules = GirderClient().get('molecules', parameters=params)

    if not molecules:
        raise Exception('No molecules found with parameters:', params)

     # This will return a list of molecules. Only keep the first one.
    return molecules[0]


def _get_molecule_or_calculation_result(molecule, image_name, input_parameters, input_geometry):
    is_calc_query = (image_name is not None and input_parameters is not None)

    # Are we searching for a specific calculation?
    if is_calc_query:
        # Look for optimization calculation
        cal = _fetch_calculation(molecule['_id'], image_name, input_parameters, input_geometry)

        if cal is not None:
            # TODO We should probably pass in the full calculation
            # so we don't have to fetch it again.
            return CalculationResult(cal['_id'])
        else:
            return None
    else:
        # If this was found by InChI or SMILES, it may not have cjson,
        # but GirderMolecule() will get the cjson via another rest call.
        return GirderMolecule(molecule['_id'], molecule.get('cjson'))


def find_structure(identifier=None, image_name=None, input_parameters=None, input_geometry=None, inchi=None, smiles=None):

    if inchi or smiles:
        molecule = find_structure_by_inchi_or_smiles(inchi, smiles)
        return _get_molecule_or_calculation_result(molecule, image_name, input_parameters, input_geometry)

    if not identifier:
        raise Exception('identifier, inchi, or smiles must be set')

    # InChiKey?
    if _is_inchi_key(identifier):
        try:
            molecule = GirderClient().get('molecules/inchikey/%s' % identifier)
            return _get_molecule_or_calculation_result(molecule, image_name, input_parameters, input_geometry)

        except HttpError as ex:
            if ex.status == 404:
                # Use cactus after this code block to lookup the structure
                pass
            else:
                raise

    # If we have been provided basis, theory or functional and we haven't found
    # a calculation, then we are done.
    is_calc_query = (image_name is not None and input_parameters is not None)
    if is_calc_query:
        return None

    # Try cactus
    molecule = _find_using_cactus(identifier)

    if not molecule:
        raise Exception('No molecules found matching identifier: \'%s\'' % identifier)

    return molecule

def load(data):
    if isinstance(data, dict):
        provider = CjsonProvider(data)
    elif isinstance(data, avogadro.core.Molecule):
        provider = AvogadroProvider(data)
    else:
        raise TypeError("Load accepts either a cjson dict, or an avogadro.core.Molecule")
    return Molecule(provider)

def monitor(results):
    taskflow_ids = []

    for result in results:
        if hasattr(result, '_properties'):
            props = result._properties
            if isinstance(props, AttributeInterceptor):
                props = props.unwrap()
            if isinstance(props, dict):
                taskflow_id = props.get('taskFlowId')
                if taskflow_id is not None:
                    taskflow_ids.append(taskflow_id)

    return _calculation_monitor(taskflow_ids)

def queue():
    if GirderClient().host is None:
        import warnings
        warnings.warn("Cannot displaying pending calculations, the notebook is not running in a Girder environment")
        return

    queue = fetch_or_create_queue(GirderClient())
    running = []
    pending = []

    for taskflow_id, status in queue['taskflows'].items():
        if status == 'pending':
            pending.append(taskflow_id)
        elif status == 'running':
            running.append(taskflow_id)

    taskflow_ids = running + pending

    return _calculation_monitor(taskflow_ids)

def find_spectra(identifier, stype='IR', source='NIST'):
    """Find spectra in source database

    Parameters
    ----------
    identifier : str
        Inchi string.
    stype : str
        Type of spectrum to query.
    source : str
        Database to query. Supported are: 'NIST'
    """
    source = source.lower()

    params = {
            'inchi' : identifier,
            'spectrum_type' : stype,
            'source' : source
    }

    spectra = GirderClient().get('experiments', parameters=params)

    return spectra

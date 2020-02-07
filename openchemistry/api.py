from jinja2 import Environment, BaseLoader
from girder_client import HttpError
import re
import avogadro

from ._girder import GirderClient
from ._molecule import Molecule
from ._calculation import (
    GirderMolecule, CalculationResult, AttributeInterceptor,
    _fetch_calculation, _fetch_or_submit_calculations, _calculation_result
)
from ._data import CjsonProvider, AvogadroProvider
from ._utils import fetch_or_create_queue

_inchi_key_regex = re.compile("^([0-9A-Z\-]+)$")

def _is_inchi_key(identifier):
    return len(identifier) == 27 and identifier[25] == '-' and \
        _inchi_key_regex.match(identifier)

def _find_molecule(identifier=None, inchi=None, smiles=None):
    if inchi:
        return _find_molecule_by_inchi(inchi)

    if smiles:
        return _find_molecule_by_smiles(smiles)

    # InChiKey?
    if _is_inchi_key(identifier):
        return _find_molecule_by_inchikey(identifier)

    # Finally, if identifier is something else (a name), try cactus
    if identifier:
        return _find_molecule_using_cactus(identifier)

    return None

def _find_molecule_by_inchi(inchi):
    params = {
        'inchi': inchi
    }
    return _find_molecule_using_girder(params)

def _find_molecule_by_smiles(smiles):
    params = {
        'smiles': smiles
    }
    return _find_molecule_using_girder(params)

def _find_molecule_by_inchikey(inchikey):
    try:
        return GirderClient().get('molecules/inchikey/%s' % inchikey)
    except HttpError as ex:
        if ex.status == 404:
            return _find_molecule_using_cactus(inchikey)
        else:
            raise

def _find_molecule_using_cactus(identifier):
    params = {
        'cactus': identifier
    }
    res = GirderClient().get('molecules/search', parameters=params)
    # Just pick the first
    if 'results' in res and len(res['results']) > 0:
        return res['results'][0]
    else:
        return None

def _find_molecule_using_girder(params):
    res = GirderClient().get('molecules', parameters=params)
    if 'results' in res and len(res['results']) > 0:
        return res['results'][0]
    else:
        return None

def _calculation_monitor(taskflow_ids):
    try:
        from ._notebook import CalculationMonitor
        table = CalculationMonitor({
            'taskFlowIds': taskflow_ids,
            'girderToken': GirderClient().token,
            'girderApiUrl': GirderClient().api_url
        })
    except ImportError:
        # Outside notebook just print message
        table = 'Pending calculations .... '

    return table

def import_structure(smiles=None, inchi=None, cjson=None, gen3d=True,
                     params=None, gen3d_forcefield='mmff94', gen3d_steps=100):
    # If the smiles begins with 'InChI=', then it is actually an inchi instead
    if smiles and smiles.startswith('InChI='):
        inchi = smiles
        smiles = None

    if params is None:
        params = {}
    if smiles:
        params['smiles'] = smiles
    elif inchi:
        params['inchi'] = inchi
    elif cjson:
        params['cjson'] = cjson
    else:
        raise Exception('SMILES, InChI, or CJson must be provided')

    params['generate3D'] = gen3d
    params['gen3dForcefield'] = gen3d_forcefield
    params['gen3dSteps'] = gen3d_steps
    molecule = GirderClient().post('molecules', json=params)

    if not molecule:
        raise Exception('Molecule could not be imported with params', params)

    return GirderMolecule(molecule['_id'], molecule.get('cjson'))

def find_molecule(identifier=None, inchi=None, smiles=None):
    molecule = _find_molecule(identifier, inchi, smiles)
    if molecule is None:
        raise Exception('Unable to find a molecule with the provided identifiers.')
    return GirderMolecule(molecule['_id'], molecule.get('cjson'))

def find_calculation(molecule, image_name=None, input_parameters=None, input_geometry=None):
    calculation = _fetch_calculation(molecule._id, image_name, input_parameters, input_geometry)
    if calculation is None:
        raise Exception('Unable to find a matching calculation in the database')
    return CalculationResult(calculation['_id'])

def find_structure(identifier=None, image_name=None, input_parameters=None, input_geometry=None, inchi=None, smiles=None):
    molecule = find_molecule(identifier, inchi, smiles)

    # If we have been provided basis, theory or functional it means the user is
    # looking for a calculation, otherwise they're looking for a molecule
    is_calc_query = (image_name is not None and input_parameters is not None)

    if is_calc_query:
        return find_calculation(molecule, image_name, input_parameters, input_geometry)
    else:
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

    inchi = GirderClient().get('molecules/{}'.format(identifier))["inchi"]

    params = {
            'inchi' : inchi,
            'spectrum_type' : stype,
            'source' : source
    }

    spectrum = GirderClient().get('experiments', parameters=params)

    if spectrum is not None:
        frequencies = [float(w) for w in spectrum['x'][1:-1].split()]
        intensities = [float(i) for i in spectrum['y'][1:-1].split()]
        max_intensity = max(intensities)
        intensities = [i / max_intensity for i in intensities]
        spectrum = {'intensities': intensities, 'frequencies': frequencies}

    return spectrum

def run_calculations(girder_molecules, image_name, input_parameters,
                     input_geometries=None, run_parameters=None,
                     force=False):
    """Run multiple calculations in one taskflow

    This will search for each calculation to see if it has already been
    done. If it has been done, it will not run it unless force is true.
    It will then submit all calculations that need to be submitted.

    Parameters
    ----------
    girder_molecules : list of GirderMolecule
        The list of molecules for which to run calculations
    image_name : str
        The name of the image to run the calculations on
    input_paramters : dict
        The input parameters for the taskflow
    input_geometries: list of str
        The input geometries of the molecules
    run_parameters : dict
        The run parameters for the taskflow
    force : bool
        Force all of the calculations to be performed, even if they
        have already been run once.
    """
    if (not isinstance(input_parameters, dict) or
            'task' not in input_parameters):
        print('Warning: no task was specified in "input_parameters"',
              'for oc.run_calculations().')
        print('The default task for the image (usually "energy") will be',
              'performed.')

    molecule_ids = [x._id for x in girder_molecules]
    calculations = _fetch_or_submit_calculations(molecule_ids, image_name,
                                                 input_parameters,
                                                 input_geometries,
                                                 run_parameters, force)

    results = []
    for c, m in zip(calculations, molecule_ids):
        results.append(_calculation_result(c, m))

    return results

from abc import ABC, abstractmethod
import urllib.parse

from ._girder import GirderClient
from ._utils import hash_object, camel_to_space, cjson_has_3d_coords

class Visualization(ABC):
    def __init__(self, provider):
        self._provider = provider
        self._params = {}

    @abstractmethod
    def show(self, viewer='moljs', spectrum=False, volume=False,
             isosurface=False, menu=True, mo=None, iso=None,
             transfer_function=None, mode=-1, play=False, alt=None,
             geometry_id=None, exp_spec=None):
        self._params = {
            'moleculeRenderer': viewer,
            'showSpectrum': spectrum,
            'showVolume': volume,
            'showIsoSurface': isosurface,
            'showMenu': menu,
            'mo': mo,
            'isoValue': iso,
            'mode': mode,
            'play': play,
            **self._transfer_function_to_params(transfer_function)
        }

        if geometry_id is not None:
            # Note: only a MoleculeProvider has this function...
            cjson = self._provider.geometry_cjson(geometry_id)
        else:
            cjson = self._provider.cjson

        # Show SVG if 3D coords are not available
        if not cjson_has_3d_coords(cjson):
            try:
                from ._notebook import SVG
                return SVG(self._provider.svg)
            except ImportError:
                print(self._provider.svg)

        try:
            if exp_spec is not None:
                self._provider.cjson['exp_vibrations'] = exp_spec

            from ._notebook import CJSON

            return CJSON(cjson, **self._params)

        except ImportError:
            # Outside notebook print CJSON
            if alt is None:
                print(cjson)
            else:
                print(alt)

    @abstractmethod
    def data(self):
        return self._provider.cjson

    def url(self):
        url = self._provider.url
        if url is None:
            raise ValueError('The current structure is not coming from a calculation')

        if self._params:
            url = '%s?%s' % (url, urllib.parse.urlencode(self._params))
        try:
            from IPython.display import Markdown
            return Markdown('[%s](%s)' % (url, url))
        except ImportError:
            # Outside notebook just print the url
            print(url)

    def _transfer_function_to_params(self, transfer_function):
        '''
        transfer_function = {
            "colormap": {
                "colors": [[r0, g0, b0], [r1, g1, b1], ...],
                "points": [p0, p1, ...]
            },
            "opacitymap": {
                "opacities": [alpha0, alpha1, ...],
                "points": [p0, p1, ...]
            }
        }
        '''
        params = {}

        if transfer_function is None or not isinstance(transfer_function, dict):
            return params

        colormap = transfer_function.get('colormap')
        if colormap is not None:
            colors = colormap.get('colors')
            points = colormap.get('points')
            if colors is not None:
                params['colors'] = colors
                params['activeMapName'] = 'Custom'
            if points is not None:
                params['colorsX'] = points

        opacitymap = transfer_function.get('opacitymap')
        if opacitymap is not None:
            opacities = opacitymap.get('opacities')
            points = opacitymap.get('points')
            if opacities is not None:
                params['opacities'] = opacities
            if points is not None:
                params['opacitiesX'] = points

        return params

class Structure(Visualization):

    def show(self, viewer='moljs', menu=True, **kwargs):
        return super(Structure, self).show(viewer=viewer, menu=menu, **kwargs)

    def data(self):
        return self._provider.cjson

    def generate_3d(self, forcefield='mmff94', steps=100):
        if cjson_has_3d_coords(self._provider.cjson):
            print('Molecule already has 3D coordinates')
            return

        id = self._provider._id
        params = {
            'gen3dForcefield': forcefield,
            'gen3dSteps': steps
        }

        GirderClient().post('/molecules/%s/3d' % id, parameters=params)
        print('Generating 3D coordinates...')

        # Remove the cjson so it will update
        self._provider._cjson_ = None

class Vibrations(Visualization):

    def show(self, viewer='moljs', spectrum=True, menu=True, mode=-1,
             play=True, experimental=False, **kwargs):

        if experimental:
            from .api import find_spectra
            identifier = self._provider._molecule_id
            exp_spec = find_spectra(identifier, stype='IR', source='NIST')
            kwargs['exp_spec'] = exp_spec

        return super(Vibrations, self).show(viewer=viewer, spectrum=spectrum,
                menu=menu, mode=mode, play=play, **kwargs)

    def data(self):
        return self._provider.vibrations

    def table(self):
        vibrations = self._provider.vibrations
        try:
            from IPython.display import Markdown
            table = self._md_table(vibrations)
            return Markdown(table)
        except ImportError:
            # Outside notebook print CJSON
            print(vibrations)

    def _md_table(self, vibrations):
        import math
        table = '''### Normal Modes
| # | Frequency | Intensity |
|------|-------|-------|'''
        frequencies = vibrations.get('frequencies', [])
        intensities = vibrations.get('intensities', [])

        n = len(frequencies)
        if len(intensities) != n:
            intensities = None

        for i, freq in enumerate(frequencies):
            try:
                freq = float(freq)
            except ValueError:
                freq = math.nan

            intensity = math.nan if intensities is None else intensities[i]
            try:
                intensity = float(intensity)
            except ValueError:
                intensity = math.nan

            table += '\n| %s | %.2f | %.2f |' % (
                i,
                freq,
                intensity
            )

        return table

class Orbitals(Visualization):

    def show(self, viewer='moljs', volume=False, isosurface=True, menu=True, mo='homo', iso=0.05, transfer_function=None, **kwargs):
        self._provider.load_orbital(mo)
        return super(Orbitals, self).show(viewer=viewer, volume=volume, isosurface=isosurface, menu=menu, mo=mo, iso=iso, transfer_function=transfer_function)

    def data(self):
        whitelist = ['orbitals', 'properties']
        output = {}
        for item in whitelist:
            output[item] = self._provider.cjson[item]

        return output

class Properties(Visualization):

    def show(self, **kwargs):
        cjson = self._provider.cjson
        properties = cjson.get('properties', {})
        try:
            from IPython.display import Markdown
            table = self._md_table(properties)
            return Markdown(table)
        except ImportError:
            # Outside notebook print CJSON
            print(properties)

    def data(self):
        return self._provider.cjson.get('properties', {})

    def _md_table(self, properties):
        import math
        table = '''### Calculated Properties
| Name | Value |
|------|-------|'''

        for prop, value in properties.items():
            try:
                value = float(value)
            except ValueError:
                value = math.nan
            table += '\n| %s | %.2f |' % (
                camel_to_space(prop),
                value
            )

        return table

class Geometries(Visualization):

    def show(self, geometry_id=None, **kwargs):
        if geometry_id is not None:
            # User requested a specific geometry. Show that.
            return super(Geometries, self).show(geometry_id=geometry_id,
                                                **kwargs)

        geometries = self._provider.geometries
        try:
            from IPython.display import Markdown
            table = self._md_table(geometries)
            return Markdown(table)
        except ImportError:
            # Outside notebook print CJSON
            print(geometries)

    def data(self):
        return self._provider.geometries

    def _md_table(self, geometries):
        import math
        table = '''### Geometries
| Id | Provenance | Energy |
|----|------------|--------|'''

        for geometry in geometries:
            id = geometry.get('_id')
            provenance = geometry.get('provenanceType')
            try:
                energy = float(geometry.get('energy', math.nan))
            except ValueError:
                energy = math.nan

            table += '\n| %s | %s | %.2f |' % (
                id,
                provenance,
                energy
            )

        return table

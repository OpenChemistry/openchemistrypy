from abc import ABC, abstractmethod
import urllib.parse

from ._utils import hash_object, camel_to_space

class Visualization(ABC):
    def __init__(self, provider):
        self._provider = provider
        self._params = {}

    @abstractmethod
    def show(self, viewer='moljs', spectrum=False, volume=False, isosurface=False, menu=True, mo=None, iso=None, transfer_function=None, mode=-1, play=False, alt=None):
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
        try:
            from ._notebook import CJSON
            return CJSON(self._provider.cjson, **self._params)
        except ImportError:
            # Outside notebook print CJSON
            if alt is None:
                print(self._provider.cjson)
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

class Vibrations(Visualization):

    def show(self, viewer='moljs', spectrum=True, menu=True, mode=-1, play=True, **kwargs):
        return super(Vibrations, self).show(viewer=viewer, spectrum=spectrum, menu=menu, mode=mode, play=play, **kwargs)

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

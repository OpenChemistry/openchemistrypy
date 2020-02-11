from abc import ABC, abstractmethod
import json
import collections
import avogadro
import requests

from ._girder import GirderClient
from ._utils import calculate_mo

from girder_client import HttpError

class DataProvider(ABC):
    @property
    @abstractmethod
    def cjson(self):
        pass

    @property
    @abstractmethod
    def vibrations(self):
        pass

    @abstractmethod
    def load_orbital(self, mo):
        pass

    @property
    @abstractmethod
    def url(self):
        pass

class CachedDataProvider(DataProvider):
    MAX_CACHED = 5

    def __init__(self):
        self._cached_volumes = collections.OrderedDict()

    def _get_cached_volume(self, mo):
        if mo in self._cached_volumes:
            return self._cached_volumes[mo]
        return None

    def _set_cached_volume(self, mo, cube):
        keys = self._cached_volumes.keys()
        if len(keys) >= self.MAX_CACHED:
            del self._cached_volumes[next(iter(keys))]
        self._cached_volumes[mo] = cube

class CjsonProvider(CachedDataProvider):
    def __init__(self, cjson):
        super(CjsonProvider, self).__init__()
        self._cjson_ = cjson

    @property
    def cjson(self):
        return self._cjson_

    @property
    def vibrations(self):
        if 'vibrations' in self.cjson:
            return self.cjson['vibrations']
        else:
            return {'modes': [], 'intensities': [], 'frequencies': []}

    def load_orbital(self, mo):
        cube = super(CjsonProvider, self)._get_cached_volume(mo)
        if cube is None:
            cube = calculate_mo(self.cjson, mo)
            super(CjsonProvider, self)._set_cached_volume(mo, cube)
        cjson = self.cjson
        cjson['cube'] = cube

    @property
    def url(self):
        return None

class MoleculeProvider(CjsonProvider):
    def __init__(self, cjson, molecule_id):
        super(MoleculeProvider, self).__init__(cjson)
        self._id = molecule_id
        self._svg_ = None

    @property
    def cjson(self):
        if self._cjson_ is None:
            # Try to update the cjson
            self._cjson_ = GirderClient().get('molecules/%s/cjson' % self._id)

        return self._cjson_

    @property
    def svg(self):
        if self._svg_ is None:
            resp = GirderClient().get('molecules/%s/svg' % self._id,
                                      jsonResp=False)
            self._svg_ = resp.content.decode('utf-8')

        return self._svg_

    def geometry_cjson(self, geometry_id=None):
        if geometry_id is None:
            # Just return the molecule's default cjson
            return self.cjson

        try:
            resp = GirderClient().get(
                'molecules/%s/geometries/%s' % (self._id, geometry_id))
        except HttpError as e:
            if ('Invalid ObjectId' in e.responseText or
                    'Geometry not found' in e.responseText):
                print('Geometry ID not found')
                return None

            raise

        return resp.get('cjson')

    @property
    def geometries(self):
        resp = GirderClient().get('molecules/%s/geometries' % self._id)
        return resp.get('results', [])

    @property
    def url(self):
        return '%s/molecules/%s' % (GirderClient().app_url.rstrip('/'), self._id)

class CalculationProvider(CachedDataProvider):
    def __init__(self, calculation_id, molecule_id):
        super(CalculationProvider, self).__init__()
        self._id = calculation_id
        self._molecule_id = molecule_id
        self._cjson_ = None
        self._vibrational_modes_ = None

    @property
    def cjson(self):
        if self._cjson_ is None:
            self._cjson_ = GirderClient().get('calculations/%s/cjson' % self._id)

        return self._cjson_

    @property
    def vibrations(self):
        if self._vibrational_modes_ is None:
            self._vibrational_modes_ = GirderClient().get('calculations/%s/vibrationalmodes' % self._id)

        return self._vibrational_modes_

    def load_orbital(self, mo):
        cube = super(CalculationProvider, self)._get_cached_volume(mo)
        if cube is None:
            try:
                cube = GirderClient().get('calculations/%s/cube/%s' % (self._id, mo))['cube']
                super(CalculationProvider, self)._set_cached_volume(mo, cube)
            except requests.HTTPError:
                import warnings
                warnings.warn("No molecular orbital data was found for this calculation.")
                return
        cjson = self.cjson
        cjson['cube'] = cube

    @property
    def url(self):
        return '%s/calculations/%s' % (GirderClient().app_url.rstrip('/'), self._id)

class AvogadroProvider(CjsonProvider):
    def __init__(self, molecule):
        super(AvogadroProvider, self).__init__(None)
        self._molecule = molecule

    @property
    def cjson(self):
        if self._cjson_ is None:
            conv = avogadro.io.FileFormatManager()
            cjson_str = conv.write_string(self._molecule, 'cjson')
            self._cjson_ = json.loads(cjson_str)
        return self._cjson_

from abc import ABC, abstractmethod
import json
import collections
import avogadro

from ._girder import GirderClient
from ._utils import calculate_mo

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
            cube = GirderClient().get('calculations/%s/cube/%s' % (self._id, mo))['cube']
            super(CalculationProvider, self)._set_cached_volume(mo, cube)
        cjson = self.cjson
        cjson['cube'] = cube

    @property
    def url(self):
        return '%s/calculations/%s' % (GirderClient().app_url.rstrip('/'), self._id)

class AvogadroProvider(CjsonProvider):
    def __init__(self, molecule):
        self._molecule = molecule
        self._cjson = None

    @property
    def cjson(self):
        if self._cjson is None:
            conv = avogadro.io.FileFormatManager()
            cjson_str = conv.write_string(self._molecule, 'cjson')
            self._cjson = json.loads(cjson_str)
        return self._cjson

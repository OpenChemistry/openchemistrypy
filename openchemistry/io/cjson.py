import json
from .base import BaseReader

# Trivial Cjson reader
class CjsonReader(BaseReader):
    def read(self):
        return json.load(self._file)

from abc import ABC, abstractmethod

class BaseReader(ABC):

    def __init__(self, f):
        """
        Instantiate an OpenChemistry Reader

        Parameters
        ----------
        f : file-like object
            A quantum chemistry output file
        """
        self._file = f

    @abstractmethod
    def read(self):
        """
        Converts an output file to CJSON

        Returns
        -------
        cjson : dict
            A cjson compliant dict
        """
        pass

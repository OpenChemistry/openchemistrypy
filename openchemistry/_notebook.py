from IPython.display import display, JSON, DisplayObject

class CJSON(JSON):
    """A display class for displaying CJSON visualizations in the Jupyter Notebook and IPython kernel.
    CJSON expects a JSON-able dict, not serialized JSON strings.
    Scalar types (None, number, string) are not allowed, only dict containers.
    """

    def __init__(self, data=None, url=None, filename=None, **kwargs):
        super(CJSON, self).__init__(data, url, filename)
        self.metadata = {**self.metadata, **kwargs}

    def _ipython_display_(self):
        bundle = {
            'application/vnd.oc.cjson+json': self.data,
            'text/plain': '<jupyterlab_cjson.CJSON object>'
        }
        metadata = {
            'application/vnd.oc.cjson+json': self.metadata
        }
        display(bundle, metadata=metadata, raw=True)

class FreeEnergy(JSON):
    """A display class for displaying free energy visualizations in the Jupyter Notebook and IPython kernel.
    FreeEnergy expects a JSON-able dict.
    """

    def __init__(self, data=None, url=None, filename=None):
        super(FreeEnergy, self).__init__(data, url, filename)

    def _ipython_display_(self):
        bundle = {
            'application/vnd.oc.free_energy+json': self.data,
            'text/plain': '<jupyterlab_cjson.FreeEnergy object>'
        }
        metadata = {
            'application/vnd.oc.free_energy+json': self.metadata
        }
        display(bundle, metadata=metadata, raw=True)

class CalculationMonitor(DisplayObject):
    """
    A display class for monitoring calculations Jupyter Notebook and IPython kernel.
    """

    def __init__(self, data=None, url=None, filename=None):
        super(CalculationMonitor, self).__init__(data, url, filename)

    def _ipython_display_(self):
        bundle = {
            'application/vnd.oc.calculation+json': self.data,
            'text/plain': '<jupyterlab_cjson.CalculationMonitor object>'
        }
        metadata = {
            'application/vnd.oc.calculation+json': {}
        }
        display(bundle, metadata=metadata, raw=True)

    def __getattr__(self, name):
        # This is a little fragile, it seem that ipython is looking for the
        # absence of _ipython_canary_method_should_not_exist_, so only return
        # self for 'public' methods.
        if name[0] != '_':
            return self
        else:
            return DisplayObject.__getattr__(self, name)

    def __call__(self):
        return self

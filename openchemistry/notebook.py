from IPython.display import display, JSON, DisplayObject

# A display class that can be used within a notebook.
#   from openchemistry.notebook import CJSON
#   CJSON(data)

DEFAULT_ISO = 0.05;
DEFAULT_ISO_SURFACES = [{
    'value': DEFAULT_ISO,
    'color': 'blue',
    'opacity': 0.9,
  }, {
    'value': -DEFAULT_ISO,
    'color': 'red',
    'opacity': 0.9
  }]

class CJSON(JSON):
    """A display class for displaying CJSON visualizations in the Jupyter Notebook and IPython kernel.
    CJSON expects a JSON-able dict, not serialized JSON strings.
    Scalar types (None, number, string) are not allowed, only dict containers.
    """



    def __init__(self, data=None, url=None, filename=None, vibrational=True, structure=True,
                 iso_surfaces=DEFAULT_ISO_SURFACES, animate_mode=None, calculation_id=None,
                 iso_value=DEFAULT_ISO, mo=None):
        super(CJSON, self).__init__(data, url, filename)
        self.metadata['vibrational'] = vibrational
        self.metadata['structure'] = structure
        self.metadata['isoValue'] = iso_value
        self.metadata['isoSurfaces'] = iso_surfaces
        self.metadata['animateMode'] = animate_mode
        self.metadata['mo'] = mo
        self.metadata['calculationId'] = calculation_id

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
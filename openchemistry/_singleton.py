class Singleton(object):
    """Singleton decorator
    """
    def __init__(self, cls):
        self._cls = cls
        self._instance = self._cls()

    def __call__(self):
        return self._instance

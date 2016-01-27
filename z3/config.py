import ConfigParser
import os
import os.path

import z3


_settings = None


class OnionDict(object):
    """Wrapps multiple dictionaries. Tries to read data from each dict
    in turn.
    Used to implement a fallback mechanism.
    """
    def __init__(self, *args):
        self.__dictionaries = args

    def __getitem__(self, key):
        for d in self.__dictionaries:
            if key in d:
                return d[key]
        raise KeyError(key)

    def __contains__(self, key):
        for d in self.__dictionaries:
            if key in d:
                return True
        return False

    def get(self, key, default=None):
        if key in self:
            return self[key]
        else:
            return default


def get_config():
    global _settings
    if _settings is None:
        _config = ConfigParser.ConfigParser()
        default = os.path.join(z3.__path__[0], "z3.conf")
        _config.read(default)
        _config.read("/etc/z3_backup/z3.conf")
        _settings = OnionDict(
            os.environ,  # env variables take precedence
            dict((k.upper(), v) for k, v in _config.items("main"))
        )
    return _settings

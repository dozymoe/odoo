from odoo.tools import config
from .pickled_redis import PickledRedis
from .prefixed_redis import PrefixedRedis

_persistent_cache = None
_volatile_cache = None


def get_persistent_cache():
    global _persistent_cache
    if not _persistent_cache:
        prefix = config.get('redis_prefix')
        args = dict(
                host=config.get('redis_host', 'localhost'),
                port=int(config.get('redis_port', 6379)),
                password=config.get('redis_password', None))

        if prefix:
            _persistent_cache = PrefixedRedis(prefix=prefix, **args)
        else:
            _persistent_cache = PickledRedis(**args)

    return _persistent_cache


def get_volatile_cache():
    global _volatile_cache
    if not _volatile_cache:
        prefix = config.get('redis_cache_prefix', config.get('redis_prefix'))
        args = dict(
                host=config.get('redis_cache_host',
                    config.get('redis_host', 'localhost')),
                port=int(config.get('redis_cache_port',
                    config.get('redis_port', 6379))),
                password=config.get('redis_cache_password',
                    config.get('redis_password', None)))

        if prefix:
            _volatile_cache = PrefixedRedis(prefix=prefix, **args)
        else:
            _volatile_cache = PickledRedis(**args)

    return _volatile_cache

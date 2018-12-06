import logging
try:
    import cPickle as pickle
except ImportError:
    import pickle
from redis import StrictRedis
from redis.client import list_or_args

_logger = logging.getLogger(__name__)


def _dump(value, pickled):
    if value and pickled:
        return pickle.dumps(value)
    return value


def _load(value, pickled):
    if value and pickled:
        try:
            return pickle.loads(value)
        except pickle.UnpicklingError:
            _logger.exception('Datastore entry ignored: %s', repr(value))
            return None
    return value


def _pickle_load_withscores(items, withscores, pickled):
    if withscores:
        for score, value in items:
            yield (score, _load(value, pickled))
    else:
        for value in items:
            yield _load(value, pickled)


class PickledRedis(StrictRedis):

    def get(self, name, pickle=False):
        """
        Return the value at key ``name``, or None if the key doesn't exist
        """
        ret = super(PickledRedis, self).get(name)
        return _load(ret, pickle)


    def getset(self, name, value, pickle=False):
        """
        Sets the value at key ``name`` to ``value``
        and returns the old value at key ``name`` atomically.
        """
        ret = super(PickledRedis, self).getset(name, _dump(value, pickle))
        return _load(ret, pickle)


    def mget(self, keys, *args, **kwargs):
        """
        Returns a list of values ordered identically to ``keys``
        """
        pickle = kwargs.pop('pickle', False)
        ret = super(PickledRedis, self).mget(keys, *args)
        return [_load(x, pickle) for x in ret]


    def mset(self, *args, **kwargs):
        """
        Sets key/values based on a mapping. Mapping can be supplied as a single
        dictionary argument or as kwargs.
        """
        pickle = kwargs.pop('pickle', False)
        if args:
            if len(args) != 1 or not isinstance(args[0], dict):
                raise RedisError('MSET requires **kwargs or a single dict arg')
            kwargs.update(args[0])

        return super(PickledRedis, self).mset(
                **{k: _dump(v, pickle) for k,v in kwargs.items()})


    def msetnx(self, *args, **kwargs):
        """
        Sets key/values based on a mapping if none of the keys are already set.
        Mapping can be supplied as a single dictionary argument or as kwargs.
        Returns a boolean indicating if the operation was successful.
        """
        pickle = kwargs.pop('pickle', False)
        if args:
            if len(args) != 1 or not isinstance(args[0], dict):
                raise RedisError('MSETNX requires **kwargs or a single '
                                 'dict arg')
            kwargs.update(args[0])

        return super(PickledRedis, self).msetnx(
                **{k: _dump(v, pickle) for k,v in kwargs.items()})


    def psetex(self, name, time_ms, value, pickle=False):
        """
        Set the value of key ``name`` to ``value`` that expires in ``time_ms``
        milliseconds. ``time_ms`` can be represented by an integer or a Python
        timedelta object
        """
        return super(PickledRedis, self).psetex(name, time_ms,
                _dump(value, pickle))


    def set(self, name, value, ex=None, px=None, nx=False, xx=False,
            pickle=False):
        """
        Set the value at key ``name`` to ``value``

        ``ex`` sets an expire flag on key ``name`` for ``ex`` seconds.

        ``px`` sets an expire flag on key ``name`` for ``px`` milliseconds.

        ``nx`` if set to True, set the value at key ``name`` to ``value`` only
            if it does not exist.

        ``xx`` if set to True, set the value at key ``name`` to ``value`` only
            if it already exists.
        """
        return super(PickledRedis, self).set(name, _dump(value, pickle), ex,
                px, nx, xx)


    def setex(self, name, time, value, pickle=False):
        """
        Set the value of key ``name`` to ``value`` that expires in ``time``
        seconds. ``time`` can be represented by an integer or a Python
        timedelta object.
        """
        return super(PickledRedis, self).setex(name, time, _dump(value, pickle))


    def setnx(self, name, value, pickle=False):
        "Set the value of key ``name`` to ``value`` if key doesn't exist"
        return super(PickledRedis, self).setnx(name, _dump(value, pickle))


    # LIST COMMANDS
    def blpop(self, keys, timeout=0, pickle=False):
        """
        LPOP a value off of the first non-empty list
        named in the ``keys`` list.

        If none of the lists in ``keys`` has a value to LPOP, then block
        for ``timeout`` seconds, or until a value gets pushed on to one
        of the lists.

        If timeout is 0, then block indefinitely.
        """
        ret = super(PickledRedis, self).blpop(keys, timeout)
        return _load(ret, pickle)


    def brpop(self, keys, timeout=0, pickle=False):
        """
        RPOP a value off of the first non-empty list
        named in the ``keys`` list.

        If none of the lists in ``keys`` has a value to RPOP, then block
        for ``timeout`` seconds, or until a value gets pushed on to one
        of the lists.

        If timeout is 0, then block indefinitely.
        """
        ret = super(PickledRedis, self).brpop(keys, timeout)
        return _load(ret, pickle)


    def brpoplpush(self, src, dst, timeout=0, pickle=False):
        """
        Pop a value off the tail of ``src``, push it on the head of ``dst``
        and then return it.

        This command blocks until a value is in ``src`` or until ``timeout``
        seconds elapse, whichever is first. A ``timeout`` value of 0 blocks
        forever.
        """
        ret = super(PickledRedis, self).brpoplpush(src, dst, timeout)
        return _load(ret, pickle)


    def lindex(self, name, index, pickle=False):
        """
        Return the item from list ``name`` at position ``index``

        Negative indexes are supported and will return an item at the
        end of the list
        """
        ret = super(PickledRedis, self).lindex(name, index)
        return _load(ret, pickle)


    def linsert(self, name, where, refvalue, value, pickle=False):
        """
        Insert ``value`` in list ``name`` either immediately before or after
        [``where``] ``refvalue``

        Returns the new length of the list on success or -1 if ``refvalue``
        is not in the list.
        """
        return super(PickledRedis, self).linsert(name, where,
                _dump(refvalue, pickle), _dump(value, pickle))


    def lpop(self, name, pickle=False):
        "Remove and return the first item of the list ``name``"
        ret = super(PickledRedis, self).lpop(name)
        return _load(ret, pickle)


    def lpush(self, name, *values, **kwargs):
        "Push ``values`` onto the head of the list ``name``"
        pickle = kwargs.pop('pickle', False)
        return super(PickledRedis, self).lpush(name,
                *(_dump(x, pickle) for x in values))


    def lpushx(self, name, value, pickle=False):
        "Push ``value`` onto the head of the list ``name`` if ``name`` exists"
        return super(PickledRedis, self).lpushx(name, _dump(value, pickle))


    def lrange(self, name, start, end, pickle=False):
        """
        Return a slice of the list ``name`` between
        position ``start`` and ``end``

        ``start`` and ``end`` can be negative numbers just like
        Python slicing notation
        """
        ret = super(PickledRedis, self).lrange(name, start, end)
        return [_load(x, pickle) for x in ret]


    def lrem(self, name, count, value, pickle=False):
        """
        Remove the first ``count`` occurrences of elements equal to ``value``
        from the list stored at ``name``.

        The count argument influences the operation in the following ways:
            count > 0: Remove elements equal to value moving from head to tail.
            count < 0: Remove elements equal to value moving from tail to head.
            count = 0: Remove all elements equal to value.
        """
        return super(PickledRedis, self).lrem(name, count,
                _dump(value, pickle))


    def lset(self, name, index, value, pickle=False):
        "Set ``position`` of list ``name`` to ``value``"
        return super(PickledRedis, self).lset(name, index,
                _dump(value, pickle))


    def rpop(self, name, pickle=False):
        "Remove and return the last item of the list ``name``"
        ret = super(PickledRedis, self).rpop(name)
        return _load(ret, pickle)


    def rpoplpush(self, src, dst, pickle=False):
        """
        RPOP a value off of the ``src`` list and atomically LPUSH it
        on to the ``dst`` list.  Returns the value.
        """
        ret = super(PickledRedis, self).rpoplpush(src, dst)
        return _load(ret, pickle)


    def rpush(self, name, *values, **kwargs):
        "Push ``values`` onto the tail of the list ``name``"
        pickle = kwargs.pop('pickle', False)
        return super(PickledRedis, self).rpush(name,
                *(_dump(x, pickle) for x in values))


    def rpushx(self, name, value, pickle=False):
        "Push ``value`` onto the tail of the list ``name`` if ``name`` exists"
        return super(PickledRedis, self).rpushx(name, _dump(value, pickle))


    def sort(self, name, start=None, num=None, by=None, get=None, desc=False,
            alpha=False, store=None, groups=False, pickle=False):
        """
        Sort and return the list, set or sorted set at ``name``.

        ``start`` and ``num`` allow for paging through the sorted data

        ``by`` allows using an external key to weight and sort the items.
            Use an "*" to indicate where in the key the item value is located

        ``get`` allows for returning items from external keys rather than the
            sorted data itself.  Use an "*" to indicate where int he key
            the item value is located

        ``desc`` allows for reversing the sort

        ``alpha`` allows for sorting lexicographically rather than numerically

        ``store`` allows for storing the result of the sort into
            the key ``store``

        ``groups`` if set to True and if ``get`` contains at least two
            elements, sort will return a list of tuples, each containing the
            values fetched from the arguments to ``get``.

        """
        ret = super(PickledRedis, self).sort(name, start, num, by, get, desc,
                alpha, store, groups)

        return [_load(x, pickle) for x in ret]


    # SCAN COMMANDS
    def sscan(self, name, cursor=0, match=None, count=None, pickle=False):
        """
        Incrementally return lists of elements in a set. Also return a cursor
        indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        cur, values = super(PickledRedis, self).sscan(name, cursor, match,
                count)

        return (cur, [_load(x, pickle) for x in values])


    def sscan_iter(self, name, match=None, count=None, pickle=False):
        """
        Make an iterator using the SSCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        ret = super(PickledRedis, self).sscan_iter(name, match, count)
        return (_load(x, pickle) for x in ret)


    def hscan(self, name, cursor=0, match=None, count=None, pickle=False):
        """
        Incrementally return key/value slices in a hash. Also return a cursor
        indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        def _pickle(items):
            is_key = False
            for item in items:
                is_key = not is_key
                if is_key:
                    yield item
                else:
                    yield _load(item, pickle)

        cur, ret = super(PickledRedis, self).hscan(name, cursor, match, count)
        return (cur, list(_pickle(ret)))


    def hscan_iter(self, name, match=None, count=None, pickle=False):
        """
        Make an iterator using the HSCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        ret = super(PickledRedis, self).hscan_iter(name, match, count)
        is_key = False
        for item in ret:
            is_key = not is_key
            if is_key:
                yield item
            else:
                yield _load(item, pickle)


    def zscan(self, name, cursor=0, match=None, count=None,
              score_cast_func=float, pickle=False):
        """
        Incrementally return lists of elements in a sorted set. Also return a
        cursor indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns

        ``score_cast_func`` a callable used to cast the score return value
        """
        cur, ret = super(PickledRedis, self).zscan(name, cursor, match, count,
                score_cast_func)

        return (cur, [_load(x, pickle) for x in ret])


    def zscan_iter(self, name, match=None, count=None,
                   score_cast_func=float, pickle=False):
        """
        Make an iterator using the ZSCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns

        ``score_cast_func`` a callable used to cast the score return value
        """
        ret = super(PickledRedis, self).zscan_iter(name, match, count,
                score_cast_func)

        return (_load(x, pickle) for x in ret)


    # SET COMMANDS
    def sadd(self, name, *values, **kwargs):
        "Add ``value(s)`` to set ``name``"
        pickle = kwargs.pop('pickle', False)
        return super(PickledRedis, self).sadd(name,
                *(_dump(x, pickle) for x in values))


    def sdiff(self, keys, *args, **kwargs):
        "Return the difference of sets specified by ``keys``"
        pickle = kwargs.pop('pickle', False)
        ret = super(PickledRedis, self).sdiff(keys, *args)
        return [_load(x, pickle) for x in ret]


    def sinter(self, keys, *args, **kwargs):
        "Return the intersection of sets specified by ``keys``"
        pickle = kwargs.pop('pickle', False)
        ret = super(PickledRedis, self).sinter(keys, *args)
        return [_load(x, pickle) for x in ret]


    def sismember(self, name, value, pickle=False):
        "Return a boolean indicating if ``value`` is a member of set ``name``"
        return super(PickledRedis, self).sismember(name, _dump(value, pickle))


    def smembers(self, name, pickle=False):
        "Return all members of the set ``name``"
        ret = super(PickledRedis, self).smembers(name)
        return [_load(x, pickle) for x in ret]


    def smove(self, src, dst, value, pickle=False):
        "Move ``value`` from set ``src`` to set ``dst`` atomically"
        return super(PickledRedis, self).smove(src, dst,
                _dump(value, pickle))


    def spop(self, name, pickle=False):
        "Remove and return a random member of set ``name``"
        ret = super(PickledRedis, self).spop(name)
        return _load(ret, pickle)


    def srandmember(self, name, number=None, pickle=False):
        """
        If ``number`` is None, returns a random member of set ``name``.

        If ``number`` is supplied, returns a list of ``number`` random
        memebers of set ``name``. Note this is only available when running
        Redis 2.6+.
        """
        ret = super(PickledRedis, self).srandmember(name, number)
        if number is None:
            return _load(ret, pickle)
        else:
            return [_load(x, pickle) for x in ret]


    def srem(self, name, *values, **kwargs):
        "Remove ``values`` from set ``name``"
        pickle = kwargs.pop('pickle', False)
        return super(PickledRedis, self).srem(name,
                *(_dump(x, pickle) for x in values))


    def sunion(self, keys, *args, **kwargs):
        "Return the union of sets specified by ``keys``"
        pickle = kwargs.pop('pickle', False)
        ret = super(PickledRedis, self).sunion(keys, args)
        return [_load(x, pickle) for x in ret]


    # SORTED SET COMMANDS
    def zadd(self, name, *args, **kwargs):
        """
        Set any number of score, element-name pairs to the key ``name``. Pairs
        can be specified in two ways:

        As *args, in the form of: score1, name1, score2, name2, ...
        or as **kwargs, in the form of: name1=score1, name2=score2, ...

        The following example would add four values to the 'my-key' key:
        redis.zadd('my-key', 1.1, 'name1', 2.2, 'name2', name3=3.3, name4=4.4)
        """
        pickle = kwargs.pop('pickle', False)
        def _pickle(items):
            is_score = False
            for item in items:
                is_score = not is_score
                if is_score:
                    yield item
                else:
                    yield _dump(item, pickle)

        pieces = []
        if args:
            if len(args) % 2 != 0:
                raise RedisError("ZADD requires an equal number of "
                                 "values and scores")
            pieces.extend(args)
        for pair in iteritems(kwargs):
            pieces.append(pair[1])
            pieces.append(pair[0])

        return super(PickledRedis, self).zadd(name, *_pickle(pieces))


    def zincrby(self, name, value, amount=1, pickle=False):
        "Increment the score of ``value`` in sorted set ``name`` by ``amount``"
        return super(PickledRedis, self).zincrby(name, _dump(value, pickle),
                amount)


    def zrange(self, name, start, end, desc=False, withscores=False,
               score_cast_func=float, pickle=False):
        """
        Return a range of values from sorted set ``name`` between
        ``start`` and ``end`` sorted in ascending order.

        ``start`` and ``end`` can be negative, indicating the end of the range.

        ``desc`` a boolean indicating whether to sort the results descendingly

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs

        ``score_cast_func`` a callable used to cast the score return value
        """
        ret = super(PickledRedis, self).zrange(name, start, end, desc,
                withscores, score_cast_func)

        return list(_pickle_load_withscores(ret, withscores, pickle))


    def zrangebylex(self, name, min, max, start=None, num=None, pickle=False):
        """
        Return the lexicographical range of values from sorted set ``name``
        between ``min`` and ``max``.

        If ``start`` and ``num`` are specified, then return a slice of the
        range.
        """
        ret = super(PickledRedis, self).zrangebylex(name, min, max, start, num)
        return [_load(x, pickle) for x in ret]


    def zrevrangebylex(self, name, max, min, start=None, num=None, pickle=False):
        """
        Return the reversed lexicographical range of values from sorted set
        ``name`` between ``max`` and ``min``.

        If ``start`` and ``num`` are specified, then return a slice of the
        range.
        """
        ret = super(PickledRedis, self).zrevrangebylex(name, max, min, start,
                num)

        return [_load(x, pickle) for x in ret]


    def zrangebyscore(self, name, min, max, start=None, num=None,
                      withscores=False, score_cast_func=float, pickle=False):
        """
        Return a range of values from the sorted set ``name`` with scores
        between ``min`` and ``max``.

        If ``start`` and ``num`` are specified, then return a slice
        of the range.

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs

        `score_cast_func`` a callable used to cast the score return value
        """
        ret = super(PickledRedis, self).zrangebyscore(name, min, max, start,
                num, withscores, score_cast_func)

        return list(_pickle_load_withscores(ret, withscores, pickle))


    def zrank(self, name, value, pickle=False):
        """
        Returns a 0-based value indicating the rank of ``value`` in sorted set
        ``name``
        """
        return super(PickledRedis, self).zrank(name, _dump(value, pickle))


    def zrem(self, name, *values, **kwargs):
        "Remove member ``values`` from sorted set ``name``"
        pickle = kwargs.pop('pickle', False)
        return super(PickledRedis, self).zrem(name,
                *(_dump(x, pickle) for x in values))


    def zrevrange(self, name, start, end, withscores=False,
                  score_cast_func=float, pickle=False):
        """
        Return a range of values from sorted set ``name`` between
        ``start`` and ``end`` sorted in descending order.

        ``start`` and ``end`` can be negative, indicating the end of the range.

        ``withscores`` indicates to return the scores along with the values
        The return type is a list of (value, score) pairs

        ``score_cast_func`` a callable used to cast the score return value
        """
        ret = super(PickledRedis, self).zrevrange(name, start, end, withscores,
                score_cast_func)

        return list(_pickle_load_withscores(ret, withscores, pickle))


    def zrevrangebyscore(self, name, max, min, start=None, num=None,
                         withscores=False, score_cast_func=float, pickle=False):
        """
        Return a range of values from the sorted set ``name`` with scores
        between ``min`` and ``max`` in descending order.

        If ``start`` and ``num`` are specified, then return a slice
        of the range.

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs

        ``score_cast_func`` a callable used to cast the score return value
        """
        ret = super(PickledRedis, self).zrevrangebyscore(name, max, min, start,
                num, withscores, score_cast_func)

        return list(_pickle_load_withscores(ret, withscores, pickle))


    def zrevrank(self, name, value, pickle=False):
        """
        Returns a 0-based value indicating the descending rank of
        ``value`` in sorted set ``name``
        """
        return super(PickledRedis, self).zrevrank(name, _dump(value, pickle))


    def zscore(self, name, value, pickle=False):
        "Return the score of element ``value`` in sorted set ``name``"
        return super(PickledRedis, self).zscore(name, _dump(value, pickle))


    # HYPERLOGLOG COMMANDS
    def pfadd(self, name, *values, **kwargs):
        "Adds the specified elements to the specified HyperLogLog."
        pickle = kwargs.pop('pickle', False)
        return super(PickledRedis, self).pfadd(name,
                *(_dump(x, pickle) for x in values))


    # HASH COMMANDS
    def hget(self, name, key, pickle=False):
        "Return the value of ``key`` within the hash ``name``"
        ret = super(PickledRedis, self).hget(name, key)
        return _load(ret, pickle)


    def hgetall(self, name, pickle=False):
        "Return a Python dict of the hash's name/value pairs"
        ret = super(PickledRedis, self).hgetall(name)
        return {k: _load(v, pickle) for k, v in ret.items()}


    def hset(self, name, key, value, pickle=False):
        """
        Set ``key`` to ``value`` within hash ``name``
        Returns 1 if HSET created a new field, otherwise 0
        """
        return super(PickledRedis, self).hset(name, key, _dump(value, pickle))


    def hsetnx(self, name, key, value, pickle=False):
        """
        Set ``key`` to ``value`` within hash ``name`` if ``key`` does not
        exist.  Returns 1 if HSETNX created a field, otherwise 0.
        """
        return super(PickledRedis, self).hsetnx(name, key,
                _dump(value, pickle))


    def hmset(self, name, mapping, pickle=False):
        """
        Set key to value within hash ``name`` for each corresponding
        key and value from the ``mapping`` dict.
        """
        if not mapping:
            raise DataError("'hmset' with 'mapping' of length 0")
        return super(PickledRedis, self).hmset(name,
                {k: _dump(v, pickle) for k,v in mapping.items()})


    def hmget(self, name, keys, *args, **kwargs):
        "Returns a list of values ordered identically to ``keys``"
        pickle = kwargs.pop('pickle', False)
        ret = super(PickledRedis, self).hmget(name, keys, *args)
        return [_load(x, pickle) for x in ret]


    def hvals(self, name, pickle=False):
        "Return the list of values within hash ``name``"
        ret = super(PickledRedis, self).hvals(name)
        return [_load(x, pickle) for x in ret]

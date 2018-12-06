from .pickled_redis import PickledRedis


class PrefixedRedis(PickledRedis):

    prefix = None

    def __init__(self, host='localhost', port=6379, db=0, password=None,
            socket_timeout=None, socket_connect_timeout=None,
            socket_keepalive=None, socket_keepalive_options=None,
            connection_pool=None, unix_socket_path=None, encoding='utf-8',
            encoding_errors='strict', charset=None, errors=None,
            decode_responses=False, retry_on_timeout=False, ssl=False,
            ssl_keyfile=None, ssl_certfile=None, ssl_cert_reqs=None,
            ssl_ca_certs=None, max_connections=None, prefix=None):

        super(PrefixedRedis, self).__init__(host, port, db, password,
                 socket_timeout, socket_connect_timeout, socket_keepalive,
                 socket_keepalive_options, connection_pool, unix_socket_path,
                 encoding, encoding_errors, charset, errors, decode_responses,
                 retry_on_timeout, ssl, ssl_keyfile, ssl_certfile,
                 ssl_cert_reqs, ssl_ca_certs, max_connections)

        self.prefix = prefix


    def _normalize_key(self, name):
        key = self.prefix + name
        try:
            return key.encode('utf-8')
        except AttributeError:
            return key


    def lock(self, name, timeout=None, sleep=0.1, blocking_timeout=None,
            lock_class=None, thread_local=True):
        """
        Return a new Lock object using key ``name`` that mimics
        the behavior of threading.Lock.

        If specified, ``timeout`` indicates a maximum life for the lock.
        By default, it will remain locked until release() is called.

        ``sleep`` indicates the amount of time to sleep per loop iteration
        when the lock is in blocking mode and another client is currently
        holding the lock.

        ``blocking_timeout`` indicates the maximum amount of time in seconds to
        spend trying to acquire the lock. A value of ``None`` indicates
        continue trying forever. ``blocking_timeout`` can be specified as a
        float or integer, both representing the number of seconds to wait.

        ``lock_class`` forces the specified lock implementation.

        ``thread_local`` indicates whether the lock token is placed in
        thread-local storage. By default, the token is placed in thread local
        storage so that a thread only sees its token, not a token set by
        another thread. Consider the following timeline:

            time: 0, thread-1 acquires `my-lock`, with a timeout of 5 seconds.
                     thread-1 sets the token to "abc"
            time: 1, thread-2 blocks trying to acquire `my-lock` using the
                     Lock instance.
            time: 5, thread-1 has not yet completed. redis expires the lock
                     key.
            time: 5, thread-2 acquired `my-lock` now that it's available.
                     thread-2 sets the token to "xyz"
            time: 6, thread-1 finishes its work and calls release(). if the
                     token is *not* stored in thread local storage, then
                     thread-1 would see the token value as "xyz" and would be
                     able to successfully release the thread-2's lock.

        In some use cases it's necessary to disable thread local storage. For
        example, if you have code where one thread acquires a lock and passes
        that lock instance to a worker thread to release later. If thread
        local storage isn't disabled in this case, the worker thread won't see
        the token set by the thread that acquired the lock. Our assumption
        is that these cases aren't common and as such default to using
        thread local storage.
        """
        return super(PrefixedRedis, self).lock(self._normalize_key(name),
                timeout, sleep, blocking_timeout, lock_class, thread_local)


    def append(self, key, value):
        """
        Appends the string ``value`` to the value at ``key``. If ``key``
        doesn't already exist, create it with a value of ``value``.
        Returns the new length of the value at ``key``.
        """
        return super(PrefixedRedis, self).append(self._normalize_key(key),
                value)


    def bitcount(self, key, start=None, end=None):
        """
        Returns the count of set bits in the value of ``key``.  Optional
        ``start`` and ``end`` paramaters indicate which bytes to consider
        """
        return super(PrefixedRedis, self).bitcount(self._normalize_key(key),
                start, end)


    def bitop(self, operation, dest, *keys):
        """
        Perform a bitwise operation using ``operation`` between ``keys`` and
        store the result in ``dest``.
        """
        return super(PrefixedRedis, self).bitop(operation,
                self._normalize_key(dest),
                *(self._normalize_key(k) for k in keys))


    def bitpos(self, key, bit, start=None, end=None):
        """
        Return the position of the first bit set to 1 or 0 in a string.
        ``start`` and ``end`` difines search range. The range is interpreted
        as a range of bytes and not a range of bits, so start=0 and end=2
        means to look at the first three bytes.
        """
        return super(PrefixedRedis, self).bitpos(self._normalize_key(key), bit,
                start, end)


    def decr(self, name, amount=1):
        """
        Decrements the value of ``key`` by ``amount``.  If no key exists,
        the value will be initialized as 0 - ``amount``
        """
        return super(PrefixedRedis, self).decr(self._normalize_key(name),
                amount)


    def delete(self, *names):
        "Delete one or more keys specified by ``names``"
        return super(PrefixedRedis, self).delete(
                *(self._normalize_key(k) for k in names))


    def dump(self, name):
        """
        Return a serialized version of the value stored at the specified key.
        If key does not exist a nil bulk reply is returned.
        """
        return super(PrefixedRedis, self).dump(self._normalize_key(name))


    def exists(self, name):
        "Returns a boolean indicating whether key ``name`` exists"
        return super(PrefixedRedis, self).exists(self._normalize_key(name))


    def expire(self, name, time):
        """
        Set an expire flag on key ``name`` for ``time`` seconds. ``time``
        can be represented by an integer or a Python timedelta object.
        """
        return super(PrefixedRedis, self).expire(self._normalize_key(name),
                time)


    def expireat(self, name, when):
        """
        Set an expire flag on key ``name``. ``when`` can be represented
        as an integer indicating unix time or a Python datetime object.
        """
        return super(PrefixedRedis, self).expireat(self._normalize_key(name),
                when)


    def get(self, name, pickle=False):
        """
        Return the value at key ``name``, or None if the key doesn't exist
        """
        return super(PrefixedRedis, self).get(self._normalize_key(name),
                pickle)


    def getbit(self, name, offset):
        "Returns a boolean indicating the value of ``offset`` in ``name``"
        return super(PrefixedRedis, self).getbit(self._normalize_key(name),
                offset)


    def getrange(self, key, start, end):
        """
        Returns the substring of the string value stored at ``key``,
        determined by the offsets ``start`` and ``end`` (both are inclusive)
        """
        return super(PrefixedRedis, self).getrange(self._normalize_key(key),
                start, end)


    def getset(self, name, value, pickle=False):
        """
        Sets the value at key ``name`` to ``value``
        and returns the old value at key ``name`` atomically.
        """
        return super(PrefixedRedis, self).getset(self._normalize_key(name),
                value, pickle)


    def incr(self, name, amount=1):
        """
        Increments the value of ``key`` by ``amount``.  If no key exists,
        the value will be initialized as ``amount``
        """
        return super(PrefixedRedis, self).incr(self._normalize_key(name),
                amount)


    def incrby(self, name, amount=1):
        """
        Increments the value of ``key`` by ``amount``.  If no key exists,
        the value will be initialized as ``amount``
        """
        return super(PrefixedRedis, self).incrby(self._normalize_key(name),
                amount)


    def incrbyfloat(self, name, amount=1.0):
        """
        Increments the value at key ``name`` by floating ``amount``.
        If no key exists, the value will be initialized as ``amount``
        """
        return super(PrefixedRedis, self).incrbyfloat(self._normalize_key(name),
                amount)


    def keys(self, pattern='*'):
        "Returns a list of keys matching ``pattern``"
        ret = super(PrefixedRedis, self).keys(self._normalize_key(pattern))
        return [x[len(self.prefix):] for x in ret]


    def mget(self, keys, *args, **kwargs):
        """
        Returns a list of values ordered identically to ``keys``
        """
        pickle = kwargs.pop('pickle', False)
        args = list_or_args(keys, args)
        return super(PrefixedRedis, self).mget(
                *(self._normalize_key(k) for k in args), pickle=pickle)


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

        return super(PrefixedRedis, self).mset(pickle=pickle,
                **{self._normalize_key(k): v for k,v in kwargs.items()})


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

        return super(PrefixedRedis, self).msetnx(pickle=pickle,
                **{self._normalize_key(k): v for k,v in kwargs.items()})


    def move(self, name, db):
        "Moves the key ``name`` to a different Redis database ``db``"
        return super(PrefixedRedis, self).move(self._normalize_key(name), db)


    def persist(self, name):
        "Removes an expiration on ``name``"
        return super(PrefixedRedis, self).persist(self._normalize_key(name))


    def pexpire(self, name, time):
        """
        Set an expire flag on key ``name`` for ``time`` milliseconds.
        ``time`` can be represented by an integer or a Python timedelta
        object.
        """
        return super(PrefixedRedis, self).pexpire(self._normalize_key(name),
                time)


    def pexpireat(self, name, when):
        """
        Set an expire flag on key ``name``. ``when`` can be represented
        as an integer representing unix time in milliseconds (unix time * 1000)
        or a Python datetime object.
        """
        return super(PrefixedRedis, self).pexpireat(self._normalize_key(name),
                when)


    def psetex(self, name, time_ms, value, pickle=False):
        """
        Set the value of key ``name`` to ``value`` that expires in ``time_ms``
        milliseconds. ``time_ms`` can be represented by an integer or a Python
        timedelta object
        """
        return super(PrefixedRedis, self).psetex(self._normalize_key(name),
                time_ms, value, pickle)


    def pttl(self, name):
        "Returns the number of milliseconds until the key ``name`` will expire"
        return super(PrefixedRedis, self).pttl(self._normalize_key(name))


    def rename(self, src, dst):
        """
        Rename key ``src`` to ``dst``
        """
        return super(PrefixedRedis, self).rename(self._normalize_key(src),
                self._normalize_key(dst))


    def renamenx(self, src, dst):
        "Rename key ``src`` to ``dst`` if ``dst`` doesn't already exist"
        return super(PrefixedRedis, self).renamenx(self._normalize_key(src),
                self._normalize_key(dst))


    def restore(self, name, ttl, value, replace=False):
        """
        Create a key using the provided serialized value, previously obtained
        using DUMP.
        """
        return super(PrefixedRedis, self).restore(self._normalize_key(name),
                ttl, value, replace)


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
        return super(PrefixedRedis, self).set(self._normalize_key(name), value,
                ex, px, nx, xx, pickle)


    def setbit(self, name, offset, value):
        """
        Flag the ``offset`` in ``name`` as ``value``. Returns a boolean
        indicating the previous value of ``offset``.
        """
        return super(PrefixedRedis, self).setbit(self._normalize_key(name),
                offset, value)


    def setex(self, name, time, value, pickle=False):
        """
        Set the value of key ``name`` to ``value`` that expires in ``time``
        seconds. ``time`` can be represented by an integer or a Python
        timedelta object.
        """
        return super(PrefixedRedis, self).setex(self._normalize_key(name), time,
                value, pickle)


    def setnx(self, name, value, pickle=False):
        "Set the value of key ``name`` to ``value`` if key doesn't exist"
        return super(PrefixedRedis, self).setnx(self._normalize_key(name),
                value, pickle)


    def setrange(self, name, offset, value):
        """
        Overwrite bytes in the value of ``name`` starting at ``offset`` with
        ``value``. If ``offset`` plus the length of ``value`` exceeds the
        length of the original value, the new value will be larger than before.
        If ``offset`` exceeds the length of the original value, null bytes
        will be used to pad between the end of the previous value and the start
        of what's being injected.

        Returns the length of the new string.
        """
        return super(PrefixedRedis, self).setrange(self._normalize_key(name),
                offset, value)


    def strlen(self, name):
        "Return the number of bytes stored in the value of ``name``"
        return super(PrefixedRedis, self).strlen(self._normalize_key(name))


    def substr(self, name, start, end=-1):
        """
        Return a substring of the string at key ``name``. ``start`` and ``end``
        are 0-based integers specifying the portion of the string to return.
        """
        return super(PrefixedRedis, self).substr(self._normalize_key(name),
                start, end)


    def touch(self, *args):
        """
        Alters the last access time of a key(s) ``*args``. A key is ignored
        if it does not exist.
        """
        return super(PrefixedRedis, self).touch(
                *(self._normalize_key(k) for k in args))


    def ttl(self, name):
        "Returns the number of seconds until the key ``name`` will expire"
        return super(PrefixedRedis, self).ttl(self._normalize_key(name))


    def type(self, name):
        "Returns the type of key ``name``"
        return super(PrefixedRedis, self).type(self._normalize_key(name))


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
        return super(PrefixedRedis, self).blpop(
                [self._normalize_key(k) for k in keys], timeout, pickle)


    def brpop(self, keys, timeout=0, pickle=False):
        """
        RPOP a value off of the first non-empty list
        named in the ``keys`` list.

        If none of the lists in ``keys`` has a value to RPOP, then block
        for ``timeout`` seconds, or until a value gets pushed on to one
        of the lists.

        If timeout is 0, then block indefinitely.
        """
        return super(PrefixedRedis, self).brpop(
                [self._normalize_key(k) for k in keys], timeout, pickle)


    def brpoplpush(self, src, dst, timeout=0, pickle=False):
        """
        Pop a value off the tail of ``src``, push it on the head of ``dst``
        and then return it.

        This command blocks until a value is in ``src`` or until ``timeout``
        seconds elapse, whichever is first. A ``timeout`` value of 0 blocks
        forever.
        """
        return super(PrefixedRedis, self).brpoplpush(self._normalize_key(src),
                self._normalize_key(dst), timeout, pickle)


    def lindex(self, name, index, pickle=False):
        """
        Return the item from list ``name`` at position ``index``

        Negative indexes are supported and will return an item at the
        end of the list
        """
        return super(PrefixedRedis, self).lindex(self._normalize_key(name),
                index, pickle)


    def linsert(self, name, where, refvalue, value, pickle=False):
        """
        Insert ``value`` in list ``name`` either immediately before or after
        [``where``] ``refvalue``

        Returns the new length of the list on success or -1 if ``refvalue``
        is not in the list.
        """
        return super(PrefixedRedis, self).linsert(self._normalize_key(name),
                where, refvalue, value, pickle)


    def llen(self, name):
        "Return the length of the list ``name``"
        return super(PrefixedRedis, self).llen(self._normalize_key(name))


    def lpop(self, name, pickle=False):
        "Remove and return the first item of the list ``name``"
        return super(PrefixedRedis, self).lpop(self._normalize_key(name),
                pickle)


    def lpush(self, name, *values, **kwargs):
        "Push ``values`` onto the head of the list ``name``"
        pickle = kwargs.pop('pickle', False)
        return super(PrefixedRedis, self).lpush(self._normalize_key(name),
                *values, pickle=pickle)


    def lpushx(self, name, value, pickle=False):
        "Push ``value`` onto the head of the list ``name`` if ``name`` exists"
        return super(PrefixedRedis, self).lpushx(self._normalize_key(name),
                value, pickle)


    def lrange(self, name, start, end, pickle=False):
        """
        Return a slice of the list ``name`` between
        position ``start`` and ``end``

        ``start`` and ``end`` can be negative numbers just like
        Python slicing notation
        """
        return super(PrefixedRedis, self).lrange(self._normalize_key(name),
                start, end, pickle)


    def lrem(self, name, count, value, pickle=False):
        """
        Remove the first ``count`` occurrences of elements equal to ``value``
        from the list stored at ``name``.

        The count argument influences the operation in the following ways:
            count > 0: Remove elements equal to value moving from head to tail.
            count < 0: Remove elements equal to value moving from tail to head.
            count = 0: Remove all elements equal to value.
        """
        return super(PrefixedRedis, self).lrem(self._normalize_key(name),
                count, value, pickle)


    def lset(self, name, index, value, pickle=False):
        "Set ``position`` of list ``name`` to ``value``"
        return super(PrefixedRedis, self).lset(self._normalize_key(name),
                index, value, pickle)


    def ltrim(self, name, start, end):
        """
        Trim the list ``name``, removing all values not within the slice
        between ``start`` and ``end``

        ``start`` and ``end`` can be negative numbers just like
        Python slicing notation
        """
        return super(PrefixedRedis, self).ltrim(self._normalize_key(name),
                start, end)


    def rpop(self, name, pickle=False):
        "Remove and return the last item of the list ``name``"
        return super(PrefixedRedis, self).rpop(self._normalize_key(name),
                pickle)


    def rpoplpush(self, src, dst, pickle=False):
        """
        RPOP a value off of the ``src`` list and atomically LPUSH it
        on to the ``dst`` list.  Returns the value.
        """
        return super(PrefixedRedis, self).rpoplpush(self._normalize_key(src),
                self._normalize_key(dst), pickle)


    def rpush(self, name, *values, **kwargs):
        "Push ``values`` onto the tail of the list ``name``"
        pickle = kwargs.pop('pickle', False)
        return super(PrefixedRedis, self).rpush(self._normalize_key(name),
                *values, pickle=pickle)


    def rpushx(self, name, value, pickle=False):
        "Push ``value`` onto the tail of the list ``name`` if ``name`` exists"
        return super(PrefixedRedis, self).rpushx(self._normalize_key(name),
                value, pickle)


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
        return super(PrefixedRedis, self).sort(self._normalize_key(name),
                start, num, by, get, desc, alpha, store, groups, pickle)


    # SCAN COMMANDS
    def scan(self, cursor=0, match=None, count=None):
        """
        Incrementally return lists of key names. Also return a cursor
        indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        cur, keys = super(PrefixedRedis, self).scan(cursor, match, count)
        return (cur, [x[len(self.prefix):] for x in keys if\
                x.startswith(self.prefix)])


    def scan_iter(self, match=None, count=None):
        """
        Make an iterator using the SCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        ret = super(PrefixedRedis, self).scan_iter(match, count)
        for item in ret:
            if item.startswith(self.prefix):
                yield item[len(self.prefix):]
            else:
                continue


    def sscan(self, name, cursor=0, match=None, count=None, pickle=False):
        """
        Incrementally return lists of elements in a set. Also return a cursor
        indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        return super(PrefixedRedis, self).sscan(self._normalize_key(name),
                cursor, match, count, pickle)


    def sscan_iter(self, name, match=None, count=None, pickle=False):
        """
        Make an iterator using the SSCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        return super(PrefixedRedis, self).sscan_iter(self._normalize_key(name),
                match, count, pickle)


    def hscan(self, name, cursor=0, match=None, count=None, pickle=False):
        """
        Incrementally return key/value slices in a hash. Also return a cursor
        indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        return super(PrefixedRedis, self).hscan(self._normalize_key(name),
                cursor, match, count, pickle)


    def hscan_iter(self, name, match=None, count=None, pickle=False):
        """
        Make an iterator using the HSCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        return super(PrefixedRedis, self).hscan_iter(self._normalize_key(name),
                match, count, pickle)


    def zscan(self, name, cursor=0, match=None, count=None,
              score_cast_func=float, pickle=False):
        """
        Incrementally return lists of elements in a sorted set. Also return a
        cursor indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns

        ``score_cast_func`` a callable used to cast the score return value
        """
        return super(PrefixedRedis, self).zscan(self._normalize_key(name),
                cursor, match, count, score_cast_func, pickle)


    def zscan_iter(self, name, match=None, count=None,
                   score_cast_func=float, pickle=False):
        """
        Make an iterator using the ZSCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns

        ``score_cast_func`` a callable used to cast the score return value
        """
        return super(PrefixedRedis, self).zscan_iter(self._normalize_key(name),
                match, count, score_cast_func, pickle)


    # SET COMMANDS
    def sadd(self, name, *values, **kwargs):
        "Add ``value(s)`` to set ``name``"
        pickle = kwargs.pop('pickle', False)
        return super(PrefixedRedis, self).sadd(self._normalize_key(name),
                *values, pickle=pickle)


    def scard(self, name):
        "Return the number of elements in set ``name``"
        return super(PrefixedRedis, self).scard(self._normalize_key(name))


    def sdiff(self, keys, *args, **kwargs):
        "Return the difference of sets specified by ``keys``"
        pickle = kwargs.pop('pickle', False)
        args = list_or_args(keys, args)
        return super(PrefixedRedis, self).sdiff(
                *(self._normalize_key(x) for x in args), pickle=pickle)


    def sdiffstore(self, dest, keys, *args):
        """
        Store the difference of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        args = [dest] + list_or_args(keys, args)
        return super(PrefixedRedis, self).sdiffstore(
                *(self._normalize_key(x) for x in args))


    def sinter(self, keys, *args, **kwargs):
        "Return the intersection of sets specified by ``keys``"
        pickle = kwargs.pop('pickle', False)
        args = list_or_args(keys, args)
        return super(PrefixedRedis, self).sinter(
                *(self._normalize_key(x) for x in args), pickle=pickle)


    def sinterstore(self, dest, keys, *args):
        """
        Store the intersection of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        args = [dest] + list_or_args(keys, args)
        return super(PrefixedRedis, self).sinterstore(
                *(self._normalize_key(x) for x in args))


    def sismember(self, name, value, pickle=False):
        "Return a boolean indicating if ``value`` is a member of set ``name``"
        return super(PrefixedRedis, self).sismember(self._normalize_key(name),
                value, pickle)


    def smembers(self, name, pickle=False):
        "Return all members of the set ``name``"
        return super(PrefixedRedis, self).smembers(self._normalize_key(name),
                pickle)


    def smove(self, src, dst, value, pickle=False):
        "Move ``value`` from set ``src`` to set ``dst`` atomically"
        return super(PrefixedRedis, self).smove(self._normalize_key(src),
                self._normalize_key(dst), value, pickle)


    def spop(self, name, pickle=False):
        "Remove and return a random member of set ``name``"
        return super(PrefixedRedis, self).spop(self._normalize_key(name),
                pickle)


    def srandmember(self, name, number=None, pickle=False):
        """
        If ``number`` is None, returns a random member of set ``name``.

        If ``number`` is supplied, returns a list of ``number`` random
        memebers of set ``name``. Note this is only available when running
        Redis 2.6+.
        """
        return super(PrefixedRedis, self).srandmember(self._normalize_key(name),
                number, pickle)


    def srem(self, name, *values, **kwargs):
        "Remove ``values`` from set ``name``"
        pickle = kwargs.pop('pickle', False)
        return super(PrefixedRedis, self).srem(self._normalize_key(name),
                *values, pickle=pickle)


    def sunion(self, keys, *args, **kwargs):
        "Return the union of sets specified by ``keys``"
        pickle = kwargs.pop('pickle', False)
        args = list_or_args(keys, args)
        return super(PrefixedRedis, self).sunion(
                *(self._normalize_key(x) for x in args), pickle=pickle)


    def sunionstore(self, dest, keys, *args):
        """
        Store the union of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        args = [dest] + list_or_args(keys, args)
        return super(PrefixedRedis, self).sunionstore(
                *(self._normalize_key(x) for x in args))


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
        return super(PrefixedRedis, self).zadd(self._normalize_key(name),
                *args, pickle=pickle, **kwargs)


    def zcard(self, name):
        "Return the number of elements in the sorted set ``name``"
        return super(PrefixedRedis, self).zcard(self._normalize_key(name))


    def zcount(self, name, min, max):
        """
        Returns the number of elements in the sorted set at key ``name`` with
        a score between ``min`` and ``max``.
        """
        return super(PrefixedRedis, self).zcount(self._normalize_key(name),
                min, max)


    def zincrby(self, name, value, amount=1, pickle=False):
        "Increment the score of ``value`` in sorted set ``name`` by ``amount``"
        return super(PrefixedRedis, self).zincrby(self._normalize_key(name),
                value, amount, pickle)


    def zinterstore(self, dest, keys, aggregate=None):
        """
        Intersect multiple sorted sets specified by ``keys`` into
        a new sorted set, ``dest``. Scores in the destination will be
        aggregated based on the ``aggregate``, or SUM if none is provided.
        """
        args = list_or_args(keys)
        return super(PrefixedRedis, self).zincrby(self._normalize_key(dest),
                [self._normalize_key(x) for x in args], aggregate)


    def zlexcount(self, name, min, max):
        """
        Return the number of items in the sorted set ``name`` between the
        lexicographical range ``min`` and ``max``.
        """
        return super(PrefixedRedis, self).zlexcount(self._normalize_key(name),
                min, max)


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
        return super(PrefixedRedis, self).zrange(self._normalize_key(name),
                start, end, desc, withscores, score_cast_func, pickle)


    def zrangebylex(self, name, min, max, start=None, num=None, pickle=False):
        """
        Return the lexicographical range of values from sorted set ``name``
        between ``min`` and ``max``.

        If ``start`` and ``num`` are specified, then return a slice of the
        range.
        """
        return super(PrefixedRedis, self).zrangebylex(self._normalize_key(name),
                min, max, start, num, pickle)


    def zrevrangebylex(self, name, max, min, start=None, num=None, pickle=False):
        """
        Return the reversed lexicographical range of values from sorted set
        ``name`` between ``max`` and ``min``.

        If ``start`` and ``num`` are specified, then return a slice of the
        range.
        """
        return super(PrefixedRedis, self).zrevrangebylex(
                self._normalize_key(name), max, min, start, num, pickle)


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
        return super(PrefixedRedis, self).zrangebyscore(
                self._normalize_key(name), min, max, start, num, withscores,
                score_cast_func, pickle)


    def zrank(self, name, value, pickle=False):
        """
        Returns a 0-based value indicating the rank of ``value`` in sorted set
        ``name``
        """
        return super(PrefixedRedis, self).zrank(self._normalize_key(name),
                value, pickle)


    def zrem(self, name, *values, **kwargs):
        "Remove member ``values`` from sorted set ``name``"
        pickle = kwargs.pop('pickle', False)
        return super(PrefixedRedis, self).zrem(self._normalize_key(name),
                *values, pickle=pickle)


    def zremrangebylex(self, name, min, max):
        """
        Remove all elements in the sorted set ``name`` between the
        lexicographical range specified by ``min`` and ``max``.

        Returns the number of elements removed.
        """
        return super(PrefixedRedis, self).zremrangebylex(
                self._normalize_key(name), min, max)


    def zremrangebyrank(self, name, min, max):
        """
        Remove all elements in the sorted set ``name`` with ranks between
        ``min`` and ``max``. Values are 0-based, ordered from smallest score
        to largest. Values can be negative indicating the highest scores.
        Returns the number of elements removed
        """
        return super(PrefixedRedis, self).zremrangebyrank(
                self._normalize_key(name), min, max)


    def zremrangebyscore(self, name, min, max):
        """
        Remove all elements in the sorted set ``name`` with scores
        between ``min`` and ``max``. Returns the number of elements removed.
        """
        return super(PrefixedRedis, self).zremrangebyscore(
                self._normalize_key(name), min, max)


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
        return super(PrefixedRedis, self).zrevrange(
                self._normalize_key(name), start, end, withscores,
                score_cast_func, pickle)


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
        return super(PrefixedRedis, self).zrevrangebyscore(
                self._normalize_key(name), max, min, start, num, withscores,
                score_cast_func, pickle)


    def zrevrank(self, name, value, pickle=False):
        """
        Returns a 0-based value indicating the descending rank of
        ``value`` in sorted set ``name``
        """
        return super(PrefixedRedis, self).zrevrank(self._normalize_key(name),
                value, pickle)


    def zscore(self, name, value, pickle=False):
        "Return the score of element ``value`` in sorted set ``name``"
        return super(PrefixedRedis, self).zscore(self._normalize_key(name),
                value, pickle)


    def zunionstore(self, dest, keys, aggregate=None):
        """
        Union multiple sorted sets specified by ``keys`` into
        a new sorted set, ``dest``. Scores in the destination will be
        aggregated based on the ``aggregate``, or SUM if none is provided.
        """
        args = list_or_args(keys)
        return super(PrefixedRedis, self).zunionstore(
                self._normalize_key(dest),
                [self._normalize_key(x) for x in args], aggregate)


    # HYPERLOGLOG COMMANDS
    def pfadd(self, name, *values, **kwargs):
        "Adds the specified elements to the specified HyperLogLog."
        pickle = kwargs.pop('pickle', False)
        return super(PrefixedRedis, self).pfadd(self._normalize_key(name),
                *values, pickle=pickle)


    def pfcount(self, *sources):
        """
        Return the approximated cardinality of
        the set observed by the HyperLogLog at key(s).
        """
        return super(PrefixedRedis, self).pfcount(
                *(self._normalize_key(x) for x in sources))


    def pfmerge(self, dest, *sources):
        "Merge N different HyperLogLogs into a single one."
        return super(PrefixedRedis, self).pfmerge(self._normalize_key(dest),
                *(self._normalize_key(x) for x in sources))


    # HASH COMMANDS
    def hdel(self, name, *keys):
        "Delete ``keys`` from hash ``name``"
        return super(PrefixedRedis, self).hdel(self._normalize_key(name),
                *keys)


    def hexists(self, name, key):
        "Returns a boolean indicating if ``key`` exists within hash ``name``"
        return super(PrefixedRedis, self).hexists(self._normalize_key(name),
                key)


    def hget(self, name, key, pickle=False):
        "Return the value of ``key`` within the hash ``name``"
        return super(PrefixedRedis, self).hget(self._normalize_key(name), key,
                pickle)


    def hgetall(self, name, pickle=False):
        "Return a Python dict of the hash's name/value pairs"
        return super(PrefixedRedis, self).hgetall(self._normalize_key(name),
                pickle)


    def hincrby(self, name, key, amount=1):
        "Increment the value of ``key`` in hash ``name`` by ``amount``"
        return super(PrefixedRedis, self).hincrby(self._normalize_key(name),
                key, amount)


    def hincrbyfloat(self, name, key, amount=1.0):
        """
        Increment the value of ``key`` in hash ``name`` by floating ``amount``
        """
        return super(PrefixedRedis, self).hincrbyfloat(
                self._normalize_key(name), key, amount)

    def hkeys(self, name):
        "Return the list of keys within hash ``name``"
        return super(PrefixedRedis, self).hkeys(self._normalize_key(name))


    def hlen(self, name):
        "Return the number of elements in hash ``name``"
        return super(PrefixedRedis, self).hlen(self._normalize_key(name))


    def hset(self, name, key, value, pickle=False):
        """
        Set ``key`` to ``value`` within hash ``name``
        Returns 1 if HSET created a new field, otherwise 0
        """
        return super(PrefixedRedis, self).hset(self._normalize_key(name),
                key, value, pickle)


    def hsetnx(self, name, key, value, pickle=False):
        """
        Set ``key`` to ``value`` within hash ``name`` if ``key`` does not
        exist.  Returns 1 if HSETNX created a field, otherwise 0.
        """
        return super(PrefixedRedis, self).hsetnx(self._normalize_key(name),
                key, value, pickle)


    def hmset(self, name, mapping, pickle=False):
        """
        Set key to value within hash ``name`` for each corresponding
        key and value from the ``mapping`` dict.
        """
        return super(PrefixedRedis, self).hmset(self._normalize_key(name),
                mapping, pickle)


    def hmget(self, name, keys, *args, **kwargs):
        "Returns a list of values ordered identically to ``keys``"
        pickle = kwargs.pop('pickle', False)
        return super(PrefixedRedis, self).hmget(self._normalize_key(name),
                keys, *args, pickle=pickle)


    def hvals(self, name, pickle=False):
        "Return the list of values within hash ``name``"
        return super(PrefixedRedis, self).hvals(self._normalize_key(name),
                pickle)


    def hstrlen(self, name, key):
        """
        Return the number of bytes stored in the value of ``key``
        within hash ``name``
        """
        return super(PrefixedRedis, self).hstrlen(self._normalize_key(name),
                key)


    # GEO COMMANDS
    def geoadd(self, name, *values):
        """
        Add the specified geospatial items to the specified key identified
        by the ``name`` argument. The Geospatial items are given as ordered
        members of the ``values`` argument, each item or place is formed by
        the triad longitude, latitude and name.
        """
        return super(PrefixedRedis, self).geoadd(self._normalize_key(name),
                *values)


    def geodist(self, name, place1, place2, unit=None):
        """
        Return the distance between ``place1`` and ``place2`` members of the
        ``name`` key.
        The units must be one of the following : m, km mi, ft. By default
        meters are used.
        """
        return super(PrefixedRedis, self).geodist(self,_normalize_key(name),
                place1, place2, unit)


    def geohash(self, name, *values):
        """
        Return the geo hash string for each item of ``values`` members of
        the specified key identified by the ``name``argument.
        """
        return super(PrefixedRedis, self).geohash(self._normalize_key(name),
                *values)


    def geopos(self, name, *values):
        """
        Return the positions of each item of ``values`` as members of
        the specified key identified by the ``name``argument. Each position
        is represented by the pairs lon and lat.
        """
        return super(PrefixedRedis, self).geopos(self._normalize_key(name),
                *values)


    def georadius(self, name, longitude, latitude, radius, unit=None,
                  withdist=False, withcoord=False, withhash=False, count=None,
                  sort=None, store=None, store_dist=None):
        """
        Return the members of the specified key identified by the
        ``name`` argument which are within the borders of the area specified
        with the ``latitude`` and ``longitude`` location and the maximum
        distance from the center specified by the ``radius`` value.

        The units must be one of the following : m, km mi, ft. By default

        ``withdist`` indicates to return the distances of each place.

        ``withcoord`` indicates to return the latitude and longitude of
        each place.

        ``withhash`` indicates to return the geohash string of each place.

        ``count`` indicates to return the number of elements up to N.

        ``sort`` indicates to return the places in a sorted way, ASC for
        nearest to fairest and DESC for fairest to nearest.

        ``store`` indicates to save the places names in a sorted set named
        with a specific key, each element of the destination sorted set is
        populated with the score got from the original geo sorted set.

        ``store_dist`` indicates to save the places names in a sorted set
        named with a specific key, instead of ``store`` the sorted set
        destination score is set with the distance.
        """
        return super(PrefixedRedis, self).georadius(self._normalize_key(name),
                longitude, latitude, radius, unit, withdist, withcoord,
                withhash, count, sort, store, store_dist)


    def georadiusbymember(self, name, member, radius, unit=None,
                          withdist=False, withcoord=False, withhash=False,
                          count=None, sort=None, store=None, store_dist=None):
        """
        This command is exactly like ``georadius`` with the sole difference
        that instead of taking, as the center of the area to query, a longitude
        and latitude value, it takes the name of a member already existing
        inside the geospatial index represented by the sorted set.
        """
        return super(PrefixedRedis, self).georadiusbymember(
                self._normalize_key(name), member, radius, unit, withdist,
                withcoord, withhash, count, sort, store, store_dist)

# -*- coding: utf-8 -*-
"""
Created on 09/11/2011
@author: Carlo Pires <carlopires@gmail.com>
@author: Fahri Reza <dozymoe@gmail.com>
@copyright: January 2018

Source: https://gist.github.com/carlopires/1451947
"""
import logging
import os
import tempfile
from werkzeug.contrib.sessions import SessionStore
from .singletons import get_persistent_cache

SESSION_TIMEOUT = 60*60*24*7 # 7 weeks in seconds

_logger = logging.getLogger(__name__)

class RedisSessionStore(SessionStore):
    """
    SessionStore that saves session to redis
    """
    path = None
    redis = None
    key_prefix = 'session.'

    def __init__(self, path=None, session_class=None):
        super(RedisSessionStore, self).__init__(session_class)
        if path is None:
            path = tempfile.gettempdir()
        self.path = path
        self.redis = get_persistent_cache()


    def save(self, session):
        self._touch_files(session)
        self.redis.setex(self.key_prefix + session.sid, SESSION_TIMEOUT,
                dict(session), pickle=True)


    def delete(self, session):
        return self.redis.delete(self.key_prefix + session.sid)


    def get(self, sid):
        if not self.is_valid_key(sid):
            return self.new()

        key = self.key_prefix + sid
        data = self.redis.get(key, pickle=True)
        if data:
            self.redis.expire(key, SESSION_TIMEOUT)
            self._touch_files(data)
        else:
            data = {}

        return self.session_class(data, sid, False)


    def list(self):
        """
        Lists all sessions in the store.
        """
        session_keys = self.redis.keys(self.key_prefix + '*')
        return [s[len(self.key_prefix):] for s in session_keys]


    def _touch_files(self, data):
        request_data = data.get('serialized_request_data')
        if request_data:
            for storename, filename, ctype in request_data['files'].values():
                try:
                    os.utime(filename, None)
                except OSError:
                    pass

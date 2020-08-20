# -*- coding: utf-8 -*-

# Copyright 2020 Susumu OTA
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from os import getenv
from io import BytesIO
from json import loads, dumps
from hashlib import md5
from datetime import datetime
from datetime import timezone
from operator import attrgetter
from functools import update_wrapper
from collections import namedtuple

from boto3 import resource


def s3_fifo_cache(maxsize=128, typed=False, bucket_name=None):
    return _s3_cache(maxsize, typed, bucket_name, False)

def s3_lru_cache(maxsize=128, typed=False, bucket_name=None):
    return _s3_cache(maxsize, typed, bucket_name, True)


# See https://github.com/python/cpython/blob/master/Lib/functools.py#L430
_CacheInfo = namedtuple("CacheInfo", ["hits", "misses", "maxsize", "currsize"])


class _HashedSeq(list):
    """See https://github.com/python/cpython/blob/master/Lib/functools.py#L432"""
    __slots__ = 'hashvalue'

    def __init__(self, tup, hash=hash):
        self[:] = tup
        self.hashvalue = hash(tup)

    def __hash__(self):
        return self.hashvalue


def _make_key(args, kwds, typed,
             kwd_mark = (object(),),
             fasttypes = {int, str},
             tuple=tuple, type=type, len=len):
    """See https://github.com/python/cpython/blob/master/Lib/functools.py#L448"""
    key = args
    if kwds:
        key += kwd_mark
        for item in kwds.items():
            key += item
    if typed:
        key += tuple(type(v) for v in args)
        if kwds:
            key += tuple(type(v) for v in kwds.values())
    elif len(key) == 1 and type(key[0]) in fasttypes:
        return key[0]
    return _HashedSeq(key)


def _s3_cache(maxsize, typed, bucket_name, is_lru):
    """See https://github.com/python/cpython/blob/master/Lib/functools.py#L479"""
    if callable(maxsize) and isinstance(typed, bool):
        # The user_function was passed in directly via the maxsize argument
        user_function, maxsize = maxsize, 128
        wrapper = _s3_cache_wrapper(user_function, maxsize, typed, _CacheInfo, bucket_name, is_lru)
        wrapper.cache_parameters = lambda : {'maxsize': maxsize, 'typed': typed}
        return update_wrapper(wrapper, user_function)
    elif maxsize is None or (isinstance(maxsize, int) and maxsize > 0):
        pass
    else:
        raise TypeError('Expected first argument to be a non-zero positive integer, a callable or None')

    def decorating_function(user_function):
        wrapper = _s3_cache_wrapper(user_function, maxsize, typed, _CacheInfo, bucket_name, is_lru)
        wrapper.cache_parameters = lambda : {'maxsize': maxsize, 'typed': typed}
        return update_wrapper(wrapper, user_function)

    return decorating_function


def _s3_cache_wrapper(user_function, maxsize, typed, _CacheInfo, bucket_name, is_lru):
    """See https://github.com/python/cpython/blob/master/Lib/functools.py#L525"""
    sentinel = object()
    make_key = _make_key
    # cache = {}
    cache = _S3Dict(bucket_name, is_lru=is_lru)
    hits = misses = 0
    cache_get = cache.get
    cache_len = cache.__len__
    if maxsize is None or (isinstance(maxsize, int) and maxsize > 0):
        def wrapper(*args, **kwds):
            # Size limited caching that tracks accesses by recency
            nonlocal hits, misses
            k = make_key(args, kwds, typed, kwd_mark = ('kwd_mark',))
            key = md5(str(k).encode()).hexdigest()
            result = cache_get(key, sentinel)
            if result is not sentinel:
                hits += 1
                return result
            misses += 1
            result = user_function(*args, **kwds)
            if isinstance(maxsize, int) and cache_len() >= maxsize:
                cache.popitem(last=False) # delete oldest item (FIFO)
            cache[key] = result
            return result
    else:
        raise TypeError('Expected maxsize argument to be a non-zero positive integer or None')

    def cache_info():
        """Report cache statistics"""
        return _CacheInfo(hits, misses, maxsize, cache_len())

    def cache_clear():
        """Clear the cache and cache statistics"""
        nonlocal hits, misses
        cache.clear()
        hits = misses = 0

    def cache_set_expiration(days):
        """Set cache expiration days"""
        cache.set_expiration(days)

    wrapper.cache_info = cache_info
    wrapper.cache_clear = cache_clear
    wrapper.cache_set_expiration = cache_set_expiration
    return wrapper


class _S3Dict: # class _S3Dict(dict): # TODO: implement dict (or OrderedDict) interface properly
    def __init__(self, bucket_name, is_lru=False):
        self.s3_bucket = resource('s3').Bucket(bucket_name) # s3.resource
        self.is_lru = is_lru # FIFO or LRU

    def __getitem__(self, key):
        with BytesIO() as f:
            self.s3_bucket.download_fileobj(Key=key, Fileobj=f)
            if self.is_lru:
                # there is no 'touch' API. instead it needs to use 'copy' which updates 'last_modified' field.
                # https://stackoverflow.com/a/39596988
                # https://faragta.com/aws-s3/touch-command.html
                metadata = self.s3_bucket.Object(key).metadata
                self.s3_bucket.copy(CopySource={'Bucket': self.s3_bucket.name, 'Key': key}, Key=key, ExtraArgs={'Metadata': metadata, 'MetadataDirective': 'REPLACE'})
            return loads(f.getvalue().decode(encoding='utf-8'))

    def get(self, key, default=None):
        try:
            return self[key]
        except Exception as e:
            # print(str(e))
            return default

    def __setitem__(self, key, value):
        with BytesIO(dumps(value).encode(encoding='utf-8')) as f:
            self.s3_bucket.upload_fileobj(Fileobj=f, Key=key, ExtraArgs={'Metadata': {'Created': datetime.now(timezone.utc).isoformat()}})

    def __len__(self):
        # return len(list(self.s3_bucket.objects.all()))
        # https://stackoverflow.com/a/32409177
        return sum(1 for _ in self.s3_bucket.objects.all())

    def clear(self):
        self.s3_bucket.objects.all().delete()

    def popitem(self, last=True):
        # https://docs.python.org/3/library/collections.html#collections.OrderedDict.popitem
        # LIFO order if last is true or FIFO order if false
        # https://stackoverflow.com/a/45379754
        target_func = max if last else min # no need to use 'sorted'
        target = target_func(self.s3_bucket.objects.all(), key=attrgetter('last_modified'), default=None) # find the oldest or latest object
        if target is None:
            raise KeyError('popitem(): dictionary is empty')
        else:
            target.delete()
            return target.key, None # None is dummy because self[key] takes too much cost

    # set cache TTL
    def set_expiration(self, days=None):
        if days is None:
            self.s3_bucket.LifecycleConfiguration().delete()
        elif isinstance(days, int) and days > 0:
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.BucketLifecycleConfiguration.put
            self.s3_bucket.LifecycleConfiguration().put(
                LifecycleConfiguration={
                    'Rules': [{'Expiration': {'Days': days},
                               'Filter': {'Prefix': ''},
                               'ID': f'Expire after {days} days',
                               'Status': 'Enabled'}]})
        else:
            raise TypeError('Expected days argument to be an non-zero positive integer or None')

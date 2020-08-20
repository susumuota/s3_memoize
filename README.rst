s3_memoize
==========

Memoization function decorators using Amazon S3 for Python

Install
-------

::

   pip install s3-memoize

Usage
-----

Setup AWS credentials.

Make test.py.

.. code:: test.py

   BUCKET_NAME='mybucketname'

   @s3_fifo_cache(maxsize=2, typed=False, bucket_name=BUCKET_NAME)
   # @s3_lru_cache(maxsize=2, typed=False, bucket_name=BUCKET_NAME)
   def test(num):
       print(f'test: {num}')
       return num * 2

   print(test.cache_clear())
   print(test.cache_info())
   print(test(10))
   print(test.cache_info())
   print(test(10))
   print(test.cache_info())
   print(test(20))
   print(test.cache_info())
   print(test(20))
   print(test.cache_info())
   print(test(10))
   print(test.cache_info())
   print(test(30))
   print(test.cache_info())
   print(test(30))
   print(test.cache_info())

Run.

::

   AWS_PROFILE=myprofile python test.py

Author
------

Susumu OTA

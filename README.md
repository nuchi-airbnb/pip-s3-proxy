Caching S3 Proxy 
----

Provides an unauthenticated plain HTTP frontend 
for public and private S3 buckets, and caches on the filesystem.
Least Recently Used objects are evicted from the cache first.

Example:
```
  python setup.py install
  caching-s3-proxy &
  curl localhost:8000/org.mozilla.crash-stats.symbols-private/v1/symupload-1.0-Linux-20120709194529-symbols.txt
```

If you want to listen on a different port, just set the PORT variable:
```
  PORT=9999 caching-s3-proxy
```

NOTE - the cache code is not threadsafe, this is intended to be run
single threaded. You probably do want this to block anyway, to prevent a
thundering-herd problem if there are multiple clients hitting it when
the cache is cold.

Pip S3 Proxy
----

Provides an unauthenticated plain HTTP frontend for public and private
S3 buckets that's intended to be used as a front-end so that pip can
install packages from S3.

It's based on https://github.com/rhelmer/caching-s3-proxy and supports
many of the same features.  Thank you rhelmer!

It works by wrapping pip.  You run 'pipsss' instead of 'pip' - pipsss
starts a web server in the background and then passes your CLI
parameters to pip.  At the moment you'll definitely want to add an
extra-index-url to the command line but in the future we might have
pipsss add that itself.

Example:
```
  python setup.py install
  (set up your s3 credentials however you like to do that)
  pipsss install --extra-index-url http://localhost:8000/my-pypi-bucket my-private-package
```

You can still use the web server in a standalone mode.  It works like
caching-s3-proxy.

Example:
```
  python setup.py install
  (set up your s3 credentials however you like to do that)
  pip-s3-proxy &
  curl localhost:8000/my_bucket/v1/my_file.txt
```

If you want to listen on a different port, just set the PORT variable:
```
  PORT=9999 pip-s3-proxy
```

Alternatively, you can run under uwsgi. It's safe to use multiple workers
processes (the shared file cache uses file locking to allow concurrency):
```
  uwsgi -w proxy.wsgi --http=localhost:8000 --workers=10
```

If you want to put this behind Nginx or Apache, use a socket instead:
```
  uwsgi -w proxy.wsgi -s /var/run/pip-s3-proxy.sock --workers=10
```

Then see http://uwsgi-docs.readthedocs.org/en/latest/Nginx.html or
http://uwsgi-docs.readthedocs.org/en/latest/Apache.html

import boto3
import botocore
import hashlib
import logging
from proxy.cache import LRUCache
from proxy.cache import NoOpCache
import tempfile


class CachingS3Proxy(object):
    def __init__(self, capacity=(10*10**9), cache_dir=tempfile.gettempdir()):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        if capacity:
            self.cache = LRUCache(capacity, cache_dir)
        else:
            self.cache = NoOpCache()
        self.s3 = boto3.resource('s3')

    def proxy_s3_bucket(self, environ, start_response):
        """proxy private s3 buckets"""
        path_info = environ.get('PATH_INFO', '')
        if path_info == '/':
            status = '200 OK'
            response_headers = [('Content-type', 'text/plain')]
            start_response(status, response_headers)
            return ['Caching S3 Proxy']

        path_info = path_info.lstrip('/')
        (bucket, key) = path_info.split('/', 1)
        status = '200 OK'
        response_headers = []
        try:
            s3_result = self.fetch_s3_object(bucket, key)
        except botocore.exceptions.ClientError as ce:
            # this is a compatibility hack for pip.  tools like s3pypi
            # build index.html files that pip can use to find out what
            # versions of a package are available.  pip expects the
            # web server to serve an index page, though, so we need to
            # make another request to get an index.html page if there
            # is one.
            if key.endswith('/'):
                try:
                    s3_result = self.fetch_s3_object(bucket, key + 'index.html')
                    response_headers = [('Content-type', 'text/html')]
                except botocore.exceptions.ClientError:
                    s3_result = ce.response['Error']['Message']
                    status = '404 NOT FOUND'
                    response_headers = [('Content-type', 'text/plain')]

        start_response(status, response_headers)
        return [s3_result]

    def fetch_s3_object(self, bucket, key):
        m = hashlib.md5()
        m.update(bucket+key)
        cache_key = m.hexdigest()

        try:
            return self.cache[cache_key]
        except KeyError:
            self.logger.debug('cache miss for %s' % cache_key)

            obj = self.s3.Object(bucket, key).get()
            body = obj['Body'].read()
            self.cache[cache_key] = body
            return body

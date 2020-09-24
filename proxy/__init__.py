from __future__ import print_function
import boto3
import botocore
import hashlib
import logging
from proxy.cache import LRUCache
from proxy.cache import NoOpCache
import tempfile


class CachingS3Proxy(object):
    def __init__(self, capacity=(10 * 10**9), cache_dir=tempfile.gettempdir()):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        if capacity:
            self.cache = LRUCache(capacity, cache_dir)
        else:
            self.cache = NoOpCache()
        self.s3 = boto3.resource('s3')
        self.paginator = boto3.client('s3').get_paginator('list_objects_v2')

    def proxy_s3_bucket(self, environ, start_response):
        """proxy private s3 buckets"""
        path_info = environ.get('PATH_INFO', '')
        if path_info == '/':
            status = '200 OK'
            response_headers = [('Content-type', 'text/plain')]
            start_response(status, response_headers)
            return ['Caching S3 Proxy'.encode('utf-8')]

        path_info = path_info.lstrip('/')

        if '/' not in path_info:
            status = '404 NOT FOUND'
            start_response(status, [])
            return ['No bucket specified'.encode('utf-8')]

        (bucket, key) = path_info.split('/', 1)
        status = '200 OK'
        response_headers = []
        try:
            # this is a compatibility hack for pip.  pip expects the
            # web server to serve an index page, so we need to
            # generate one if the path is a 'directory'
            if key.endswith('/'):
                listing = self.fetch_directory_listing(bucket, key)
                s3_result = self.serve_index(key, listing)
                response_headers = [('Content-type', 'text/html')]
            else:
                s3_result = self.fetch_s3_object(bucket, key)
        except botocore.exceptions.ClientError as ce:
            s3_result = [ce.response['Error']['Message'].encode('utf-8')]
            status = '404 NOT FOUND'

        start_response(status, response_headers)
        return s3_result

    def fetch_s3_object(self, bucket, key):
        m = hashlib.md5()
        m.update((bucket + key).encode('utf-8'))
        cache_key = m.hexdigest()

        try:
            return self.cache[cache_key]
        except KeyError:
            self.logger.debug('cache miss for %s' % cache_key)

            obj = self.s3.Object(bucket, key).get()
            body = obj['Body'].read()
            self.cache[cache_key] = body
            return [body]

    def fetch_directory_listing(self, bucket, key):
        """Fetch a listing of the ersatz 'directory'.  S3 is an object store
        so it doesn't have directories as such, but we're pretending
        that it's a file system.

        """
        listing = self.paginator.paginate(Bucket=bucket, Prefix=key, Delimiter='/').build_full_result()

        files = [
            { 'name': l['Key'][len(key):], 'uri': l['Key'][len(key):] }
            for l in listing.get('Contents', [])
        ]
        subdirectories = [
            { 'name': l['Prefix'][len(key):-1], 'uri': l['Prefix'][len(key):] }  # Strip trailing slash from name
            for l in listing.get('CommonPrefixes', [])
        ]

        return files + subdirectories

    def serve_index(self, base_key, listing):
        """Generate an HTML index page from a listing of objects."""

        yield "<html><head><title>Package Index</title></head><body>".encode('utf-8')
        for item in listing:
            yield('<a href="{uri}">{name}</a><br/>'.format(uri=item['uri'], name=item['name'])).encode('utf-8')
        yield "</body></html>".encode('utf-8')

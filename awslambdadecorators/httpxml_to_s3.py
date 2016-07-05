import xml.etree.ElementTree as ET
import uuid
import httplib
import logging
import json
import urlparse
import boto.s3 as s3

import s3etag
import common

logger = logging.getLogger('httpxml_to_s3')

def httpxml_to_s3(http_url,
                  s3_url,
                  region='eu-west-1',
                  profile_name=None,
                  xml_preprocessor=lambda s: s):
    def inner(fn_inner):
        return sync_to_bucket(s3_url, region, profile_name)(from_xml(http_url)(fn_inner))
    return inner;

def sync_to_bucket(s3_url,
                   region='eu-west-1',
                   profile_name=None):
    """
    Decorator function configuring function
    xml_preprocessor - If some preprocessing needs to be done on the xml as
                       a string a lambda can be sent in. Defaults to the
                       identity function
    """

    parsed_s3_url = urlparse.urlparse(s3_url);

    bucket_name = parsed_s3_url.hostname;
    key_prefix = parsed_s3_url.path;
    if key_prefix[0] == '/':
        key_prefix = key_prefix[1:]
    if key_prefix[-1] != '/':
        key_prefix = key_prefix + '/'

    def inner(fn_inner):
        """
        Decorator function function sent in should be having signature
        func(None,None, XmlDoc) and should yield JSON document one for
        each file that should be persisted to S3
        """

        def handler(event, context):
            """
            The AWS Lambda Entry Point
            """
            s3conn = s3.connect_to_region(region, profile_name=profile_name)
            bucket = s3conn.get_bucket(bucket_name)

            # Use a map to track keys that are no longer in the feed, used for deletion
            remaining_keys = { key.name : True for key in bucket.list(prefix=key_prefix)}

            logger.debug("Existing keys in bucket\n%s", '\n'.join(remaining_keys));

            for id, json_data in fn_inner():
                key_name = key_prefix + str(uuid.uuid5(uuid.NAMESPACE_URL, id.encode('utf-8')))

                # Key found, delete it from cleanup map
                if key_name in remaining_keys:
                    del remaining_keys[key_name]

                string_data = json.dumps(json_data)
                s3_object = bucket.get_key(key_name)
                if s3_object == None:
                    key = bucket.new_key(key_name);
                    key.set_contents_from_string(string_data)
                    logger.info('Creating:\ts3://%s/%s', bucket_name, key_name)
                    logger.debug(string_data)
                else:
                    if s3_object.etag[1:len(s3_object.etag)-1] != s3etag.from_string(string_data):
                        logger.info('Updating:\ts3://%s/%s', bucket_name, key_name)
                        logger.debug(string_data)
                        s3_object.set_contents_from_string(string_data)
                    else:
                        logger.info('Same:\ts3://%s/%s', bucket_name, key_name);
                        logger.debug(string_data)

            # Remvoe remaining keys from the bucket to allow for cleanup
            for key in remaining_keys:
                logger.info('Removing:\ts3://%s/%s', bucket_name, key);
                bucket.delete_key(key);

            logger.info('Done');

        return handler

    return inner


def from_xml(url):
    def inner(fn_inner):
        def handler():
            input_xml = ET.fromstring(common.get_page(url))
            return fn_inner(input_xml)
        return handler
    return inner

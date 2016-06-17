import xml.etree.ElementTree as ET
import uuid
import httplib
import logging
import json

import boto.s3 as s3

import s3etag

FORMAT = '%(asctime)-15s %(levelname)s: %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger('httpxml_to_s3')
logger.setLevel(logging.INFO)


def httpxml_to_s3(config1, xml_preprocessor=lambda s: s):
    """
    Decorator function configuring function
    config1 - Is a ConfigParser parser object containing information on which
              URL to poll and the location on s3 where to store the synced
              files.
    xml_preprocessor - If some preprocessing needs to be done on the xml as
                       a string a lambda can be sent in. Defaults to the
                       identity function
    """
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
            s3conn = s3.connect_to_region('eu-west-1', profile_name='personal')
            bucket_name = config1.get('httpxml_to_s3', 'bucket')
            key_prefix = config1.get('httpxml_to_s3', 'key_prefix')

            input_xml = get_page(config1.get('httpxml_to_s3', 'host'),
                                 config1.getint('httpxml_to_s3', 'port'),
                                 config1.get('httpxml_to_s3','path'), xml_preprocessor)

            bucket = s3conn.get_bucket(bucket_name)

            # Use a map to track keys that are no longer in the feed, used for deletion
            remaining_keys = { key.name : True for key in bucket.list(prefix=key_prefix)}

            logger.debug("Existing keys in bucket\n%s", '\n'.join(remaining_keys));

            for id, json_data in fn_inner(input_xml):
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

def get_page(host, port, path, xml_preprocessor=lambda s: s):
    logger.info("Syncing feed on https://%s:%s%s",
                host,
                port,
                path)
    conn = httplib.HTTPSConnection(host, port)
    conn.request('GET', path);
    resp = conn.getresponse()
    response_string = xml_preprocessor(resp.read())
    resp.close()
    conn.close()
    return ET.fromstring(response_string)


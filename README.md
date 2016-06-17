# lambda-decorators
Handy Python Decorators for AWS Lambda


## XML over HTTP to S3 Sync
Following example will sync from an xml feed published on a http endpoint and publish these as files on S3.

It will remove files from the bucket if a yield disappears, and it will not update the file
on S3 if the etag has not changed.

The input is an etree document

One output file will be created for each yield that is made from this generator function. The yield is a tuple where the first has to be parseable as some type of URI, the second element is a JSON payload to store in the file. The name of the file in S3 is the UUIDv5 generated from the URI

This could be useful for integration flows where a SNS topic is receiving the envents from the S3 buket and then triggering functionalities on addition, modification or deletion of some events in the feed.

```python    
from awslambdadecorators import httpxml_to_s3
@httpxml_to_s3('https://host:port/path/to/xmlfeed',
               's3://bucket/path/to/prefix,
               profile_name='aws_profile',
               region='eu-west-1')
def handler(input_xml):
    print "Yielding value"
    # This will create a file in the bucket
    yield 'test://1', {'json' : 'test'}
```

# Known Issues
The XML document is not streamed so too massive documents may cause excessive heap usage

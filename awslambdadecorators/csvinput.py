import csv
import urlparse
import httplib
import logging
import StringIO

import common

logger = logging.getLogger(__name__)

def from_csv(url):
    """
    Decorator that will provide dictionary objects
    """
    def inner(fn_inner):
        def handler():
            logging.info("Opening URL: %s", url);
            f = StringIO.StringIO(common.get_page(url))
            jobreader = csv.DictReader(f, delimiter=',', quotechar="\"")
            for i, row in enumerate(jobreader):
                r = fn_inner(i, row)
                if r is not None:
                    yield r
        return handler
    return inner

def from_gsheet(id):
    """
    Convenience decorator that loads a sheet from google. Note that the sheet must be publically shared
    with read access for this to work
    """
    return from_csv('https://docs.google.com/spreadsheets/d/%s/export?gid=0&format=csv' % id)

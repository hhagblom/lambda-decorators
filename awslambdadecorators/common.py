import urlparse
import httplib


def get_page(url):
    p = urlparse.urlparse(url)
    try:
        if p.scheme == 'https':
            conn = httplib.HTTPSConnection(p.hostname, p.port if p.port is not None else 443)
        else:
            conn = httplib.HTTPConnection(p.hostname, p.port if p.port is not None else 80)

        conn.request('GET', urlparse.urlunparse(['', ''] + list(p[2:]))) # The urlunparse removes scheme host and port from the url
        res = conn.getresponse()
        if res.status != 200:
            raise Exception("Got an unexpected status code %d from url %s" % (res.status, url))
        ret = res.read()
        return ret
    finally:
        res.close();
        conn.close();
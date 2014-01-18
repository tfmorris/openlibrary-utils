'''
Given a list of OpenLibrary URLs for books which have ePubs available on Internet Archive,
download the IA MARC XML file and update it with our desired info.
'''

import codecs
import xml.etree.ElementTree  as ET
import pymarc
from requests import ConnectionError
import requests_cache
import time
import urllib

CACHE_DIR = '../cache/'
DATA_DIR = '../data/'
RATE = 2.0 # requests/second

count = 0

requests_cache.install_cache('openlibrary')
session = requests_cache.CachedSession()

bitly = file('../bitly_credentials.txt').readlines()

BITLY_LOGIN = bitly[0].rstrip('\n').strip()
BITLY_API_KEY = bitly[1].rstrip('\n').strip()

def make_throttle_hook(timeout=1.0):
    """
    Returns a response hook function which sleeps for `timeout` seconds if
    response is not cached
    """
    def hook(response, **kwargs):
        if not getattr(response, 'from_cache', False):
            time.sleep(timeout)
        return response
    return hook

session.hooks = {'response': make_throttle_hook(1.0/RATE)}

def get_json(url):
    response = session.get(url)
    if response.status_code == 200:
        edition = response.json()
        return edition
    else:
        print 'Failed to get JSON for %s - status code %d' % (url,response.status_code)
        
def get_ia_edition(iaid):
    '''Get JSON for an edition using its IA identifier.  Follows non-HTTP OpenLibrary redirect records '''
    edition_url = 'http://openlibrary.org/books/ia:%s.json' % iaid
    edition = get_json(edition_url)
    if edition and 'type' in edition and 'key' in edition['type'] and edition['type']['key'] == '/type/redirect':
        edition_url =  'http://openlibrary.org%s.json' % edition['location']
        edition = get_json(edition_url)
    return edition

def get_file(iaid,suffix,body=False):
    '''
    Test whether a file in the given format is available for an Internet Archive ID.
    Follows redirects if necessary.
    
    If the "suffix" parameter doesn't contain a period, one will be prepended.
    This allows both "epub" and "_files.xml" style suffixes.

    if body=False (default), the content will not be fetched.  Redirects are followed and the
    final URL is returned.  If this is True, the actual content will be fetched and returned.
    '''
    #files = session.get('http://archive.org/download/%s/%s_files.xml' % (iaid,iaid))
    if suffix.find('.') < 0:
        suffix = '.'+suffix
    url = 'http://archive.org/download/%s/%s%s' % (iaid,iaid,suffix)

    try:
        if not body:
            epub = session.head(url)
        else:
            epub = session.get(url)
        
        if epub.status_code == 302:
            url = epub.headers['location']
            #print 'Redirecting to ',url
            if not body:
                epub = session.head(url)
            else:
                epub = session.get(url)
        if epub.status_code == 200:
            return epub if body else url
        elif epub.status_code != 403:
            print 'HTTP error (%d) fetching %s for %s from %s ' % (epub.status_code,suffix,iaid,url)
    except ConnectionError as e:
        print 'Error fetching URL: ',url,e
    return None

def rate_wait(time):
    last = now()
    while True:
        yield()
    
def get_files(ia):
    suffix = '_files.xml'
    filename = CACHE_DIR + ia + suffix
    root = None
    
    # check cache
    try:
        with file(filename, 'r') as cachefile:
            root = ET.parse(cachefile).getroot()
    except IOError:
        files_xml =  get_file(ia,suffix,body=True)
        if files_xml and files_xml.status_code == 200:
            with file(filename, 'w') as output:
                output.write(files_xml.content)
            root = ET.fromstring(files_xml.content)
    if root is not None:
        files = [f.get('name') for f in root.findall('file')]
        return files

def find_file(files,suffix):
    if files and suffix:
        for f in files:
            if f[-len(suffix):len(f)] == suffix:
                return f

def url_field(url,tag):
    return pymarc.Field(
        tag = '856',
        indicators = ['4','0'],
        subfields = [
            'u', url,
            'z', 'Always available eBooks (%s)' % tag,
        ])

def add_url(record, url,tag):
    # Amazon will return 405 for a HEAD verb, so just skip the check
    # it's a service which should always be available anyway
    skip_check = url.find("amazon.com") > 0
    
    # For our initial project, all URLs except one succeeded and that was hand-verified
    # to be an intermittent error, so we're going to skip this check for now because  its
    # causing more problems than it's designed to prevent due to IA instability
    skip_check = True
    
    if not skip_check:
        response = session.head(url)
        if response.status_code == 302:
            response = session.head(response.headers['Location'])
    if skip_check or response.status_code == 200:
        short_url = shorten_url(url)
        record.add_ordered_field(url_field(short_url, tag))
    else:
        print "Unable to resolve URL: ",url, response

def update_marc_record(record,iaid,olurl):
    # Delete Internet Archive labels for 856s and add our own
    for field in record.get_fields('856'):
        record.remove_field(field)
    add_url(record, "https://archive.org/download/%s/%s.epub" % (iaid,iaid),"ePub")
    add_url(record, "https://www.amazon.com/gp/digital/fiona/web-to-kindle?clientid=IA&itemid=%s&docid=%s" % (iaid, iaid),"Kindle")
    add_url(record, olurl,"multiple formats")

    # Add a series
    record.add_ordered_field(pymarc.Field(
                                tag = u'490',
                                indicators = [u'1',''],
                                subfields = [
                                    u'a', u'Rediscover the classics',
                                ])
                     )
    # Add a couple new subjects
    record.add_ordered_field(pymarc.Field(
                                tag = u'655',
                                indicators = [u'4',''],
                                subfields = [
                                    u'a', u'Electronic books.',
                                ])
                     )
    record.add_ordered_field(pymarc.Field(
                                tag = u'655',
                                indicators = [u'4',''],
                                subfields = [
                                    u'a', u'EBook classics.',
                                ])
                     )
    # Add additional Uniform Title
    record.add_ordered_field(pymarc.Field(
                                tag = u'830',
                                indicators = [u'0',''],
                                subfields = [
                                    u'a', u'Rediscover the classics.',
                                ])
                     )
    return record

def shorten_url(url):
    template = 'http://api.bit.ly/v3/shorten?login=%s&apiKey=%s&longUrl=%s'
    url = urllib.quote(url)
    requesturl = template % (BITLY_LOGIN, BITLY_API_KEY, url)
    response = session.get(requesturl)
    json = response.json()
    if json['status_code'] != 200:
        print 'Failed to shorten URL ', url, response
    else:
        short_url = json['data']['url']
        print url,'\t',short_url
        return short_url
    
def main():
#    with pymarc.MARCWriter(file(DATA_DIR+'SCCLclassics.mrc','wb')) as writer:
    writer = pymarc.MARCWriter(codecs.open(DATA_DIR+'SCCLclassics.mrc','w','utf-8'))
    count = 0
    for line in codecs.open(DATA_DIR+'SCCL classics candidates - v3 selected.tsv', encoding='utf-8'):
        count += 1
        if count == 1:
            continue # skip header line
        url = line.rstrip('\n').split('\t')[6].replace('https:','http:')
        print '  ',url
        jsonurl = '/'.join(url.split('/')[0:5])+'.json'
        json = get_json(jsonurl)
        if 'ocaid' in json:
            ia = json['ocaid']
            # This is the Internet Archive version of the MARC record for the electronic version e.g.
            #   https://archive.org/download/myantonia00cathrich/myantonia00cathrich_archive_marc.xml
            # not the original libraries MARC record for the paper book e.g.
            #   https://archive.org/download/myantonia00cathrich/myantonia00cathrich_marc.xml
            marcurl = 'http://archive.org/download/%s/%s_archive_marc.xml' % (ia,ia)
            records = None
            retries = 0
            while retries < 3 and records == None:
                retries += 1
                try:
                    records = pymarc.parse_xml_to_array(marcurl)
                except SAXParseException:
                    records = None
            record = update_marc_record(records[0],ia,url)
            record.force_utf8 = True
            try:
                writer.write(record)
            except:
                'Print failed to write MARC record for ',marcurl
        else:
            print 'Unexpectedly missing ocaid for ',jsonurl
    writer.close()
    print 'Wrote %d MARC records' % (count-1)

main()

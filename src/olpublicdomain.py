'''
Given a list of candidate public domain book titles and authors attempt to find the best matching
OpenLibrary records with full text available on Internet Archive.
'''

import codecs
import xml.etree.ElementTree  as ET
import pymarc
from requests import ConnectionError
import requests_cache
import time

CACHE_DIR = '../cache/'
DATA_DIR = '../data/'
RATE = 2.0 # requests/second
count = 0

requests_cache.install_cache('openlibrary')
session = requests_cache.CachedSession()
    
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

def search_open_library(author,title, language):
    result = []
    author = author.strip()
    author = ','.join(author.split(',')[0:2])
    if author and author[-1] == '.':
        author = author[0:-1]
    payload = {'has_fulltext' : 'true', 'title': title} #, 'language': language}
    if author:
        payload.update({'author': author})
    response = session.get('http://openlibrary.org/search.json',params=payload)
    if response.status_code == 200:
        docs = response.json()['docs']
        result = docs
    else:
        print 'Request failed',response.status_code,payload
    return result


def check_language(lang,doc):
    if 'language' in doc:
        if  lang in doc['language']:
            return True
        else:
            print 'Non-English version ',doc['key'],doc['language']
    else:
        # TODO:
        #aid = doc['ocaid']
        #marc_url = 'https://archive.org/download/%s/%s_marc.xml' % (aid,aid)
        # download MARC
        # check for 'English' in 240$l
        
        print 'Unknown language ',doc['key']
        return False

def all_editions(doc):
    '''
    Merge the contents of the two IA editions fields
    (one may be a superset of the other, but lets just be safe)
    '''
    editions = set()
    for k in ('ia_loaded_id','ia'):
        if k in doc:
            editions.update(doc[k])
    print len(editions), ' editions'
    return editions

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
    final URL is returned.  If this is false, the actual content will be fetched and returned.
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

def merge(base, added):
    keys = [b['key'] for b in base]
    base += [a for a in added if not a['key'] in keys]
    return base

def publicdomain(date):
    if not date:
        return False
    try:
        return int(date) < 1923
    except ValueError:
        return False
    
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
        
def main():
    with codecs.open(DATA_DIR+'SCCL-classics-ebook-candidates.tsv','w',encoding='utf-8') as output:
        count = 0
        for line in codecs.open(DATA_DIR+'SCCL-classics-edition-author-work.tsv', encoding='utf-8'):
            count += 1
            if count == 1:
                continue # skip header line
            title,author,work_title = line.rstrip('\n').split('\t')
            title = title.split(':')[0].strip() # main title only
            docs = search_open_library(author, title, 'eng')
            print '\n%d OpenLibrary works found for %s by %s:' % (len(docs),title,author)
            
            if work_title and work_title != title:
                before = len(docs)
                docs = merge(docs,search_open_library(author, title, 'eng'))
                added = len(docs) - before
                if added:
                    print 'Added %d new search results' % added
 
            if not docs:
                # Output a blank record so we know it got no matches
                print 'No matches for %s by %s' % (title, author)
                output.write('\t'.join([title, author])+'\n')
            for doc in docs:
                key = doc['key']
                #if not 'public_scan_b' in doc or doc['public_scan_b']: # not reliable
                if True or check_language('eng',doc):
                    nonIA = 0
                    for e in all_editions(doc):
                        edition = get_ia_edition(e)
                        if edition and 'ocaid' in edition:
                            ia = edition['ocaid']
                            key = edition['key']
                            date = edition['publish_date'] if 'publish_date' in edition else ''
                            if publicdomain(date):
                                #print date
                                files = get_files(ia)
                                # ePub is not listed typically
                                #epub_url = find_file(files,'epub')
                                abbyy_url = find_file(files,'_abbyy.gz')
                                marc_url = find_file(files,'_meta.mrc')
                                if abbyy_url and marc_url:
                                    response = get_file(ia,'_meta.mrc',body=True)
                                    if response and response.status_code == 200:
                                        try:
                                            marcfile = pymarc.MARCReader(response.content)
                                            marc = marcfile.next()
                                            field = marc['008'].value()
                                            yr = field[7:11]
                                            lang = field[35:38]
                                            print yr, lang
                                            ol_edition_url = 'http://openlibrary.org' + key
                                            #print '\t'.join([ date, ol_edition_url, epub_url])
                                            if lang == 'eng':
                                                # We have a winner! - write it to our output file.
                                                output.write('\t'.join([title, author, date, ol_edition_url])+'\n')
                                            else:
                                                print 'Skipping lang: ',lang
                                        except:
                                            print 'Failed to parse MARC record ',marc_url
                                    else:
                                        print 'No MARC record ',ia,key
                                else:
                                    print 'Skipping ',ia,key,abbyy_url,marc_url
                        else:
                            nonIA += 1
                            print 'OL edition record unexpectedly missing "ocaid" key',edition
                    # print 'Editions with no IA equiv = %d' % nonIA

    print count,title,author

main()

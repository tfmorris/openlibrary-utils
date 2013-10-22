'''
Given a list of candidate public domain book titles and authors attempt to find the best matching
OpenLibrary records with full text available on Internet Archive.
'''

import codecs
import requests
import requests_cache
from time import clock,sleep

RATE = 2.0

requests_cache.install_cache('openlibrary')

count = 0

def search_open_library(author,title):
    title = title.split(':')[0].strip() # main title only
    author = author.strip()
    payload = {'has_fulltext' : 'true', 'title': title}
    if author:
        payload.update({'author': author})
    response = requests.get('http://openlibrary.org/search.json',params=payload)
    if response.status_code == 200:
        docs = response.json()['docs']
        print '\n%d OpenLibrary works found for %s by %s:' % (len(docs),title,author)
        return docs
    else:
        print 'Request failed',response.status_code,payload
        return []

def check_language(lang,doc):
    return not 'language' in doc or not doc['language'] or lang in doc['language']

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

def get_ia_edition(iaid):
    '''Get JSON for an edition using its IA identifier.  Follows non-HTTP OpenLibrary redirect records '''
    edition_url = 'http://openlibrary.org/books/ia:%s.json' % iaid
    edition = requests.get(edition_url).json()
    if edition['type']['key'] == '/type/redirect':
        edition_url =  'http://openlibrary.org%s.json' % edition['location']
        edition = requests.get(edition_url).json()
    return edition

def test_file_availability(iaid,suffix):
    '''
    Test whether a file in the given format is available for an Internet Archive ID.
    Follows redirects if necessary.
    
    If the "suffix" parameter doesnt contain a period, one will be prepended.
    This allows both "epub" and "_files.xml" style suffixes.
    '''
    #files = requests.get('http://archive.org/download/%s/%s_files.xml' % (iaid,iaid))
    if suffix.find('.') < 0:
        suffix = '.'+suffix
    epub_url = 'http://archive.org/download/%s/%s%s' % (iaid,iaid,suffix)
    epub = requests.head(epub_url)
    if epub.status_code == 302:
        #print 'Redirecting to ',epub.headers['location']
        epub = requests.head(epub.headers['location'])
    if epub.status_code == 200:
        return epub_url
    elif epub.status_code != 403:
        print 'HTTP error (%d) fetching %s for %s :from %s ',(epub.status_code,suffix,iaid,epub_url)
    return None

def rate_wait(time):
    last = now()
    while True:
        yield()
    
    
def main():
    with codecs.open('../data/SCCL-classics-candidates.tsv','w',encoding='utf-8') as output:
        for line in codecs.open('../data/SCCL-classics.tsv', encoding='utf8'):
            title,author = line.rstrip('\n').split('\t')
            docs = search_open_library(author, title)
            for doc in docs:
                key = doc['key']
                #if not 'public_scan_b' in doc or doc['public_scan_b']: # not reliable
                if check_language('eng',doc):
                    nonIA = 0
                    for e in all_editions(doc):
                        last = clock()
                        edition = get_ia_edition(e)
                        if 'ocaid' in edition:
                            ia = edition['ocaid']
                            key = edition['key']
                            epub_url = test_file_availability(ia,'epub')
                            if epub_url:
                                ol_edition_url = 'http://openlibrary.org/books/%s' % key
                                print '\t'.join([ ol_edition_url, epub_url])
                                output.write('\t'.join([title, author, ol_edition_url, epub_url])+'\n')
                        else:
                            nonIA += 1
                            print 'OL edition record unexpectedly missing "ocaid" key',edition
                        elapsed = last-clock()
                        last = clock()
                        if elapsed > (1.0/RATE):
                            sleep((1.0/RATE)-elapsed)
                    # print 'Editions with no IA equiv = %d' % nonIA
                else:
                    print 'Non-English version ',key,doc['language']

    print count,title,author

main()

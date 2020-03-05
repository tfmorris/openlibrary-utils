'''
Prototype to tally ABBY OCR stats as recorded in XML file including:
- character confidence
- in dictionary word percentage
- average word penalty
'''
from __future__ import print_function

from datetime import datetime
import glob
import gzip
import lxml.etree as ET
import sys

DEBUG = False
HEADER = [b'<?xml version="1.0" encoding="UTF-8"?>',
#    '<document version="1.0" producer="LuraDocument XML Exporter for ABBYY FineReader" pagesCount="1"',
#    'xmlns="http://www.abbyy.com/FineReader_xml/FineReader6-schema-v1.xml" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.abbyy.com/FineReader_xml/FineReader6-schema-v1.xml http://www.abbyy.com/FineReader_xml/FineReader6-schema-v1.xml">',
]

CACHE_DIR = '../cache/'


def processfile(filename):
    print('Opening %s' % filename)
    pagenum = 0
    xmllinenum = 0
    wordcount = 0
    susp_count = 0
    charcount = 0
    dic_count = 0
    penalty_tot = 0
    penalty_count = 0
    conf_tot = 0
    with gzip.open(filename, 'rb') as f:
        try:
            for line in f:
                xmllinenum += 1
                if line.startswith(b'<page'):
                    pagenum += 1
                    xml = list(HEADER)
                    while not line.startswith(b'</page'):
                        xml.append(line)
                        xmllinenum += 1
                        line = next(f)
                    xml.append(line)
                #        xml.append('</document>')

                    dom = ET.fromstring(b'\n'.join(xml))
                    chars = dom.findall('.//charParams')
                    for c in chars:
                        if c.text == ' ':
                            continue
                        conf = int(c.get('charConfidence', 255))
                        conf_tot = conf_tot + conf
                        charcount += 1
                        susp = c.get('suspicious', False)
                        if susp:
                            susp_count += 1
                        start = c.get('wordStart', False)

                        # Word attributes - these should stay constant for whole word
                        dic = c.get('wordFromDictionary', False)
                        penalty = c.get('wordPenalty', False)
                        normal = c.get('wordNormal', False)
                        if start == 'true':
                            wordcount += 1
                            wdic = dic
                            wpenalty = penalty
                            wnormal = normal
                            if dic == 'true':
                                dic_count += 1
                            if penalty and penalty != '0':
                                penalty_tot += int(penalty)
                                penalty_count += 1
                        elif DEBUG:
                            if dic != wdic:
                                print('Warning dictionary flag not consistent for entire word')
                            if penalty != wpenalty:
                                print('Warning penalty not consistent for entire word')
                                print(ET.tostring(c))
                            if normal != wnormal:
                                print('Warning normal flag not consistent for entire word')

            avg_word_penalty = 0
            avg_word_chars = 0
            pct_in_dict = 0
            penalty_pct = 0
            if wordcount:
                avg_word_penalty = penalty_tot * 1.0 / wordcount
                pct_in_dict = dic_count * 100.0 / wordcount
                avg_word_chars = charcount * 1.0 / wordcount
                penalty_pct = penalty_count * 100.0 / wordcount
            avg_char_confidence = 0
            if charcount:
                avg_char_confidence = conf_tot * 1.0 / charcount
            print("%d pages, %d words, %5.2f%% in dict, %5.2f%% penalties, %5.2f char/word, %5.2f avg word penalty, %5.2f char conf" %
                  (pagenum, wordcount, pct_in_dict, penalty_pct, avg_word_chars, avg_word_penalty, avg_char_confidence))

        except IOError as e:
            print('Error decoding ', filename, e)


def main(argv):
    if argv:
        files = [argv[1]]
    else:
        files = glob.glob(CACHE_DIR+'*_abbyy.gz')
        print('Found %d files' % len(files))
    start = datetime.now()
    for f in files:
        processfile(f)
    print('Processed %d files in %s' % (len(files), str(datetime.now() - start)))

    
if __name__ == '__main__':
    main(sys.argv)

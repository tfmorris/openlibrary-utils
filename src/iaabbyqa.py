'''
Prototype to tally ABBY OCR stats as recorded in XML file including:
- character confidence
- in dictionary word percentage
- average word penalty
'''
import gzip
import lxml.etree as ET
from xml.etree import cElementTree
import zlib

HEADER = ['<?xml version="1.0" encoding="UTF-8"?>',
#    '<document version="1.0" producer="LuraDocument XML Exporter for ABBYY FineReader" pagesCount="1"',
#    'xmlns="http://www.abbyy.com/FineReader_xml/FineReader6-schema-v1.xml" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.abbyy.com/FineReader_xml/FineReader6-schema-v1.xml http://www.abbyy.com/FineReader_xml/FineReader6-schema-v1.xml">',
    ]
FILENAME = '../pilgrimsprogr1796buny_abbyy.gz'



def processfile(filename):
    localfile =filename

    print 'Opening %s' % localfile
    pagenum = 0
    linenum = 0
    wordcount = 0
    confidence = 0
    susp_count = 0
    charcount = 0
    dic_count = 0
    penalty_tot = 0
    penalty_count = 0
    conf_tot = 0
    with gzip.open(localfile, 'rb') as f:
        for line in f:
                linenum += 1
                if line.startswith('<page'):
                    pagenum += 1
                    xml = list(HEADER)
                    while not line.startswith('</page'):
                        xml.append(line)
                        linenum += 1
                        line = f.next()
                    xml.append(line)
            #        xml.append('</document>')
                    

                    dom = ET.fromstring('\n'.join(xml))
                    chars = dom.findall('.//charParams')
                    for c in chars:
                        s = ''
                        conf = int(c.attrib['charConfidence'])
                        if conf<255: # spaces have a confidence of 255 and can be skipped
                            conf_tot = conf_tot + conf
                            charcount += 1
                        if 'suspicious' in c.attrib:
                            susp = c.attrib['suspicious']
                            susp_count += 1
                        start = c.attrib['wordStart']

                        # Word attributes - these should stay constant for whole word
                        dic = c.attrib['wordFromDictionary']
                        penalty = c.attrib['wordPenalty']
                        normal = c.attrib['wordNormal']
                        if start == 'true':
                            wordcount += 1
                            wdic = dic
                            if dic == 'true':
                                dic_count += 1
                            wpenalty = penalty
                            if penalty and penalty != '0':
                                penalty_tot += int(penalty)
                                penalty_count += 1
                            wnormal = normal
                            #print ''
                        elif penalty != '0':
                            assert wdic == dic
                            assert wpenalty == penalty
                            assert wnormal == normal
                            #print '\t'.join([str(conf),susp,dic,penalty,c.text])
        avg_word_penalty = 0
        pct_in_dict = 0
        if wordcount:
            avg_word_penalty = penalty_tot*1.0/wordcount
            pct_in_dict = dic_count * 100.0 / wordcount
        avg_char_confidence = 0
        if charcount:
            avg_char_confidence = conf_tot * 1.0 / charcount
        print "%d pages, %d words, %5.2f%% in dict, %5.2f%% penalties, %5.2f char/word, %5.2f avg word penalty, %5.2f char conf"% (pagenum,wordcount,pct_in_dict, penalty_count*100.0/wordcount,charcount*1.0/wordcount, avg_word_penalty, avg_char_confidence)

def main():
    processfile(FILENAME)

main()

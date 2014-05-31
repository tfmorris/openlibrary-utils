'''
Program to take a list of bitly short URLs and get monthly stats for them

@author: Tom Morris <tfmorris@gmail.com>
'''
import datetime
import requests
import time

bitly = file('../bitly_credentials.txt').readlines()

BITLY_LOGIN = bitly[0].rstrip('\n').strip()
BITLY_API_KEY = bitly[1].rstrip('\n').strip()
BITLY_ACCESS_TOKEN = bitly[2].rstrip('\n').strip()
FILE = '../data/SCCL-classic-eBooks-URLs-all.tsv'
MONTHS = 6
BITLY_TEMPLATE = 'https://api-ssl.bit.ly/v3/link/clicks?access_token=%s&link=%s&unit=month&units=%d&rollup=false'

def get_stats(short_url):
    url = BITLY_TEMPLATE % (BITLY_ACCESS_TOKEN, short_url, MONTHS)
    response = requests.get(url)
    json = response.json()
    status_code = json['status_code']
    if status_code == 200:
        data = json['data']
        today = datetime.datetime.fromtimestamp(data['unit_reference_ts'])
        return [[datetime.datetime.fromtimestamp(month['dt']),month['clicks']] for month in data['link_clicks']]
    else:
        print 'Request failed ',url,response

def format_stats(stats):
    return '\t'.join([str(m[1]) for m in stats])

def main():
    print 'SCCL eBooks Bitly clicks for %d months ending on %s' % (MONTHS,time.strftime("%m/%d/%Y") )
    # TODO: Get column headers
    with file(FILE) as url_list:
        url_list.readline() # discard header
        ol_total = epub_total = amazon_total = 0
        for line in url_list:
            urls = line.rstrip('\n').split('\t')
            owned = 'Y' if urls[0] else 'N'
            ol_edition = urls[1]
            ol_bitly = urls[2]
            epub_bitly = urls[4]
            amazon_bitly = urls[6]
            ol_stats = get_stats(ol_bitly)
            ol_total += sum([m[1] for m in ol_stats])
            epub_stats = get_stats(epub_bitly)
            epub_total += sum([m[1] for m in epub_stats])
            amazon_stats = get_stats(amazon_bitly)
            amazon_total += sum([m[1] for m in amazon_stats])

            total = sum([m[1] for m in ol_stats]) + sum([m[1] for m in epub_stats]) +  sum([m[1] for m in amazon_stats])

            print '\t'.join((owned, ol_edition,format_stats(ol_stats),format_stats(epub_stats),format_stats(amazon_stats),str(total)))

if __name__ == '__main__':
    main()
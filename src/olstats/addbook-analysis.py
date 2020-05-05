# -*- coding: utf-8 -*-
"""
Use the OpenLibrary recentchanges API to plot new book additions.

Created on 2017-04-04

@author: Tom Morris <tfmorris@gmail.com>
@copyright 2015,2017 Thomas F. Morris
"""

import datetime
import matplotlib.pyplot as plt
import requests
import requests_cache

requests_cache.install_cache('openlibrary_cache')

BASE='https://openlibrary.org'
CHANGES = BASE + '/recentchanges/%04d/%02d/%02d/'
LIMIT=1000
MAX= 10000 # 10000
CLIP=10000 # don't plot anything above this value
START = datetime.datetime(2010, 8, 1)
END = datetime.datetime.today() + datetime.timedelta(days = 1)
counts = []
dates = []
outliers = []

date = START
while date < END:
    count = 0
    kind = 'add-book'
    url = (CHANGES % (date.year, date.month, date.day)) + kind + '.json'
    for offset in range(0, MAX+1, LIMIT):
        params = {'offset': offset, 'limit' : LIMIT}
        # TODO: In production, don't use cache for recentchange list
        response = requests.get(url, params = params)
        if not response.ok:
            print('Failed to fetch url %d %s' % (response.status_code, url))
            break
        books = response.json()
        count += len(books)
        if len(books) < LIMIT:
            break
            
    #print(date,count)
    dates.append(date)
    counts.append(min(count, CLIP))
#    counts.append(count)
    if count > CLIP:
        print(date, count)
        outliers.append((len(dates), count))
    
    #print('%s\t%d' % (date.isoformat(), count))
    date += datetime.timedelta(days = 1)

fig = plt.figure()
ax = fig.add_subplot(111)

ax.plot(dates, counts)
plt.ylabel('OpenLibrary new books / day')
#ax.annotate('Outlier = %d' % outliers[0][1], xy=(outliers[0][0],200), xytext=(10,10),
#            arrowprops=dict(facecolor='black', shrink=0.05))
fig.savefig('openlibrary-books.svg',format='svg')
plt.show()

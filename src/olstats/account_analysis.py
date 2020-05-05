# -*- coding: utf-8 -*-
"""
Use the OpenLibrary recentchanges API to get stats on new account creation.

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
START = datetime.datetime(2009, 3, 15)
END = datetime.datetime.today() + datetime.timedelta(days = 1)
CUTOFF = datetime.datetime(2011, 6, 9)
CLIP = 2700
counts = []
dates = []
outliers = []

date = START
while date < END:
    count = 0
    # Before 2011 it's 'create' then 'register' not 'new-account'
    if date < CUTOFF:
        kind = 'register'
    else:
        kind = 'new-account'
    url = (CHANGES % (date.year, date.month, date.day)) + kind + '.json'
    for offset in range(0, MAX-LIMIT, LIMIT):
        params = {'offset': offset, 'limit' : LIMIT}
        # TODO: In production, don't use cache for recentchange list
        response = requests.get(url, params = params)
        if not response.ok:
            print('Failed to fetch url %d %s' % (response.status_code, url))
            break
        accounts = response.json()
        count += len(accounts)
        if len(accounts) < LIMIT: # short read means we're done
            break
    dates.append(date)
    counts.append(min(count, CLIP))
    if count > CLIP:
        print(date, count)
        outliers.append((len(dates), count))
    
    #print('%s\t%d' % (date.isoformat(), count))
    date += datetime.timedelta(days = 1)

fig = plt.figure()
ax = fig.add_subplot(111)

ax.plot(dates, counts)
plt.ylabel('New OpenLibrary accounts / day')
#ax.annotate('Outlier = %d' % outliers[0][1], xy=(outliers[0][0],200), xytext=(10,10),
#            arrowprops=dict(facecolor='black', shrink=0.05))
fig.savefig('openlibrary-accounts.svg',format='svg')
plt.show()

# -*- coding: utf-8 -*-
"""
Use the OpenLibrary spam reversion history to create a list of spammers.
The revert history begins 2011-04-20, so earlier spam fighting was done with
individual edits.

Created on 2015-09-02

@author: Tom Morris <tfmorris@gmail.com>
@copyright 2015 Thomas F. Morris
"""

from __future__ import print_function
import requests
import requests_cache
from collections import Counter

requests_cache.install_cache('openlibrary_cache')

BASE='https://openlibrary.org'
CHANGES = BASE + '/recentchanges'
LIMIT=1000
 # As of Nov 2015, just over 7000 records
 # As of Nov 2018, 10K records goes back as far as 2014, so algorithm needs to be reworked
MAX= 10000
FETCH_CHANGESETS = True # True to fetch reverted change sets
CHANGESET_SAMPLE = 1 # Select one of every SAMPLE records
CHANGESETS_MAX = 10 # Only fetch first N changes of revert (ie last N to be reverted)
FETCH_CHANGES = True

total_revert_count = 0
sampled_revert_count = 0
changeset_count = 0
changes_count = 0
kinds = Counter() # kinds of changesets
types = Counter() # types of changed documents (book, work, etc)
for offset in range(0,MAX-LIMIT+1,LIMIT):
    print('=============== %d ==================' % offset)
    url = CHANGES + '/revert.json'
    params = {'offset': offset, 'limit' : LIMIT}
    # TODO: In production, don't use cache for recentchange list
    response = requests.get(url, params = params)
    if not response.ok:
        print('Failed to fetch url %d %s' % (response.status_code, url))
        continue
    reversions = response.json()
    
    # process data
    for reversion in reversions:
        total_revert_count += 1
        # Sample to get better temporal coverage
        if total_revert_count % CHANGESET_SAMPLE != 0:
            continue
        sampled_revert_count += 1
        changeset_count += len(reversion['data']['reverted_changesets'])
        changelen = len(reversion['changes'])
        key = reversion['changes'][max(-3,-1*changelen)]['key']
        print('\t'.join((key,str(changelen),reversion['timestamp'])))
    # Assume a short read means that we're done
    if len(reversions) < LIMIT:
        break
        
print('Processed %d of %d reversions (%2.1f%%) with %d changesets & %d changes (capped at %d per reversion)' % (sampled_revert_count, total_revert_count, (sampled_revert_count * 100.0) / total_revert_count, changeset_count, changes_count, CHANGESETS_MAX))
print('Changeset kinds:')
for (k, n) in kinds.most_common():
    print(k, n)
print('Document types:')
for (t, n) in types.most_common():
    print(t, n)

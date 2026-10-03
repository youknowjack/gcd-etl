from redash_toolbelt.client import Redash
from pprint import pprint
import re
import sys

def gen_datestr(cur, freq):
    delta = int(1200/freq)
    d = [ cur ]
    day = cur - delta
    while day > 100:
        d.append(day)
        day = day - delta
    day = cur + delta
    while day < 1300:
        d.append(day)
        day = day + delta
    d.sort()
    s = [str(i) for i in d]
    return ', '.join(s)

if __name__ == '__main__':
    redash_url = 'https://redash.gcdata.org'
    api_key = 'hAkGsu4QNsYCua5tQRvcHfKqgmdgryNwaI2RNJ31'
    redash = Redash(redash_url, api_key)
    query_id = sys.argv[3]
    data=redash.get_query(query_id)
    query=data['query']
    dates = gen_datestr(int(sys.argv[1]), int(sys.argv[2]))
    updated = re.sub("10000 IN.*", "10000 IN (%s)" % dates, query)
    data['query'] = updated
    print(data['query'])
    redash.update_query(query_id, data)

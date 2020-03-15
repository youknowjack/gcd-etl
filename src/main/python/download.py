from bs4 import BeautifulSoup
import logging
import re
import requests
import sys

HOME_URL = 'https://www.comics.org'
LOGIN_URL = 'https://www.comics.org/accounts/login/'
DOWNLOAD_URL = 'https://www.comics.org/download/'
CSRF_NAME = 'csrfmiddlewaretoken'
USER_AGENT = 'https://github.com/youknowjack/gcd-etl'

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)
requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(logging.DEBUG)
requests_log.propagate = True

with open(sys.argv[1], "r") as f:
    lines = f.readlines()
    username = lines[0].strip()
    password = lines[1].strip()

with requests.Session() as session:
    headers = {
        'User-Agent': USER_AGENT,
    }
    with session.get(HOME_URL, headers=headers) as resp:
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')
        csrf_token = soup.find('input', attrs={'name': CSRF_NAME})['value']
    headers = {
        'Referer': HOME_URL,
        'User-Agent': USER_AGENT,
    }
    attrs = {
        CSRF_NAME: csrf_token,
        'username': username,
        'password': password,
    }
    with session.post(LOGIN_URL, attrs, headers=headers) as resp:
        resp.raise_for_status()

    headers = {
        'Referer': HOME_URL,
        'User-Agent': USER_AGENT,
    }
    with session.get(DOWNLOAD_URL, headers=headers) as resp:
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')
        csrf_token = soup.find('input', attrs={'name': CSRF_NAME})['value']
        timestamp = soup.find(text=re.compile('MySQL:')).nextSibling.text
        print(timestamp)

    filename = 'gcd-dump-%s.zip' % timestamp.replace(' ', '_')
    headers = {
        'Referer': DOWNLOAD_URL,
        'User-Agent': USER_AGENT,
    }
    attrs = {
        CSRF_NAME: csrf_token,
        'purpose': 'non-commercial',
        'usage': '',
        'accept_license': '1',
        'mysqldump': 'Download MySQL Dump',
    }
    with session.post(DOWNLOAD_URL, attrs, headers=headers) as resp:
        resp.raise_for_status()
        if resp.headers['Content-Type'].startswith('text/html'):
            soup = BeautifulSoup(resp.text, 'html.parser')
            print(soup.find(class_='body_content').text.replace('\n', ' ').strip())
        else:
            with open(filename, 'wb') as f:
                for chunk in (resp.iter_content(chunk_size=(5000*1024))):
                    if chunk:
                        f.write(chunk)
            print("Downloaded %s" % filename)



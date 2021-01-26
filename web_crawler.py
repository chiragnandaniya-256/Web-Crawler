from pymongo import MongoClient
from cfg import config

from datetime import datetime
from datetime import timedelta
import time
import random
import string
import os

import requests
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
import warnings
warnings.filterwarnings("ignore")

##Created a mongod instance at port 3000 and connected to it
client = MongoClient('localhost', 3000)
client.list_database_names()
##Created a new database for storing links
db = client["CrawlerDB"]

#Root url from cfg.py
url=config['root url']

def insert_root(url):
    '''
    This function manually inserts the root url in the database
    '''

    doc = {
        'Link': url,
        'Source Link': url,
        'isCrawled':False,       #not crawled yet
        'Last Crawled': "Never",
        'Response Status':'' ,
        'Content Type' :'',
        'Content length': '',
        'File Path':"",
        'Date Created': datetime.now()
    }
    db.linkcol.insert_one(doc)

insert_root(url)

def is_valid(url):
    '''
    This function checks if a url is valid or not
    '''
    parsed = urlparse(url)
    return bool(parsed.netloc) and ((parsed.scheme)=='http' or (parsed.scheme)=='https' )

def get_all_links(url):
    '''
    :return: set of all unique urls on the current url
    '''

    new_urls = set()
    try:
        soup = BeautifulSoup(requests.get(url, verify=False).content, "html.parser")
    except requests.exceptions.SSLError:
        return new_urls
    for a_tags in soup.findAll("a"):
        href = a_tags.attrs.get("href")
        if href=="" or href==None:
            continue
        #for relative links
        href = urljoin(url, href)
        parsed_href = urlparse(href)
        href = parsed_href.scheme+"://"+parsed_href.netloc+parsed_href.path
        if not is_valid(href):
            continue
        if href in url:
            continue
        new_urls.add(href)
    return new_urls

def all_crawled():
    '''
    This function check if there are uncrawled links which are
    1. If they are never crawled before or
    2. if they are crawled before 24 hours
    :return: count of all uncrawled links
    '''
    count=0
    for doc in db.linkcol.find({}):
        if doc['Last Crawled']!='Never':
            time_diff = datetime.now()-doc['Last Crawled']
            if time_diff.days>=1:
                count=count+1
        else:
            count=count+1
    return count

def get_all_uncrawled():
    uncrawled_url = set()
    for doc in db.linkcol.find({}):
        if doc['Last Crawled']=='Never':
            uncrawled_url.add(doc['Link'])
        else:
            time_diff = datetime.now()-doc['Last Crawled']
            if time_diff.days>=1:
                uncrawled_url.add(doc['Link'])
    return uncrawled_url

def already_inserted(link):
    '''
    checks if a link is already present in the database
    '''
    if db.linkcol.find_one({'Link':link})==None:
        return False
    return True

def insert_new_links(new_urls, source_url, max_url):
    '''
    Inserts all the new links on a page in database
    source url is the link from which it was first extracted
    '''
    for link in new_urls:
        if(already_inserted(link)):
            continue
        doc = {
            'Link': link,
            'Source Link': source_url,
            'isCrawled':False,          ##Initially the links are not crawled
            'Last Crawled': "Never",
            'Response Status':'' ,
            'Content Type' :'',
            'Content length': '',
            'File Path':"",
            'Date Created': datetime.now()
        }
        if max_url<=db.linkcol.count():
            break
        db.linkcol.insert_one(doc)
        print(link+" inserted at "+str(db.linkcol.count()))


def crawl(max_url=config['max_url']):
    #if all the links are crawled then prints the appropriate msg
    if (all_crawled() == 0):
        print("All Links Crawled")
    else:
        uncrawled_urls = get_all_uncrawled()
        for url in uncrawled_urls:
            try:
                extension = (requests.get(url).headers['Content-Type'].split('/')[-1].split(';')[0])
                letters = string.ascii_lowercase
                result_str = ''.join(random.choice(letters) for i in range(10))
                #filename with the extension
                file_name = (result_str + '.' + extension)
                content = requests.get(url).text

                with open(file_name, 'wb') as f:
                    f.write(content.encode())

                file_path = os.path.join(os.getcwd(), file_name)

                db.linkcol.update_one({'Link': url}, {'$set':
                                                          {'isCrawled': True, 'Last Crawled': datetime.now(),
                                                           'Response Status': requests.get(url).status_code,
                                                           'Content Type': requests.get(url).headers['Content-Type'],
                                                           'Content length': len(requests.get(url).content),
                                                           'File Path': file_path}})
                if (max_url <= db.linkcol.count()):
                    print("Maximum Limit Reached")
                    continue

                new_links = get_all_links(url)
                insert_new_links(new_links, url, max_url)

            except requests.exceptions.SSLError:
                continue
            except requests.exceptions.ConnectionError:
                continue
            except requests.exceptions.Timeout:
                continue
            except requests.exceptions.HTTPError:
                continue

            except requests.exceptions.MissingSchema:
                continue

    time.sleep(5)
    crawl(max_url)

crawl()

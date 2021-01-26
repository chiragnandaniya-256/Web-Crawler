### python_web_crawler
This is the code that continuously runs in the background and recursively scrapes all 
links it can find.

It goes on root url, get all the links, insert them into database and then crawl them.

###Run locally
- Install dependencies

```bash
pip3 install -r requirements.txt
```
- To start the scraping process

```bash
python3 web_crawler.py
```


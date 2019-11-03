import argparse
from pprint import pprint
from traceback import format_exc

import requests
import multiprocess
import csv
from lxml import html


HEADERS = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36'}
def request(url, tracer, retries=5):
  for _ in range(5):
    print("Retrieving %s"%(url))
    response = requests.get(url, headers=HEADERS)
    if response.status_code == 200:
      return (response.text, tracer)
    else:
      print(response)
  raise Exception("failed")


NUM_WORKERS = 32
MAX_PAGES = 1000
def scrape(query):
  fname = "out.csv"
  with open(fname, "wt") as f:
    f.write("")
  with open(fname, "at") as f:
    c = csv.writer(f)
    page = 0
    mp = multiprocess.Batcher(num_workers=NUM_WORKERS)
    while page < MAX_PAGES:
      url = 'http://www.ebay.com/sch/i.html?_nkw={0}&_sacat=0&_pgn={1}'.format(query, page + 1)
      mp.enqueue(request, url, page)
      page += 1
      if (page % NUM_WORKERS == NUM_WORKERS - 1):
        responses = mp.process()
        responses = sorted(responses, key=lambda r: r[1])
        should_break = False
        for response_text, resp_page in responses:
          print("page {}".format(resp_page))
          parser = html.fromstring(response_text)
          product_listings = parser.xpath('//li[contains(@id,"results-listing")]')
          raw_result_count = parser.xpath("//h1[contains(@class,'count-heading')]//text()")
          found = False
          for product in product_listings:
            raw_url = product.xpath('.//a[contains(@class,"item__link")]/@href')
            raw_title = product.xpath('.//h3[contains(@class,"item__title")]//text()')
            raw_product_type = product.xpath('.//h3[contains(@class,"item__title")]/span[@class="LIGHT_HIGHLIGHT"]/text()')
            raw_price = product.xpath('.//span[contains(@class,"s-item__price")]//text()')
            price  = ' '.join(' '.join(raw_price).split())
            title = ' '.join(' '.join(raw_title).split())
            product_type = ''.join(raw_product_type)
            title = title.replace(product_type, '').strip()
            row = [raw_url[0], title, price]
            c.writerows([row])
            print(row)
            found = True
          if not found:
            print("No more items found, page {}".format(resp_page))
            should_break = True
            break
        if should_break: break


if __name__=="__main__":

  argparser = argparse.ArgumentParser()
  argparser.add_argument('query',help = 'search query')
  args = argparser.parse_args()
  query = args.query
  scrape(query)

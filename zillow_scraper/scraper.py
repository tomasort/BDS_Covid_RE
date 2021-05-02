import json, asyncio, time, socket, datetime, time
import threading

import pandas as pd
from random import randint, choice
from bs4 import BeautifulSoup
from urllib3.exceptions import MaxRetryError
from urllib3 import ProxyManager, PoolManager, disable_warnings, exceptions
from collections import Counter
from scripts.bds_real_estate_scraper.proxies import Proxies
from scripts.bds_real_estate_scraper.zillow_query import ZillowQuery

# I didn't like the data that we got.
# Trying to find something better.
class Scraper:
    """Class for scraping real estate data from zillow.com"""
    def __init__(self, zip_codes, min_price=0, max_price=20_000_000, increment=150_000, max_tries=20, use_cookies=False):
        self.domain = "www.zillow.com"
        self.zip_codes = set(zip_codes)
        self.empty_pages = set()
        self.max_price = max_price
        self.min_price = min_price
        self.rent_max_price = 13_000
        self.rent_min_price = 0
        self.rent_increment = 1_500
        PROXY_HOST = '83.149.70.159:13012'
        PROXY_PORT = '13012'
        proxy = "http://83.149.70.159:13012"
        self.http = ProxyManager(proxy)
        # if proxy:
        # else:
        #     disable_warnings(exceptions.InsecureRequestWarning)
        #     self.http = PoolManager()
        self.visited_sites = set()
        self.max_tries = max_tries
        self.increment = increment
        self.max_retries = 20
        self.failed_urls = Counter()
        self.use_cookies = use_cookies
        self.cookies = set()
        self.proxy = Proxies()
        self.fetches = 0
        self.zip_codes_df = pd.read_csv("data/zip_codes.csv")

    def get_headers(self):
        headers = self.proxy.get_header()
        if self.use_cookies:
            headers['Cookie'] = "".join(self.cookies.pop())
        return headers

    def is_captcha(self, html_soup):
        captcha_container = html_soup.select(".captcha-container")
        if captcha_container:
            return True
        if html_soup.title is None:
            return True
        return False

    def parse_cookie(self, response):
        # TODO: implement the parse_cookie method to be able to send cookies. Get the set cookie from response and add it to the set self.cookies
        pass

    def get_parsed_query_string(self, soup):
        query_state = soup.find('script', {'data-zrr-shared-data-key': 'mobileSearchPageStore'})
        assert query_state
        query_state = query_state.contents[0].replace("<!--", "").replace("-->", "")
        parsed_query_state = json.loads(query_state)
        return parsed_query_state

    def save_properties(self, zip_code, properties=None):
        if properties is None or properties == []:
            return
        for prop in properties:
            if prop:
                date_sold, price_change_date = None, None
                if 'hdpData' in prop.keys() and 'dateSold' in prop['hdpData']['homeInfo'].keys() and prop['hdpData']['homeInfo']['dateSold'] is not None:
                    date_sold = datetime.datetime.utcfromtimestamp(prop['hdpData']['homeInfo']['dateSold'] / 1000)
                    if prop['hdpData']['homeInfo']['dateSold'] == 0:
                        date_sold = None
                if 'hdpData' in prop.keys() and 'datePriceChanged' in prop['hdpData']['homeInfo'].keys() and prop['hdpData']['homeInfo'][
                    'datePriceChanged'] is not None:
                    price_change_date = datetime.datetime.utcfromtimestamp(
                        prop['hdpData']['homeInfo']['datePriceChanged'] / 1000)
                    if prop['hdpData']['homeInfo']['datePriceChanged'] == 0:
                        price_change_date = None
                values = (prop['id'],
                          prop['soldPrice'] if 'soldPrice' in prop.keys() else None,
                          prop['hdpData']['homeInfo']['price'] if 'hdpData' in prop.keys() and 'price' in prop['hdpData']['homeInfo'].keys() else None,
                          prop['price'] if 'price' in prop.keys() else None,
                          prop['unformattedPrice'] if 'unformattedPrice' in prop.keys() else None,
                          prop['addressCity'] if 'addressCity' in prop.keys() else None,
                          prop['addressState'] if 'addressState' in prop.keys() else None,
                          # there are two zip codes for some reason. they shoud be the same
                          prop['addressZipcode'] if 'addressZipcode' in prop.keys() else None,
                          prop['hdpData']['homeInfo']['zipcode'] if 'hdpData' in prop.keys() and 'zipcode' in prop['hdpData']['homeInfo'].keys() else None,
                          prop['area'] if 'area' in prop.keys() else None,
                          prop['pricePerSqft'] if 'pricePerSqft' in prop.keys() else None,
                          prop['statusType'] if 'statusType' in prop.keys() else None,
                          prop['statusText'] if 'statusText' in prop.keys() else None,
                          prop['beds'] if 'beds' in prop.keys() else None,
                          prop['baths'] if 'baths' in prop.keys() else None,
                          prop['hdpData']['homeInfo']['homeStatus'] if 'hdpData' in prop.keys() and 'homeStatus' in prop['hdpData']['homeInfo'].keys() else None,
                          prop['hdpData']['homeInfo']['city'] if 'hdpData' in prop.keys() and 'city' in prop['hdpData']['homeInfo'].keys() else None,
                          prop['latLong']['latitude'] if 'latLong' in prop.keys() and prop['latLong'] else None,
                          prop['latLong']['longitude'] if 'latLong' in prop.keys() and prop['latLong'] else None,
                          prop['hdpData']['homeInfo']['lotSize'] if 'hdpData' in prop.keys() and 'lotSize' in prop['hdpData']['homeInfo'].keys() else None,
                          prop['address'] if 'address' in prop.keys() else None,
                          prop['detailUrl'] if 'detailUrl' in prop.keys() else None,
                          prop['hdpData']['homeInfo']['zestimate'] if 'hdpData' in prop.keys() and 'zestimate' in prop['hdpData']['homeInfo'].keys() else None,
                          prop['hdpData']['homeInfo']['timeOnZillow'] if 'hdpData' in prop.keys() and 'timeOnZillow' in prop['hdpData']['homeInfo'].keys() else None,
                          prop['hdpData']['homeInfo']['priceChange'] if 'hdpData' in prop.keys() and 'priceChange' in prop['hdpData']['homeInfo'].keys() else None,
                          prop['hdpData']['homeInfo']['priceReduction'] if 'hdpData' in prop.keys() and 'priceReduction' in prop['hdpData']['homeInfo'].keys() else None,
                          prop['hdpData']['homeInfo']['rentZestimate'] if 'hdpData' in prop.keys() and 'rentZestimate' in prop['hdpData']['homeInfo'].keys() else None,
                          price_change_date if 'hdpData' in prop.keys() and 'datePriceChanged' in prop['hdpData']['homeInfo'].keys() else None,
                          prop['hdpData']['homeInfo']['homeType'] if 'hdpData' in prop.keys() and 'homeType' in prop['hdpData']['homeInfo'].keys() else None,
                          prop['hdpData']['homeInfo']['bedrooms'] if 'hdpData' in prop.keys() and 'bedrooms' in prop['hdpData']['homeInfo'].keys() else None,
                          prop['hdpData']['homeInfo']['bathrooms'] if 'hdpData' in prop.keys() and 'bathrooms' in prop['hdpData']['homeInfo'].keys() else None,
                          prop['hdpData']['homeInfo']['yearBuilt'] if 'hdpData' in prop.keys() and 'yearBuilt' in prop['hdpData']['homeInfo'].keys() else None,
                          prop['hdpData']['homeInfo']['daysOnZillow'] if 'hdpData' in prop.keys() and 'daysOnZillow' in prop['hdpData']['homeInfo'].keys() else None,
                          prop['variableData']['type'] if 'variableData' in prop.keys() and prop['variableData'] and 'type' in prop['variableData'].keys() else None,
                          prop['variableData']['text'] if 'variableData' in prop.keys() and prop['variableData'] and 'text' in prop['variableData'].keys() else None,
                          prop['hdpData']['homeInfo']['livingArea'] if 'hdpData' in prop.keys() and 'livingArea' in prop['hdpData']['homeInfo'].keys() else None,
                          prop['hdpData']['homeInfo']['lotAreaValue'] if 'hdpData' in prop.keys() and 'lotAreaValue' in prop['hdpData']['homeInfo'].keys() else None,
                          date_sold if 'hdpData' in prop.keys() and 'dateSold' in prop['hdpData']['homeInfo'].keys() and prop['hdpData']['homeInfo']['dateSold'] is not None else None,
                          prop['hdpData']['homeInfo']['taxAssessedValue'] if 'hdpData' in prop.keys() and 'taxAssessedValue' in prop['hdpData']['homeInfo'].keys() else None)
                with open(f"zip_{zip_code}.csv", "a") as out:
                    out.write(','.join(["\""+str(x)+"\"" for x in values]) + "\n")

    def get_base_url(self, soup, zip_code):
        pagination_tag = soup.find(class_='search-pagination')
        base_one, base_two = None, None
        if pagination_tag:
            base_one = pagination_tag.find('a').get('href').split('/')[1]
            base_one = f"/{base_one}/"
        base_two = "".join(['/',self.zip_codes_df[self.zip_codes_df['Zip_code']==int(zip_code)]['City'].values[0].lower().replace(" ", "-"), '-new-york-ny', f"-{zip_code}", '/'])
        if base_one is None and base_two:
            return base_two
        if base_one and base_two and (base_one != base_two):
            print(f"The bases are different! from pagination:{base_one}, from database: {base_two}")
            return base_one
        return base_one

    def process_page(self, html_soup, url, zip_code):
        print(f"Processing page: {url}")
        try:
            parsed_query_string = self.get_parsed_query_string(html_soup)
            results = parsed_query_string['cat1']['searchResults']['listResults'] if 'cat1' in parsed_query_string.keys() else []
            results.extend(parsed_query_string['cat2']['searchResults']['listResults'] if 'cat2' in parsed_query_string.keys() else [])
            self.save_properties(zip_code, results)
        except AssertionError:
            print("Can't find query state object in response")

    def find_urls(self, zip_code):
        """ Make a list of all the urls we need to scrape to find the most houses in the current zip_code"""
        urls = set()
        zip_url = f"https://www.zillow.com/homes/{zip_code}_rb"
        try:
            main_url_soup = self.fetch(zip_url)  #initial page with the zip code to find the map settings and other info
            # Now parse the query state string from the response
            parsed_query_string = self.get_parsed_query_string(main_url_soup)
            base = self.get_base_url(main_url_soup, zip_code)
            if (self.get_number_of_properties(main_url_soup) == 0):
                return urls
        except AssertionError:
            print("Unable to find the query string! going to try again later!")
            return set()  # Return an empty set
        except MaxTriesError as e:
            print("Error! Something happened")
            print(e)
            return set()  # Return an empty set
        query_state_dict = parsed_query_string['queryState']  # This a dictionary from zillow with a bunch of info
        # Create all the urls using info from the query_state_dict
        for status in [0, 1, 2]:
            current_max_price = self.min_price+self.increment
            max_price = self.max_price
            min_price = self.min_price
            increment = self.increment
            if status == 2:
                current_max_price = self.rent_min_price+self.rent_increment
                max_price = self.rent_max_price
                min_price = self.rent_min_price
                increment = self.rent_increment
            while current_max_price <= max_price:
                query = ZillowQuery(min_price=min_price, max_price=current_max_price, status=status, base=base, **query_state_dict)
                page_one_url = query.get_first_url()
                try:
                    page_one_soup = self.fetch(page_one_url)
                except MaxTriesError as e:
                    print(e)
                    current_max_price += increment
                    continue
                # num_results = self.get_number_of_properties(page_one_soup)
                num_of_pages = self.get_number_of_pages(page_one_soup)
                if self.is_empty(page_one_soup):
                    self.empty_pages.add(page_one_url)
                    min_price = current_max_price
                    current_max_price += increment
                    if current_max_price >= 700_000:
                        increment = increment*4
                    continue
                self.process_page(page_one_soup, page_one_url, zip_code)
                if num_of_pages != 0:
                    query_urls = query.get_urls(first=2, last=num_of_pages)
                    urls.update(query_urls)
                min_price = current_max_price
                current_max_price += increment
                if current_max_price >= 700_000:
                    increment = increment*3
        return urls

    def fetch(self, url):
        soup, tries = None, 1
        while True:
            headers = self.get_headers()
            try:
                source = self.http.request('GET', url, headers=headers, timeout=10)
                self.fetches += 1
                soup = BeautifulSoup(source.data, 'lxml')
                if not self.is_captcha(soup):
                    # The page is valid so we can just return a soup version of the site
                    self.visited_sites.add(url)
                    # Add cookie to the pool of cookies
                    new_cookie = self.parse_cookie(source)
                    self.cookies.add(new_cookie)
                    return soup
                tries += 1
                if tries%5 == 0 and tries >= self.max_tries/2:
                    time.sleep(tries*0.15)
                if tries > self.max_tries:
                    print(f"The upper bound of tries has been reached for url {url}")
                    raise MaxTriesError
            except (MaxTriesError, TimeoutError, MaxRetryError, socket.timeout):
                if self.failed_urls[url] >= self.max_retries:
                    del self.failed_urls[url]
                    print("The URL failed too many times, check the proxy or the internet connection")
                else:
                    self.failed_urls.update([url])
                    print("Will try again later")
                    time.sleep(10)
                return BeautifulSoup("", 'lxml')

    def run(self):
        number_of_threads = 1
        # for zip_code in self.zip_codes:
        #     with open(f"zip_{zip_code}.csv", "w") as out:
        #         out.write("")
        i = 1
        number_of_zip_codes = len(self.zip_codes)
        while self.zip_codes:
            threads = []
            # self.num_of_threads = 1
            for _ in range(number_of_threads):
                if self.zip_codes:
                    try:
                        current_zip_code = self.zip_codes.pop()
                        print(f"{i} out of {number_of_zip_codes}")
                        i += 1
                        t = threading.Thread(target=self.process_zip, args=[current_zip_code])
                        t.start()
                        threads.append(t)
                    except IndexError as e:
                        continue
            for thread in threads:
                thread.join()

    def process_zip(self, zip_code):
        print(f"Processing zip code: {zip_code}")
        urls = self.find_urls(zip_code)
        print(f"Processing Pages in zip code: {zip_code}")
        for url in urls:
            try:
                self.process_page(self.fetch(url), url, zip_code)
            except MaxTriesError:
                continue
        self.handle_failed_pages(zip_code)

    def get_number_of_pages(self, page_soup):
        pagination_div = page_soup.find(class_='search-pagination')
        if pagination_div is None:
            return 0  # There is no pagination section in the page
        try:
            return int(pagination_div.select(".Text-c11n-8-27-0__aiai24-0")[0].text.split(" ")[-1])
        except Exception as e:
            print(e)
            print("Error getting the number of page")
            return 0

    def get_number_of_properties(self, page_soup):
        num = page_soup.find(class_='result-count')
        if num:
            num = num.get_text().replace(',', '').replace('results', '').replace(" ", "")
        try:
            num = int(num)
        except (ValueError, TypeError):
            return 500
        return num

    def is_empty(self, page_soup):
        zero_result_message = page_soup.find(class_='zero-results-message')
        if zero_result_message or page_soup.title is None:
            return True
        return False

    def handle_failed_pages(self, zip_code):
        failed_urls = set(self.failed_urls.keys())
        if not failed_urls:
            return
        while failed_urls:
            try:
                current_url = failed_urls.pop()
                if current_url in self.visited_sites:
                    continue
                if 'home' in current_url:
                    zip_code = current_url.split('/')[-1].replace('_rb', '')
                    failed_urls.update(self.find_urls(zip_code))
                elif '_p' in current_url:
                    try:
                        page_soup = self.fetch(current_url)
                    except MaxTriesError:
                        continue
                    if self.is_empty(page_soup):
                        self.empty_pages.add(current_url)
                        continue
                    self.process_page(page_soup, current_url, zip_code)
                else:
                    try:
                        page_soup = self.fetch(current_url)
                    except MaxTriesError:
                        continue
                    if self.is_empty(page_soup):
                        self.empty_pages.add(current_url)
                        continue
                    self.process_page(page_soup, current_url, zip_code)
                if current_url in self.failed_urls.keys():
                    self.failed_urls.pop(current_url)
            except IndexError:
                break  # No items left


class MaxTriesError(Exception):
    pass


if __name__ == "__main__":
    # Test scraper on one page.
    # asyncio.run(do_something())
    df = pd.read_csv("data/zip_codes_x_covid.csv")

    zip_codes = set(df['Zip_code'].tolist())
    zip_codes = zip_codes.union(
        {10280, 10282, 10301, 10302, 10303, 10304, 10305, 10306, 10307, 10308, 10309, 10310, 10312, 10314, 11355, 11357,
         11418, 11419, 11420, 11421, 11422, 11426, 11427, 11428, 11429, 11432, 11433, 11434, 11435, 11436, 10451, 10452,
         10453, 10455, 10456, 10457, 10458, 10459, 10460, 10461, 10462, 10463, 10464, 10465, 10466, 10467, 10468, 10469,
         10470, 10471, 10472, 10473, 10474, 10475, 11001, 11004, 10001, 10002, 10004, 10007, 10009, 10010, 10011, 10013,
         10014, 10019, 10021, 10023, 10024, 10025, 10026, 10027, 10028, 10029, 10030, 10031, 10032, 10033, 10034, 10035,
         10036, 10037, 10038, 10039, 10040, 10044, 10065, 10075, 11109, 10541})
    s = Scraper(zip_codes)
    s.run()

















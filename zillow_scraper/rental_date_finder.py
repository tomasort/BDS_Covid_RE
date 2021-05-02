from scripts.bds_real_estate_scraper.scraper import *
import pandas as pd
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium import webdriver
import selenium, sys
# from selenium.webdriver.chrome.options import Options
from selenium.webdriver.firefox.options import Options


from scripts.bds_real_estate_scraper.proxies import Proxies
PROXY = "108.59.14.203:13010"


class RentScraper(Scraper):
    def __init__(self):
        super().__init__([], max_tries=1)
        df = pd.read_csv("raw_real_estate_data.csv")
        df = df.drop(
            ['sold_price', 'price', 'price_per_sqrft', 'hdpdata_price', 'beds', 'baths',
             'lot_size', 'year_built', 'address_zip_code'], axis=1)
        df.drop(df[df['zip_code'].isin(
            ['Bay Terrace', 'Homecrest', 'Rochdale', 'North Riverdale', 'Clason Point', 'South Bronx', 'Old Town',
             'Hunters Point', 'Ocean Hill'])].index, inplace=True)
        df.drop(
            df[(df['unformatted_price'] == 'None') | (df['latitude'] == 'None') | (df['longitude'] == 'None')].index,
            inplace=True)
        df = df[df['status_type'] != 'FOR_RENT']
        already_scraped_df = pd.read_csv("sales.csv")
        df['zillow_id'] = df['zillow_id'].astype('int64')
        df = df.sample(100_000)
        df = df[~df['zillow_id'].isin(list(already_scraped_df['zillow_id'].unique()))]
        self.df = df
        print(len(self.df))
        # chrome = webdriver.Chrome(chrome_options=chrome_options)

    def save_record(self, record_numbers, driver):
        for record_number in record_numbers:
            try:
                if self.df.iloc[record_number]['zillow_url'] in self.visited_sites:
                    continue
                soup = self.fetch(self.df.iloc[record_number]['zillow_url'], driver)
                if not soup:
                    continue
                # soup = self.fetch("https://www.hashemian.com/whoami/", driver)
                with open("sales.csv", "a") as out:
                    price_change_history = soup.find_all(class_="hdp__sc-966lqz-4 gLKwXO")
                    for row in price_change_history:
                        row_string = [str(self.df.iloc[record_number]['zillow_id']), self.df.iloc[record_number]['unformatted_price']]
                        print(row_string)
                        for val in row.find_all("span"):
                            row_string.append('"' + val.text.replace("\n", "").strip() + '"')
                        print(row_string)
                        out.write(",".join(row_string) + "\n")
                if self.df.iloc[record_number]['zillow_url'] in self.failed_urls.keys():
                    del self.failed_urls[self.df.iloc[record_number]['zillow_url']]
                # time.sleep(5)
                print("Success")
                price_change_history = soup.find_all(class_="hdp__sc-966lqz-4 gLKwXO")
                if not price_change_history:
                    with open("sales.csv", "a") as out:
                        out.write(str(self.df.iloc[record_number]['zillow_id']) + "\n")
            except Exception as e:
                        print(e)


    def run(self):
        # go through self.df get every url parse it and save the time if there is any in the page.
        i = 0
        number_of_threads = 1
        while i < len(self.df):
            threads = []
            drivers = []
            for _ in range(number_of_threads):
                if i < len(self.df):
                    try:
                        # header = self.proxy.get_header()
                        # self.chrome_options = Options()
                        # self.chrome_options.add_argument('--proxy-server=%s' % PROXY)
                        # self.chrome_options.add_argument("—incognito")
                        # prefs = {"profile.managed_default_content_settings.images": 2}
                        # # self.chrome_options.add_experimental_option("prefs", {"profile.default_content_settings.cookies": 2})
                        # self.chrome_options.add_experimental_option("prefs",prefs)
                        # # self.chrome_options.add_argument('--headless')
                        # for key in header.keys():
                        #     self.chrome_options.add_argument(f"{key}={header[key]}")
                        # driver = webdriver.Chrome(options=self.chrome_options)
                        options = Options()
                        firefox_capabilities = webdriver.DesiredCapabilities.FIREFOX
                        firefox_capabilities['marionette'] = True
                        firefox_capabilities['proxy'] = {
                            "proxyType": "MANUAL",
                            "httpProxy": PROXY,
                            "ftpProxy": PROXY,
                            "sslProxy": PROXY
                        }
                        firefox_profile = webdriver.FirefoxProfile()
                        # Disable CSS
                        firefox_profile.set_preference('permissions.default.stylesheet', 2)
                        # Disable images
                        firefox_profile.set_preference('permissions.default.image', 2)
                        options.headless = True
                        driver = webdriver.Firefox(capabilities=firefox_capabilities, firefox_profile=firefox_profile, options=options)
                        drivers.append(driver)
                        t = threading.Thread(target=self.save_record, args=(list(range(i, i+500)), driver))
                        t.start()
                        # time.sleep(5)
                        threads.append(t)
                        i += 500
                    except Exception as e:
                        continue
            try:
                for thread in threads:
                    thread.join()
                for driver in drivers:
                    driver.close()
            except Exception as e:
                for thread in threads:
                    sys.exit()
                pass
        i = 0
        while self.failed_urls:
            failed_urls = set(self.failed_urls.keys())
            failed_indexes = list(self.df[self.df["zillow_url"].isin(failed_urls)].index)
            try:
                threads = []
                drivers = []
                for _ in range(number_of_threads):
                    try:
                        # header = self.proxy.get_header()
                        # self.chrome_options = Options()
                        # self.chrome_options.add_argument('--proxy-server=%s' % PROXY)
                        # # self.chrome_options.add_argument('--headless')
                        # prefs = {"profile.managed_default_content_settings.images": 2}
                        # self.chrome_options.add_experimental_option("prefs",prefs)  # chrome_options.add_argument('--headless')
                        # # self.chrome_options.add_experimental_option("prefs", {"profile.default_content_settings.cookies": 2})
                        # for key in header.keys():
                        #     self.chrome_options.add_argument(f"{key}={header[key]}")
                        # driver = webdriver.Chrome(options=self.chrome_options)
                        options = Options()
                        firefox_capabilities = webdriver.DesiredCapabilities.FIREFOX
                        firefox_capabilities['marionette'] = True
                        firefox_capabilities['proxy'] = {
                            "proxyType": "MANUAL",
                            "httpProxy": PROXY,
                            "ftpProxy": PROXY,
                            "sslProxy": PROXY
                        }
                        firefox_profile = webdriver.FirefoxProfile()
                        # Disable CSS
                        firefox_profile.set_preference('permissions.default.stylesheet', 2)
                        # Disable images
                        firefox_profile.set_preference('permissions.default.image', 2)
                        options.headless = True
                        driver = webdriver.Firefox(capabilities=firefox_capabilities, firefox_profile=firefox_profile, options=options)
                        drivers.append(driver)
                        # driver = webdriver.Firefox()
                        # drivers.append(driver)
                        # time.sleep(5)
                        t = threading.Thread(target=self.save_record, args=(failed_indexes[i:i+100], driver))
                        t.start()
                        threads.append(t)
                        i += 100
                    except Exception as e:
                        continue
                try:
                    for thread in threads:
                        thread.join()
                    for driver in drivers:
                        driver.close()
                except Exception as e:
                    pass
            except Exception as e:
                pass


    def fetch(self, url, driver):
        soup, tries = None, 1
        while True:
            headers = self.get_headers()
            try:
                # source = self.http.request('GET', url, headers=headers, timeout=7)
                driver.get(url)
                source = driver.page_source
                self.fetches += 1
                soup = BeautifulSoup(source, 'lxml')
                if not self.is_captcha(soup) and soup:
                    # The page is valid so we can just return a soup version of the site
                    self.visited_sites.add(url)
                    # Add cookie to the pool of cookies
                    new_cookie = self.parse_cookie(source)
                    self.cookies.add(new_cookie)
                    return soup
                tries += 1
                if tries%2 == 0:
                    time.sleep(tries*4.7)
                if tries > self.max_tries:
                    print(f"The upper bound of tries has been reached for url {url}")
                    time.sleep(15)
                    raise MaxTriesError
            except (MaxTriesError, TimeoutError, MaxRetryError, socket.timeout, selenium.common.exceptions.WebDriverException):
                if self.failed_urls[url] >= self.max_retries:
                    del self.failed_urls[url]
                    print("The URL failed too many times, check the proxy or the internet connection")
                else:
                    time.sleep(25)
                    self.failed_urls.update([url])
                    print("Will try again later")
                    time.sleep(10*tries)
                return BeautifulSoup("", 'lxml')

if __name__ == '__main__':
    rs = RentScraper();
    rs.run()


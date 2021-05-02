import json
from urllib.parse import urlencode, quote
from collections import OrderedDict


class ZillowQuery:
    """This is a query object used for going through the pages of a zillow query. The job of a query object,
    is to store all the pages related to a search. It can also transform a url to a query or a query to a url.

    Note: the arguments on the constructor are named as the arguments that zillow uses so that is why the names
    are in camelcase instead of all lower case."""

    def __init__(self, max_price, min_price, base, usersSearchTerm, mapBounds, regionSelection, status=0, **kwargs):
        self.pagination = {}  # it needs to be a dictionary that contains the key 'currentPage'
        self.base = base  # the base url as in https://www.zillow.com/
        self.search_term = usersSearchTerm
        self.status = status
        self.map_bounds = mapBounds
        self.region = regionSelection
        self.map_zoom = 13
        self.min_price = min_price
        self.max_price = max_price
        self.is_map_visible = True
        self.is_list_visible = True
        self.filterState = OrderedDict()
        if self.status == 0:
            self.filterState['sortSelection'] = {'value': 'pricea'}
            self.filterState['price'] = OrderedDict({'max': self.max_price, 'min': self.min_price})
            self.head = f"https://www.zillow.com{self.base}sold/?searchQueryState"
            self.filterState['isRecentlySold'] = {'value': True}
            # self.filterState['isForSaleByAgent'] = {'value': False}
            # self.filterState['isForSaleByOwner'] = {'value': False}
            # self.filterState['isNewConstruction'] = {'value': False}
            self.filterState['isComingSoon'] = {'value': False}
            # self.filterState['isAuction'] = {'value': False}
            # self.filterState['isForSaleForeclosure'] = {'value': False}
            # self.filterState['isPreMarketForeclosure'] = {'value': False}
            # self.filterState['isPreMarketPreForeclosure'] = {'value': False}
        elif self.status == 1:
            self.filterState['sortSelection'] = {'value': 'pricea'}
            self.filterState['price'] = OrderedDict({'max': self.max_price, 'min': self.min_price})
            self.head = f"https://www.zillow.com{self.base}?searchQueryState"
        else:
            self.head = f"https://www.zillow.com{self.base}rentals/?searchQueryState"
            self.filterState['fsba'] = {'value': False}
            self.filterState['fsbo'] = {'value': False}
            self.filterState['nc'] = {'value': False}
            self.filterState['fore'] = {'value': False}
            self.filterState['cmsn'] = {'value': False}
            self.filterState['auc'] = {'value': False}
            self.filterState['pmf'] = {'value': False}
            self.filterState['pf'] = {'value': False}
            self.filterState['fr'] = {'value': True}
            self.filterState['ah'] = {'value': True}
            self.filterState['mp'] = OrderedDict({'max': self.max_price, 'min': self.min_price})


    def get_page(self, page_num):
        """Get the url for the page with pagination number num and with the settings of the current query"""
        current_state = (self.head, self.pagination)  # save current state for later so that we don't change things
        if self.status == 0:  # if status is Sold
            self.head = f"https://www.zillow.com{self.base}sold/{page_num}_p/?searchQueryState"
        elif self.status == 1:  # if status is For Sale
            self.head = f"https://www.zillow.com{self.base}{page_num}_p/?searchQueryState"
        else:
            self.head = f"https://www.zillow.com{self.base}rentals/{page_num}_p/?searchQueryState"
        self.pagination = {'currentPage': page_num}
        page_url = self.get_first_url()
        self.head, self.pagination = current_state
        return page_url

    def get_urls(self, last, first=1):
        url_list = []
        for i in range(first, last + 1):
            url_list.append(self.get_page(i))
        return url_list

    def get_first_url(self):
        url_string = ["{"]
        keys_ = ['pagination', 'usersSearchTerm', 'mapBounds', 'regionSelection', 'isMapVisible', 'mapZoom', 'filterState', 'isListVisible']
        objects_ = [self.pagination, self.search_term, self.map_bounds, self.region, self.is_map_visible, self.map_zoom, self.filterState, self.is_list_visible]
        for i in range(len(keys_)):
            url_string.append(f"\"{keys_[i]}\"")
            url_string.append(" :")
            url_string.append(json.dumps(objects_[i]))
            if i != len(keys_) - 1:
                url_string.append(',')
        url_string.append("}")
        url_string = "".join(url_string)
        url_string = url_string.replace(' ', '')
        url_string = url_string.replace('"', '%22')
        url_string = self.head + "=" + url_string
        return url_string

    def __repr__(self):
        return self.get_first_url()

    @staticmethod
    def url_to_query(url, increment):
        url_query = url.split('=')
        domain = url_query[0]
        query_state = url_query[1].replace('%22', '"')
        sold = True if 'sold' in url else False
        query_dict = json.loads(query_state)
        try:
            min_price, max_price = query_dict['price']['min'], query_dict['price']['max']
        except KeyError:
            min_price, max_price = 0, increment
        return ZillowQuery(min_price=min_price, max_price=max_price, sold=sold, base=domain, **query_dict)

from typing import Iterable
import scrapy
from scrapy import Request
from scrapy.http import JsonRequest
import json

PAGE_SIZE = 10


class BooksSpider(scrapy.Spider):
    name = "books"
    allowed_domains = ["zjlib.cn"]
    start_urls = ["https://zjlib.cn"]

    def start_requests(self) -> Iterable[Request]:
        for left_letter in list("abcdefghijklmnopqrstuvwxyz") + ["other"]:
            yield request_page(1, left_letter=left_letter, meta={"page_no": 1, "left_letter": left_letter})

    def parse(self, response):
        page_no = response.meta["page_no"]
        left_letter = response.meta["left_letter"]
        self.log(f"Fetched page where page_no={page_no}, left_letter={left_letter}")

        d = json.loads(response.text)
        assert d['code'] == 1, d
        d = d['data']
        assert page_no == int(d['curPage'])
        assert int(d['totalRecords']) <= 100000 # the API limit
        total_pages = int(d['totalPages'])

        for item in d["results"]:
            yield item

        if page_no < total_pages:
            yield request_page(page_no + 1, left_letter=response.meta["left_letter"], meta={"page_no": page_no + 1, "left_letter": left_letter})



def request_page(page_no=1, page_size=PAGE_SIZE, left_letter=None, meta=None):
    # {"page":1,"pageSize":10,"wfwfid":"2120","sorts":{"value":"default"},"classifies":[{"id":"left_letter:a","pid":"left_letter","name":"a(660)"}]}
    filters = (
        [{"id": f"left_letter:{left_letter}", "pid": "left_letter", "name": "x(0)"}]
        if left_letter
        else []
    )
    d = {
        "page": page_no,
        "pageSize": page_size,
        "wfwfid": "2120",
        "sorts": {"value": "default"},
        "classifies": filters,
    }
    return JsonRequest(
        "https://history.zjlib.cn/app/universal-search/search-list?wfwfid=2120&searchId=0&params=",
        data=d,
        meta=meta,
    )

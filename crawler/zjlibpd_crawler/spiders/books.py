from typing import Iterable
import scrapy
from scrapy import Request
from scrapy.http import JsonRequest, Response
import json
import re
from ast import literal_eval
import json
from mergedeep import merge, Strategy

PAGE_SIZE = 10


class BooksSpider(scrapy.Spider):
    name = "books"
    allowed_domains = ["zjlib.cn"]
    start_urls = ["https://zjlib.cn"]

    def start_requests(self) -> Iterable[Request]:
        # for i, left_letter in enumerate(list("abcdefghijklmnopqrstuvwxyz") + ["other"]):
        #     prio = (1 << 10) - 1 - i
        #     yield self.request_page(
        #         1,
        #         left_letter=left_letter,
        #         priority=prio,
        #     )
        yield self.request_page(1, search_id=24016)

    def parse_page(self, response):
        page_no = response.meta["page_no"]
        left_letter = response.meta["left_letter"]
        page_priority = response.meta.get("priority", 0)
        self.log(
            f"Fetched page where page_no={page_no}, left_letter={left_letter}, priority={page_priority}"
        )

        d = json.loads(response.text)
        assert d["code"] == 1, d
        d = d["data"]
        assert page_no == int(d["curPage"])
        assert int(d["totalRecords"]) <= 100000  # the API limit
        total_pages = int(d["totalPages"])

        for i, item in enumerate(d["results"]):
            # Note:
            # fields of items listed on pages are non-exhaustive, but they do have the attr
            # sub_resources which are not present when requesting items seperately
            # so we request each item separately and merge them
            priority = (page_priority << 20) + (1 << 20) - 1 - i
            self.log(
                f"page_no={page_no}, left_letter={left_letter}, i={i}, id={item['id']}"
            )
            yield self.request_item(
                item["id"],
                meta={"item_on_page": item},
                priority=priority,
            )

            for sub in item.get("sub_resources", []):  # typically volumes of a book
                # we also request items seperately for those listed in sub_resources, but we don't
                # merge fields for them
                # it is expected to be done externally
                yield self.request_item(
                    sub["id"],
                    meta={"parent_id": item["id"]},
                    priority=priority,
                )

        if page_no < total_pages:
            page_no += 1
            priority = page_priority - 1
            yield self.request_page(
                page_no,
                left_letter=response.meta["left_letter"],
                priority=priority,
            )

    def parse_item(self, response):
        parent_id = response.meta.get("parent_id")
        priority = response.meta["priority"]

        d = json.loads(extract_var("resDatails", response.text))

        if parent_id:
            # TODO: if sub_resources are also listed on pages, the flag set here might be overrided
            d["__PARENT__"] = parent_id
        else:
            d = merge(
                d, response.meta["item_on_page"], strategy=Strategy.TYPESAFE_ADDITIVE
            )
            d["__MERGED__"] = True

        for field in d["fields"]:
            if field["key"] == "获取方式" or field["key"] == "阅读":
                url = next(
                    s["orsUrl"] for s in field["subs"] if s["fieldType"] == "file"
                )
                yield Request(
                    url,
                    meta={"item": d, "priority": priority},
                    cookies={"website_id": 73953},
                    priority=priority,
                    callback=self.parse_reader,
                )
                break
        else:
            yield d

    def parse_reader(self, response: Response):
        item = response.meta["item"]

        meta = json.loads(extract_var("readerObj", response.text))
        total_pages = int(extract_var("pageNum", response.text))
        dir_url = extract_var("imgUrl", response.text)

        item["__READER__"] = {
                "readerObj": meta,
                "pageNum": total_pages,
                "imgUrl": dir_url,
            }
        yield item
        # assert dir_url.startswith("encodeURIComponent"), dir_url
        # dir_url = re.search(r'"(.+)"', dir_url).group(1)
        # assert dir_url.endswith("pdfImgaes/"), dir_url
        # dir_url = dir_url.removesuffix("pdfImgaes/") + meta['fileName'] + "." + meta['fileType']

    def request_item(self, id, **kwargs):
        meta = kwargs.get("meta", {})
        if priority := kwargs.get("priority"):
            meta['priority'] = priority
        kwargs['meta'] = meta

        # pageId is used to render webpage, irrelevant to data
        return Request(
            f"https://history.zjlib.cn/app/universal-search/resource/{id}/details?wfwfid=2120&searchId=0&params=&pageId=107556&classifyId=&classifyName=",
            # without this, probation=1 (trial mode) would be set by the server
            cookies={"website_id": 73953},
            callback=self.parse_item,
            **kwargs,
        )

    def request_page(
        self, page_no=1, page_size=PAGE_SIZE, search_id=0, left_letter=None, **kwargs
    ):
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
            "sorts": {"value": "default"}
            # "classifies": filters,
        }
        meta = kwargs.get("meta", {})
        meta['page_no'] = page_no
        meta['left_letter'] = left_letter
        meta['search_id'] = search_id
        if priority := kwargs.get("priority"):
            meta['priority'] = priority
        kwargs['meta'] = meta

        return JsonRequest(
            f"https://history.zjlib.cn/app/universal-search/search-list?wfwfid=2120&searchId={search_id}&params=",
            data=d,
            cookies={"website_id": 73953},
            callback=self.parse_page,
            **kwargs,
        )


def extract_var(varname, html):
    m = re.search(rf"var\s+{varname}\s*=\s*(.+?)\s*;?\s*$", html, flags=re.MULTILINE)
    return m.group(1)

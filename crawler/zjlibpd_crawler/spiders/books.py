from typing import Iterable
import scrapy
from scrapy import Request
from scrapy.http import JsonRequest, Response
import json
import re
from ast import literal_eval
import json
from mergedeep import merge, Strategy
from urllib.parse import urlparse, urljoin, parse_qs

PAGE_SIZE = 10


# TODO:
# 65e6fe81969db848e322aa77 65e6fe7f969db848e322aa5a 500


class BooksSpider(scrapy.Spider):
    name = "books"
    allowed_domains = ["zjlib.cn"]
    start_urls = ["https://zjlib.cn"]

    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "FEEDS": {
            "items.json": {
                "format": "jsonl",
                "encoding": "utf8",
                "overwrite": False,
                "store_empty": False,
            },
        },
    }

    def start_requests(self) -> Iterable[Request]:
        # for i, left_letter in enumerate(list("abcdefghijklmnopqrstuvwxyz") + ["other"]):
        #     prio = (1 << 10) - 1 - i
        #     yield self.request_page(
        #         1,
        #         left_letter=left_letter,
        #         priority=prio,
        #     )
        # yield self.request_item("62b5518e3157d263ee6a445e")
        yield self.request_page(1, search_id=24016, priority=(1 << 10) - 1)

    def parse_page(self, response):
        page_no = response.meta["page_no"]
        left_letter = response.meta["left_letter"]
        search_id = response.meta["search_id"]
        page_priority = response.meta.get("priority", 0)
        self.log(
            f"Fetched page where page_no={page_no}, search_id={search_id}, left_letter={left_letter}, priority={page_priority}"
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
                f"page_no={page_no}, search_id={search_id}, left_letter={left_letter}, i={i}, id={item['id']}, priority={page_priority}"
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
                search_id=search_id,
                left_letter=response.meta["left_letter"],
                priority=priority,
            )

    def parse_item(self, response):
        parent_id = response.meta.get("parent_id")
        priority = response.meta.get("priority", 0)

        d = json.loads(extract_var("resDatails", response.text))

        if parent_id:
            # sub resource
            # TODO: if sub_resources are also listed on pages, the flag set here might be overrided
            d["__PARENT__"] = parent_id
        elif item_on_page := response.meta.get("item_on_page"):
            # top-level resource
            d = merge(d, item_on_page, strategy=Strategy.TYPESAFE_ADDITIVE)
            d["__MERGED__"] = True

        for field in d["fields"]:
            if field["key"] == "获取方式" or field["key"] == "阅读":
                assert all(s["fieldType"] == "file" for s in field["subs"])
                # if len(field["subs"]) == 1:
                #     pass
                # url = next(
                #     s["orsUrl"] for s in field["subs"] if s["fieldType"] == "file"
                # )
                if subs := field["subs"]:
                    urls = [s["orsUrl"] for s in subs]
                else:
                    urls = [field["orsUrl"]]
                yield self.request_reader(urls, priority=priority, meta={"item": d})

                break
        else:
            yield d

    def parse_reader(self, response: Response):
        priority = response.meta.get("priority", 0)
        item = response.meta["item"]
        left_urls = response.meta["urls"]

        if (url := urlparse(response.url)).path.endswith("pdfjs/web/viewer.html"):
            # only for id=62b5518e3157d263ee6a445e so far
            reader = urljoin(response.url, parse_qs(url.query)["file"][0])
        else:
            metadata = json.loads(extract_var("readerObj", response.text))
            total_pages = int(extract_var("pageNum", response.text))
            dir_url = extract_var("imgUrl", response.text)
            reader = {
                "readerObj": metadata,
                "pageNum": total_pages,
                "imgUrl": dir_url,
            }
        item.setdefault("__READER__", []).append(reader)

        if left_urls:
            yield self.request_reader(left_urls, priority=priority, meta={"item": item})
        else:
            yield item
        # assert dir_url.startswith("encodeURIComponent"), dir_url
        # dir_url = re.search(r'"(.+)"', dir_url).group(1)
        # assert dir_url.endswith("pdfImgaes/"), dir_url
        # dir_url = dir_url.removesuffix("pdfImgaes/") + meta['fileName'] + "." + meta['fileType']

    def request_reader(self, urls, website_id=73953, **kwargs):
        meta = kwargs.setdefault("meta", {})
        assert "item" in meta
        assert "urls" not in meta
        if priority := kwargs.get("priority"):
            meta["priority"] = priority

        url = urls.pop(0)
        meta["urls"] = urls
        return Request(
            url,
            cookies={"website_id": website_id},
            callback=self.parse_reader,
            **kwargs,
        )

    def request_item(self, id, **kwargs):
        meta = kwargs.get("meta", {})
        if priority := kwargs.get("priority"):
            meta["priority"] = priority
        kwargs["meta"] = meta

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
            "sorts": {"value": "default"},
            # "classifies": filters,
        }
        meta = kwargs.get("meta", {})
        meta["page_no"] = page_no
        meta["left_letter"] = left_letter
        meta["search_id"] = search_id
        if priority := kwargs.get("priority"):
            meta["priority"] = priority
        kwargs["meta"] = meta

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

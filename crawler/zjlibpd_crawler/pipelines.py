# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import pymongo
from pymongo import ReplaceOne
from pymongo.errors import BulkWriteError
from itemadapter import ItemAdapter
from bson.objectid import ObjectId

BUFFER_SIZE = 100


class ZjlibpdCrawlerPipeline:

    def process_item(self, item, spider):
        return item


class MongoPipeline:
    collection_name = "items"

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.buffer = []

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get("MONGO_URI"),
            mongo_db=crawler.settings.get("MONGO_DATABASE", "zjlibpd"),
        )

    def open_spider(self, spider):
        spider.log(f"MongoPipeline BUFFER_SIZE={BUFFER_SIZE}")
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        count = 0
        self.db[self.collection_name].bulk_write(self.buffer, ordered=False)
        self.buffer = []
        # while (e := self._try_flush_buffer(spider)) and count < 1:
        #     spider.logger.info(f"Retrying to flush buffer")
        #     count += 1
        # if e:
        #     raise e

        self.client.close()

    def process_item(self, item, spider):
        item = ItemAdapter(item).asdict()
        item["_id"] = ObjectId(item["id"])
        del item["id"]
        req = ReplaceOne({"_id": item["_id"]}, item, upsert=True)
        # self.db[self.collection_name].replace_one({'_id': item['_id']}, item, upsert=True)
        self.buffer.append(req)

        if len(self.buffer) >= BUFFER_SIZE:
            # self._try_flush_buffer(spider)
            self.db[self.collection_name].bulk_write(self.buffer, ordered=False)
            self.buffer = []

        return item

    # def _try_flush_buffer(self, spider):
    #     if self.buffer:
    #         try:
    #             self.db[self.collection_name].bulk_write(self.buffer, ordered=False)
    #         except BulkWriteError as e:
    #             spider.logger.warn(f"Failed to flush buffer with {len(self.buffer)} items", exc_info=e)
    #             return e
    #         self.buffer = []

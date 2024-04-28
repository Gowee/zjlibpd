# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import pymongo
from itemadapter import ItemAdapter
from bson.objectid import ObjectId


class ZjlibpdCrawlerPipeline:

    def process_item(self, item, spider):
        return item


class MongoPipeline:
    collection_name = "items"

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get("MONGO_URI"),
            mongo_db=crawler.settings.get("MONGO_DATABASE", "zjlibpd"),
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        item = ItemAdapter(item).asdict()
        item['_id'] = ObjectId(item['id'])
        del item['id']
        self.db[self.collection_name].replace_one({'_id': item['_id']}, item, upsert=True)
        return item

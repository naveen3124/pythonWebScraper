# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

import redis
from scrapy_redis.pipelines import RedisPipeline
import json
import logging


class KanoonScraperItemPipeline(RedisPipeline):

    def __init__(self, server):
        self.server = server

    def process_item(self, item, spider):
        # logging.debug(f"Processing item: {item}")
        item_key = str(item['case_id'])
        serialized_data = json.dumps(item.__dict__)
        self.server.hset(item['stored_hset_name'], key=item_key, value=serialized_data)
        return item

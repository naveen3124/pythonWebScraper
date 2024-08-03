# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface

import json

from scrapy import signals
from scrapy_redis.pipelines import RedisPipeline
from scrapy.signalmanager import dispatcher
from kanoon_scraper.settings import REDIS_LUCENE_CHANNEL_NAME


class KanoonScraperItemPipeline(RedisPipeline):

    def __init__(self, server):
        self.server = server

    def check_subscribers(self, spider):
        # Get the list of subscribers for the specified channel
        subscribers = self.server.pubsub_channels(REDIS_LUCENE_CHANNEL_NAME)

        # Check the number of subscribers
        num_subscribers = len(list(subscribers))

        if num_subscribers <= 0:
            print(f"There are no subscribers to {REDIS_LUCENE_CHANNEL_NAME}. Closing the spider.")
            dispatcher.send(signal=signals.spider_closed, spider=spider, reason='No subscribers')

    def process_item(self, item, spider):
        # logging.debug(f"Processing item: {item}")
        item_key = str(item['case_id'])
        serialized_data = json.dumps(item.__dict__)
        #self.check_subscribers(spider)
        self.server.hset(item['stored_hset_name'], key=item_key, value=serialized_data)
        #if item['stored_hset_name'] == "id_to_doc_map":
            #self.server.publish(REDIS_LUCENE_CHANNEL_NAME, serialized_data)
        return item

    def close_spider(self, spider):
        #self.server.publish(REDIS_LUCENE_CHANNEL_NAME, "STOP_LISTENING")
        return
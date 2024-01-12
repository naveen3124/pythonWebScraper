# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class CaseItem(scrapy.Item):
    case_source = scrapy.Field()
    case_author = scrapy.Field()
    case_bench = scrapy.Field()
    case_judgement = scrapy.Field()
    case_title = scrapy.Field()

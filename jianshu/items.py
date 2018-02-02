# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy.contrib.loader.processor import TakeFirst,MapCompose

class JianshuSummaryItem(scrapy.Item):
    # define the fields for your item here like:
    title = scrapy.Field(output_processor=TakeFirst())
    link = scrapy.Field(output_processor=TakeFirst())
    author = scrapy.Field(output_processor=TakeFirst())
    author_url = scrapy.Field(output_processor=TakeFirst())
    timestamp = scrapy.Field(output_processor=TakeFirst())
    read = scrapy.Field(output_processor=TakeFirst())
    reply = scrapy.Field(output_processor=TakeFirst())
    like = scrapy.Field(output_processor=TakeFirst())
    money = scrapy.Field(output_processor=TakeFirst())
    pass

class JianshuArticleItem(scrapy.Item):
    url = scrapy.Field(output_processor=TakeFirst())
    content = scrapy.Field()
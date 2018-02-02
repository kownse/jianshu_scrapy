# -*- coding: utf-8 -*-
import scrapy
from jianshu.util.MongoCache import MongoCache
from jianshu.items import JianshuArticleItem
from scrapy.contrib.loader import ItemLoader
from scrapy.http import Request

class ArticleSpider(scrapy.Spider):
    name = 'article'
    allowed_domains = ['jianshu']

    def start_requests(self):
        mongoCache = MongoCache()
        toplinks = mongoCache.getTopNSummariesLinks(800)
        for link in toplinks:
            if mongoCache.isArticleValid(link) is False:
                yield Request(link)
            # else:
            #     print('still valid')

    def parse(self, response):
        l = ItemLoader(item=JianshuArticleItem(), response=response)
        l.add_xpath('content', '//div[@class="article"]/div[@class="show-content"]/p/text()')
        l.add_value('url', response.url)
        return l.load_item()

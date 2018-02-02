# -*- coding: utf-8 -*-
import scrapy
from scrapy.selector import Selector
from scrapy.http import HtmlResponse
from jianshu.items import JianshuSummaryItem
from scrapy.contrib.loader import ItemLoader
from scrapy.contrib.loader.processor import MapCompose, Join
from scrapy.http import Request
from urllib.parse import urljoin

import io
import sys
import codecs
sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())


class NovelSpider(scrapy.Spider):
    name = 'novel'
    allowed_domains = ['jianshu']

    def start_requests(self):
        url_paths = ['http://www.jianshu.com/c/dqfRwQ?order_by=top&page=', 
            'http://www.jianshu.com/c/dqfRwQ?order_by=added_at&page=', \
            'http://www.jianshu.com/c/dqfRwQ?order_by=commented_at&page=', \
            'http://www.jianshu.com/c/fcd7a62be697?order_by=top&page=', \
            'http://www.jianshu.com/c/fcd7a62be697?order_by=added_at&page=', \
            'http://www.jianshu.com/c/fcd7a62be697?order_by=commented_at&page=', \
            ]
            
        for urlroot in url_paths:
            for i in range(1, 201):
                cururl = urlroot + str(i)
                yield Request(cururl)

    def parse(self, response):
        contents = response.xpath('//ul[@class="note-list"]/li/div[@class="content"]')
        for content in contents:  
            l = ItemLoader(item=JianshuSummaryItem(), selector=content, response=response)
            l.add_xpath('title', 'a[@class="title"]/text()', MapCompose(lambda i:i.replace('|','.').replace('丨', '.')))
            l.add_xpath('link', 'a[@class="title"]/@href', MapCompose(lambda i:urljoin(response.url, i)))
            l.add_xpath('author', 'div[@class="author"]/div[@class="info"]/a[@class="nickname"]/text()')
            l.add_xpath('author_url', 'div[@class="author"]/div[@class="info"]/a[@class="nickname"]/@href', MapCompose(lambda i:urljoin(response.url, i)))
            l.add_xpath('timestamp', 'div[@class="author"]/div[@class="info"]/span[@class="time"]/@data-shared-at')
            l.add_xpath('read', 'div[@class="meta"]/a[1]/text()[2]', MapCompose(str.strip, int))
            l.add_xpath('reply', 'div[@class="meta"]/a[2]/text()[2]', MapCompose(str.strip, int))
            l.add_xpath('like', 'div[@class="meta"]/span[1]/text()', MapCompose(str.strip, int))
            l.add_xpath('money', 'div[@class="meta"]/span[2]/text()', MapCompose(str.strip, int))

            yield l.load_item()
        pass

# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from jianshu.util.MongoCache import MongoCache
from scrapy.exceptions import DropItem
from datetime import datetime, timedelta
import hashlib

class JianshuPipeline(object):
    def __init__(self):
        self.mongoCache = MongoCache()
        self.seenurl = set()
        self.insertSummaryBulk = None
        self.insertArticleBase = None
        self.bulks = []

    def open_spider(self, spider):
        if spider.name == 'novel':
            self.mongoCache.clearSummary()

    def process_item(self, item, spider):
        if spider.name == 'novel':
            return self.deal_novel(item)
        elif spider.name == 'article':
            return self.deal_article(item)
    
    def deal_article(self, item):
        if item['url'] in self.seenurl:
            raise DropItem('parsed item')
        else:
            self.seenurl.add(item['url'])

        str_list = []
        for section in item['content']:
            str_list.append('  ' + section + '\n')
        str_article = ''.join(str_list)
        self.mongoCache.setArticle(item['url'], str_article)
    
    def deal_novel(self, item):
        if item['link'] in self.seenurl:
            raise DropItem('parsed item')
        else:
            self.seenurl.add(item['link'])
            summary = dict(item)
            summary['key'] = hashlib.md5(summary['link'].encode("utf8")).hexdigest()
            timestr = summary['timestamp'][0:16]
            sharetime = datetime.strptime(timestr, '%Y-%m-%dT%H:%M')
            summary['timestamp'] = sharetime
            timedelta = datetime.now() - sharetime
            days = timedelta.days
            if days <= 1:
                days = 1
            summary['days'] = days

            read = 'read' in summary and summary['read'] or 0
            like = 'like' in summary and summary['like'] or 0
            reply = 'reply' in summary and summary['reply'] or 0
            money = 'money' in summary and summary['money'] or 0
            summary['read'] = read
            summary['like'] = like
            summary['reply'] = reply
            summary['money'] = money
            summary['score'] = int(( read + like * 5 + reply * 10 + money * 100) / days)

            self.insertSummaryBulk = self.mongoCache.setSummaryLineBulk(summary, self.bulks, self.insertSummaryBulk)

            info = {'key': summary['key'], 'link':summary['link'], 'title':summary['title'], 'author':summary['author'], 'author_url':summary['author_url'], 'sharetime':summary['timestamp']}
            self.insertArticleBase = self.mongoCache.setArticleBaseInfoBulk(info, self.bulks, self.insertArticleBase)
            return item

    def close_spider(self, spider):
        self.seenurl.clear()
        self.excuteMongoBulk()

    def excuteMongoBulk(self):
        for bulk in self.bulks:
            self.mongoCache.excuteBulk(bulk)
        self.bulks.clear()
        
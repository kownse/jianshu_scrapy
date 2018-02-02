
from datetime import datetime, timedelta
from pymongo import MongoClient
import pickle
import zlib
import random
from pprint import pprint
from pymongo.errors import BulkWriteError

class MongoCache:
    def __init__(self, client=None, expires=-1):
        self.client = MongoClient('mongodb://user:password@127.0.0.1:27017/jianshu') if client is None else client
        self.expires = expires

        self.db = self.client.jianshu
        # self.db = self.client.test
        
        # self.db.webpage.create_index('timestamp', expireAfterSeconeds=expires.total_seconds())

    def __del__(self):
        self.client.close()

    def __getitem__(self, url):
        record = self.db.webpage.find_one({'_id':url})
        if record:
            return pickle.loads(zlib.decompress(record['page']))
        else:
            raise KeyError(url + ' does not exist')
    
    def __setitem__(self, url, page):
        record = { \
            'page': zlib.compress(pickle.dumps(page)), \
             'timestamp':datetime.now() \
        }
        self.db.webpage.update({'_id':url}, {'$set': record}, upsert=True)

    def startUnOrderedBulk(self, colt):
        return colt.initialize_unordered_bulk_op()

    def excuteBulk(self, bulk):
        if bulk is not None:
            try:
                result = bulk.execute()
                pprint(result)
            except BulkWriteError as bwe:
                pprint(bwe.details)
            

    def checkExpireArticleAndGet(self, url):
        return self.checkExpireAndGet(url, self.db.articles, 'article')

    def isArticleValid(self, url):
        valid, record = self.checkExpire(url, self.db.articles, 'article')
        return valid

    def getArticle(self, url):
        record = self.db.articles.find_one({'_id':url})
        if record:
            return pickle.loads(zlib.decompress(record['article']))
        else:
            raise KeyError(url + ' does not exist')

    def setArticle(self, url, article):
        record = { \
            'article': zlib.compress(pickle.dumps(article)), \
             'timestamp':datetime.now() \
        }
        self.db.articles.update({'_id':url}, {'$set': record}, upsert=True)

    def checkExpire(self, url, collection, targetname):
        if self.expires == 0:
            return False, None

        record = collection.find_one({'_id':url})
        if record is None:
            return False, None

        if self.expires < 0:
            return True, record

        now = datetime.now()
        updatetime = record['timestamp']
        duration = now - updatetime
        if duration.days > self.expires:
            return (False, None)
        return True, record

    def checkExpireAndGet(self, url, collection, targetname):
        valid, record = self.checkExpire(url, collection, targetname)
        if valid is True:
            return pickle.loads(zlib.decompress(record[targetname]))

    def checkExpireGet(self, url):
        return self.checkExpireAndGet(url, self.db.webpage, 'page')

    def clearSummary(self):
        self.db.summaries.remove()

    def getSummary(self, url):
        summary = self.db.summaries.find_one({'_id':url})
        if summary:
            return summary
        else:
            raise KeyError(url + ' does not exist')

    def getTopNSummariesLinks(self, topn):
        dblinks = self.db.summaries.find({}, {'_id':0, 'link':1}).sort([('score', -1)]).limit(topn)
        links = []
        for dbl in dblinks:
            links.append(dbl['link'])
        return links

    def getBestSummaryByAuthorUrl(self, author_url, topn):
        return self.db.summaries.find({'author_url':author_url}).sort([('score',-1)]).limit(topn)

    def setSummary(self, key, link, author, author_url, title, score, read, reply, like, money, days, sharetime):
        mt = {'link':link, 'author':author, 'author_url':author_url, 'title':title, 'score':score, 'read':read, 'reply':reply, 'like':like, 'money':money, 'days':days, 'timestamp':sharetime}
        self.db.summaries.update({'key':key}, {'$set': mt}, upsert=True)

    def setSummaryByLine(self, line):
        self.db.summaries.update({'key':line['key']}, {'$set': line}, upsert=True)

    def setSummaryLineBulk(self, line, bulklist, bulk=None):
        if bulk is None:
            bulk = self.startUnOrderedBulk(self.db.summaries)
            bulklist.append(bulk)

        bulk.insert(line)
        return bulk
    
    def setSummaryRankVar(self, key, rankchg):
        self.db.summaries.update({'key':key}, {"$set":{"rankchg": rankchg}})
    
    def setSummaryRankVarBulk(self, key, rankchg, bulklist, bulk=None):
        if bulk is None:
            bulk = self.startUnOrderedBulk(self.db.summaries)
            bulklist.append(bulk)

        bulk.find({'key':key}).update({"$set":{"rankchg": rankchg}})
        return bulk

    def setArticleBaseInfo(self, key, link, title, author, author_url, sharetime):
        info = {'key': key, 'link':link, 'title':title, 'author':author, 'author_url':author_url, 'sharetime':sharetime}
        self.db.articleinfos.update({'key':key}, {'$set': info}, upsert=True)

    def setArticleBaseInfoBulk(self, info, bulklist, bulk=None):
        if bulk is None:
            bulk = self.startUnOrderedBulk(self.db.articleinfos)
            bulklist.append(bulk)

        bulk.find({'key':info['key']}).upsert().update({'$set': info})
        return bulk

    def getAllArticleBaseInfo(self):
        return self.db.articleinfos.find()

    def setArticleHistoryInfo(self, key, score, read, reply, like, money, days, rank, rankvar, today):
        info = {'key': key, 'score':score, 'read':read, 'reply':reply, 'like':like, 'money':money, 'days':days, 'rank':rank, 'rankvar':rankvar, 'date': today}
        self.db.articlehistories.update({'key':key, 'date':today}, {'$set': info}, upsert=True)

    def setJianshuDaily(self, info):
        self.db.jianshudailies.update({'date':info['date']}, {'$set': info}, upsert=True)

    def setArticleHistoryInfoByLine(self, info):
        self.db.articlehistories.update({'key':info['key'], 'date':info['date']}, {'$set': info}, upsert=True)

    def updateArticleHistoryInfoByLineBulk(self, info, bulklist, bulk=None):
        if bulk is None:
            bulk = self.startUnOrderedBulk(self.db.articlehistories)
            bulklist.append(bulk)

        bulk.find({'key':info['key'], 'date':info['date']}).upsert().update({'$set': info})
        return bulk

    def getArticleDaysByKey(self, key, date):
        art = self.db.articleinfos.find_one({'key':key})
        if art is not None:
            return (date - art['sharetime']).days

    def getArticleHistoryInfoByKey(self, key):
        return self.db.articlehistories.find({'key':key}, {'_id':0}).sort([('date', 1)])

    def getArticleHistoryInfoByDate(self, date):
        return self.db.articlehistories.find({'date':date}, {'_id':0})

    def getArticleHistoryInfo(self, key, date):
        return self.db.articlehistories.find({'key':key, 'date':date})

    def getTopSummaries(self, skip, top):
        return self.db.summaries.find({}, {'_id':0}).sort([('score',-1)]).limit(top).skip(skip)

    def getAllSummaries(self):
        return self.db.summaries.find({}, {'_id':0}).sort([('score',-1)])

    def setJianshuUser(self, url, name):
        info = {'url':url, 'name':name}
        self.db.jianshuUser.update({'_id':url}, {'$set': info}, upsert=True)

    def getJianshuUser(self, url):
        return self.db.jianshuUser.find_one({'_id':url})

    def clearAuthorStatistics(self):
        self.db.authorstats.remove()

    def setAuthorStatistics(self, info):
        self.db.authorstats.update({'url':info['url']}, {'$set': info}, upsert=True)

    def insertAuthorStatisticsBulk(self, info, bulklist, bulk=None):
        if bulk is None:
            bulk = self.startUnOrderedBulk(self.db.authorstats)
            bulklist.append(bulk)
        bulk.find({'url':info['url']}).upsert().update({'$set':info})
        return bulk

    def updateAuthorStatisticsBulk(self, info, bulklist, bulk=None):
        if bulk is None:
            bulk = self.startUnOrderedBulk(self.db.authorstats)
            bulklist.append(bulk)
        bulk.find({'url':info['url']}).upsert().update({'$set':info})
        return bulk
    
    def getAuthorStatistics(self, url):
        return self.db.authorstats.find_one({'url':url}, {'_id':0})

    def getTopAuthorStatistics(self, skip, top):
        return self.db.authorstats.find({}, {'_id':0}).sort([('score',-1)]).limit(top).skip(skip)

    def getAuthorStatisticsByScore(self):
        return self.db.authorstats.find({}, {'_id':0}).sort([('score',-1)])

    def getAuthorStatisticsHistory(self, url, date):
        return self.db.authorstathistories.find_one({'url':url, 'date':date}, {'_id':0})

    def getLastAuthorStatisticsHistory(self, url):
        return self.db.authorstathistories.find({'url':url}, {'_id':0}).sort([('date',-1)]).limit(1)

    def setAuthorStatisticsHistory(self, info):
        self.db.authorstathistories.update({'url':info['url'], 'date':info['date']}, {'$set': info}, upsert=True)

    def setAuthorStatisticsHistoryBulk(self, info, bulks, bulk):
        if bulk is None:
            bulk = self.startUnOrderedBulk(self.db.authorstathistories)
            bulks.append(bulk)
        bulk.find({'url':info['url'], 'date':info['date']}).upsert().update({'$set':info})
        return bulk

    def setOperationLog(self, tag, date, duration):
        info = {'_id': date, 'tag':tag, 'duration':duration}
        self.db.oplogs.insert(info)
# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
from jianshu.util.printutil import iprint

class JianShuNovelAnalyser:

    def __init__(self, cache=None):
        self.cache = cache
        self.log_interval = 100
        self.dictAuthor = {}
        if self.cache is not None:
            self.cache.clearAuthorStatistics()

    def sortSummaryByScore(self):
        if self.cache is None:
            return

        iprint('analyzing summary...')
        summaries = self.cache.getAllSummaries()
        now = datetime.now()

        today = datetime(now.year, now.month, now.day)
        onedaybefore = timedelta(days=-1)
        # today = today + onedaybefore
        yestoday = today + onedaybefore

        todaystr = '%d-%d-%d' % (today.year, today.month, today.day)
        yestodaystr = '%d-%d-%d' % (yestoday.year, yestoday.month, yestoday.day)

        self.dictAuthor.clear()

        daily = {'date':todaystr, 'score':0, 'read':0, 'reply':0, 'like':0, 'money':0, 'new':0}

        bulks = []
        sumamaryUpdateBulk = None
        historyBulk = None
        idx = 0
        attrname = ['score', 'read', 'reply', 'like', 'money']
        cnt = summaries.count()
        for line in summaries:
            idx += 1
            if idx % self.log_interval == 0:
                iprint('summary (%d/%d)' % (idx, cnt))

            history = {'key': line['key'], 'score':line['score'], 'read':line['read'], 'reply':line['reply'],\
                 'like':line['like'], 'money':line['money'], 'rank':idx, 'date': todaystr}
            
            days = (today - line['timestamp']).days
            history['days'] = days
            history['sharetime'] = line['timestamp']
            # 计算和昨日比的排名变化
            lastinfos = self.cache.getArticleHistoryInfo(line['key'], yestodaystr)
            if lastinfos.count() > 0:
                last = lastinfos[0]
                rankchg = last['rank'] - idx
                history['rankchg'] = rankchg

                for attr in attrname:
                    diffname = 'diff_' + attr
                    diff = history[attr] - last[attr]
                    history[diffname] = diff
                    daily[attr] += diff
            else:
                
                for attr in attrname:
                    diffname = 'diff_' + attr

                    if history['days'] <= 1:
                        history[diffname] = line[attr]
                        daily['new'] += 1
                    else:
                        history[diffname] = 0

                history['rankchg'] = 0
                rankchg = 0
            line['rankchg'] = history['rankchg']

            if history['diff_read'] > 0:
                # self.cache.setArticleHistoryInfoByLine(history)
                historyBulk = self.cache.updateArticleHistoryInfoByLineBulk(history, bulks, historyBulk)

            #self.cache.setSummaryRankVar(line['key'], line['rankchg'])
            sumamaryUpdateBulk = self.cache.setSummaryRankVarBulk(line['key'], line['rankchg'], bulks, sumamaryUpdateBulk)

            sharetime = line['timestamp']
            author_url = line['author_url']
            author = self.dictAuthor.get(author_url)
            if author is None:
                author = {'url':author_url, 'name': line['author'], 'num_article':0, 'num_read':0, 'num_like':0, \
                    'num_reply':0, 'num_money':0, 'first_time':sharetime, 'days':0}
                self.dictAuthor[author_url] = author
            
            if author is not None:
                author['num_article'] += 1
                if author['first_time'] < sharetime:
                    author['first_time'] = sharetime

        for bulk in bulks:
            self.cache.excuteBulk(bulk)

        self.cache.setJianshuDaily(daily)
        
    def analyseAuthors(self):
        iprint('analyzing author')
        now = datetime.now()
        today = datetime(now.year, now.month, now.day)
        # weekbefore = timedelta(days=-7)
        # today = today + weekbefore

        bulks = []
        # insertbulk = None
        cnt = len(self.dictAuthor)
        idx = 0
        authors = []
        for url, author in self.dictAuthor.items():
            idx += 1
            if idx % self.log_interval == 0:
                iprint('author (%d/%d)' % (idx, cnt))

            writedelta = now - author['first_time']
            days = writedelta.days
            author['days'] = days

            # 只计算最好3部作品的得分，否则容易被臭作拖累
            art_num = 3
            bestArts = self.cache.getBestSummaryByAuthorUrl(author['url'], art_num)
            for art in bestArts:
                author['num_read'] += art['read']
                author['num_like'] += art['like']
                author['num_reply'] += art['reply']
                author['num_money'] += art['money']

            author['score'] = int(( author['num_like'] * 5 + author['num_reply'] * 10 + author['num_money'] * 100) / art_num)
            author['date'] = today

            #self.cache.setAuthorStatistics(author)
            # insertbulk = self.cache.insertAuthorStatisticsBulk(author, bulks, insertbulk)
            authors.append(author)
        
        # for bulk in bulks:
        #     self.cache.excuteBulk(bulk)
        # bulks.clear()

        updateBulk = None
        historyBulk = None
        # allAuthors = self.cache.getAuthorStatisticsByScore()
        authors.sort(key=lambda obj:obj.get('score'), reverse=True)
        rank = 0
        cnt = len(authors)
        for rankedauthor in authors:
            rank += 1
            # iprint('%s score[%d]' % (rankedauthor['name'], rankedauthor['score']))
            if rank % self.log_interval == 0:
                iprint('author rank (%d/%d)' % (rank, cnt))

            rankedauthor['rank'] = rank
            rankedauthor['new'] = 0
            lastrecords = self.cache.getLastAuthorStatisticsHistory(rankedauthor['url'])
            if lastrecords.count() < 1:
                rankedauthor['rankvar'] = 0
                rankedauthor['new'] = 1
                # self.cache.setAuthorStatisticsHistory(rankedauthor)
                historyBulk = self.cache.setAuthorStatisticsHistoryBulk(rankedauthor, bulks, historyBulk)
            else:
                lastrecord = lastrecords[0]
                # iprint('currank[%d] lastrank[%d] rankvar[%d]' % (rankedauthor['rank'], lastrecord['rank'], lastrecord['rank'] - rankedauthor['rank']))
                past = rankedauthor['date'] - lastrecord['date']
                dayspast = past.days
                if dayspast >= 7:
                    rankedauthor['rankvar'] = lastrecord['rank'] - rankedauthor['rank']
                    # self.cache.setAuthorStatisticsHistory(rankedauthor)
                    historyBulk = self.cache.setAuthorStatisticsHistoryBulk(rankedauthor, bulks, historyBulk)
                elif dayspast == 0:
                    lastrecord['score'] = rankedauthor['score']
                    #self.cache.setAuthorStatisticsHistory(lastrecord)
                    historyBulk = self.cache.setAuthorStatisticsHistoryBulk(lastrecord, bulks, historyBulk)
                else:
                    rankedauthor['rankvar'] = lastrecord['rank'] - rankedauthor['rank']
            # self.cache.setAuthorStatistics(rankedauthor)
            updateBulk = self.cache.updateAuthorStatisticsBulk(rankedauthor, bulks, updateBulk)

        for bulk in bulks:
            self.cache.excuteBulk(bulk)
        self.dictAuthor.clear()
            
    def analyseArticleHistoryDiff(self):
        articles = self.cache.getAllArticleBaseInfo()
        attrname = ['score', 'read', 'reply', 'like', 'money']

        for art in articles:
            histories = self.cache.getArticleHistoryInfoByKey(art['key'])
            for his in histories:
                #if 'diff_read' not in his:

                curdate = datetime.strptime(his['date'], '%Y-%m-%d')
                onedaybefore = timedelta(days=-1)
                yestoday = curdate + onedaybefore
                yestodaystr = '%d-%d-%d' % (yestoday.year, yestoday.month, yestoday.day)

                if 'sharetime'in his.keys():
                    days = (curdate - his['sharetime']).days
                else:
                    days = self.cache.getArticleDaysByKey(his['key'], curdate)

                his['days'] = days
                lastinfos = self.cache.getArticleHistoryInfo(his['key'], yestodaystr)
                if lastinfos.count() > 0:
                    last = lastinfos[0]
                    iprint('gen diff [%s]' % art['title'])
                    for attr in attrname:
                        diffname = 'diff_' + attr
                        his[diffname] = his[attr] - last[attr]
                else:
                    for attr in attrname:
                        diffname = 'diff_' + attr
                        if his['days'] <= 1:
                            his[diffname] = his[attr]
                        else:
                            his[diffname] = 0

                if history['diff_read'] > 0:        
                    self.cache.setArticleHistoryInfoByLine(his)

    def analyseJianshuDaily(self):
        now = datetime.now()
        end = datetime(now.year, now.month, now.day)
        curtime = begin = datetime(2017, 6, 1)
        oneafter = timedelta(days=1)
        gap = end - begin
        gapdays = gap.days

        attrname = ['score', 'read', 'reply', 'like', 'money']
        for i in range(0, gapdays):
            yestoday = curtime
            yestodaystr = '%d-%d-%d' % (yestoday.year, yestoday.month, yestoday.day)

            curtime += oneafter
            curstr = '%d-%d-%d' % (curtime.year, curtime.month, curtime.day)
            daily = {'date':curstr, 'score':0, 'read':0, 'reply':0, 'like':0, 'money':0, 'new':0}

            hasdaily = False
            histories = self.cache.getArticleHistoryInfoByDate(curstr)
            for his in histories:
                lastinfos = self.cache.getArticleHistoryInfo(his['key'], yestodaystr)
                if lastinfos.count() > 0:
                    last = lastinfos[0]
                    hasdaily = True
                    for attr in attrname:
                        diff = his[attr] - last[attr]
                        daily[attr] += diff
                elif his['days'] <= 1:
                    daily['new'] += 1

            if hasdaily is True:
                self.cache.setJianshuDaily(daily)
                iprint('get daily for %s' % curstr)
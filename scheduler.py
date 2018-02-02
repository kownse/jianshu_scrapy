#! /usr/bin/env python
#coding=utf-8

from scrapy.cmdline import execute
from analyse import analyse_jianshu
from jianshu.util.timer import timer_func
import os
import sched
import time
from datetime import datetime, timedelta
from jianshu.util.printutil import iprint

schedule = sched.scheduler(time.time, time.sleep)

def crawlAnalyse():
    timer_func('dl_title', os.system, 'python3 -m scrapy crawl novel')
    timer_func('analyse_all', analyse_jianshu)
    timer_func('gen_html', os.system, "../../robot/shell/genhtml.sh")
    timer_func('dl_articles', os.system, 'python3 -m scrapy crawl article')

def perform_crawl(hour, min):
    now = datetime.now()
    onedayafter = timedelta(days=1)
    tmp = now + onedayafter
    tormorrow = datetime(tmp.year, tmp.month, tmp.day, hour, min)
    tonext = tormorrow - now
    inc = tonext.seconds + tonext.days * 86400
    schedule.enter(inc, 0, perform_crawl, (hour, min))

    crawlAnalyse()

    iprint('next crawl in %d secs' % inc)
    iprint(tormorrow)

def timing_crawl():
    # 第一次是在当天的23:50
    now = datetime.now()
    tormorrow = datetime(now.year, now.month, now.day, 10, 00)
    tonext = tormorrow - now
    sec = tonext.seconds

    schedule.enter(sec, 0, perform_crawl, (10, 00))
    crawlAnalyse()

    iprint('next crawl in %d secs' % sec)
    iprint(tormorrow)

    schedule.run()

if __name__ == "__main__":
    timing_crawl()
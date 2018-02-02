from datetime import datetime, timedelta
from jianshu.util.printutil import iprint
from jianshu.util.MongoCache import MongoCache

def timer_func(tag, func, param=None):
    cache = MongoCache()
    begin = datetime.now()

    iprint('%s start at %s' % (tag, begin))

    if param is not None:
        func(param)
    else:
        func()

    end = datetime.now()
    last = end - begin
    iprint('%s takes %d secs, end at %s' % (tag, last.seconds, end))
    cache.setOperationLog(tag, begin, last.seconds)
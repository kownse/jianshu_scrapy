from scrapy.cmdline import execute
#from analyse import analyse_jianshu
#from jianshu.util.timer import timer_func
import os

# execute(['scrapy','crawl', 'novel'])
print(os.getcwd())
execute(['scrapy','crawl', 'novel'])
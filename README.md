# jianshu_scrapy

## What is this
Crawl short fictions on jianshu.com everyday using scrapy + mongodb + schedule.
 1. It is based on scrapy framework.
 2. It uses mongodb as database.
 3. It uses python schedule to simplely follow a daily routine.
 4. It has been configed friendly to web server since 1 request per second.
 5. It is written in python 3.x.

## Get Started
 To get started, you need:
 1. A running mongod service on your local(127.0.0.1)
 2. Change the username and password in /domain/MongoCache.py to fit your own case.
 3. For one time crawling, just run: 
  ```shell
  python3 runner.py
  ```
 4.For scheduled crawling:
  open scheduler.py, scroll to the bottom, change parameters in the call to timing_crawl
  ```python
  if __name__ == "__main__":
     timing_crawl(hour, min)
  ```
  Then,
  ```shell
  python3 scheduler.py
  ```

## About timing
Every crawling takes up about **1 hour** since there are average 4000+ fictions on jianshu.com.

## For what
 - This project helps to get native grassroots novels from China for reading interest or learning Chinese.
 - it is good data source for data science studying.
 
## Analysis
 - A summary analysis based on this work can be [visited here](https://kownse.github.io/jianshurank.html)
 - For more detailed analysis results, please contact me by [email](mailto:kownse@gmail.com).

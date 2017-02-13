#!/usr/bin/python3

import scrapy
import logging
import urllib.parse
import dateutil.parser
import pytz
from pymongo import MongoClient

logger_level = logging.DEBUG
logger_formatter = "[%(asctime)s] %(levelname)s :: %(message)s"
logging.basicConfig(level=logger_level, format=logger_formatter)

headers = {
    # "Accept-Encoding": "gzip, deflate, sdch",
    # "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    # "Accept-Language": "en",
    # "Connection": "keep-alive",
    # "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.97 Safari/537.36",
}

hostname = "tieba.baidu.com"
sleeptime = 60

timezone = pytz.timezone("Asia/Shanghai")


# You should not want to run this around 12:00 p.m.
def format_time(time, oldtime=None):
    return timezone.localize(dateutil.parser.parse(time))


## thread_item perprorities
## {"id":4303087230,"author_name":"L\u5bfb\u5b88\u62a4\u661f","first_post_id":82410201113,"reply_num":275,"is_bakan":null,"vid":"","is_good":null,"is_top":null,"is_protal":null,"is_membertop":null}
## id, title, author_id, author_name, first_post_id, create_time, reply_num, last_reply_time, last_reply_author, desc, today_top_num, top_num
## reply_num_history (threshold)
class ThreadItem:
    def __init__(self, id, title, catalog='Unknown', author='', create_time='', last_reply_time='', reply_num=-1,
                 desc=''):
        self.id = id
        self.title = title
        self.catalog = catalog
        self.author = author
        self.create_time = create_time
        self.last_reply_time = last_reply_time
        self.reply_num = int(reply_num)
        self.desc = desc

    def __str__(self):
        params = "url='%s', title='%s'" % (self.id, self.title)
        if self.catalog:
            params += ", catalog='%s'" % self.catalog
        if self.author:
            params += ", author='%s'" % self.author
        if self.create_time:
            params += ", create=%s" % self.create_time
        if self.last_reply_time:
            params += ", last_reply=%s" % self.last_reply_time
        if self.reply_num != -1:
            params += ", reply=%d" % self.reply_num
        if self.desc:
            params += ", desc='%s'" % self.desc
        return "ThreadItem(%s)" % params

    def __repr__(self):
        from pprint import pformat
        return pformat(vars(self), indent=4, width=1)


def find_class_text(selector, classname, tag=''):
    return selector.css('%s.%s' % (tag, classname)).xpath('.//text()').extract_first()


def get_attr_value(selector, attr):
    return selector.xpath('@%s' % attr).extract_first()


class TiebaSpider(scrapy.Spider):
    name = "tieba"

    db = MongoClient('localhost', 3269).test

    def start_requests(self):
        yield scrapy.Request("http://" + hostname + '/f?%s' % urllib.parse.urlencode({'kw': "双梦镇"}))

    def parse(self, response):
        thread_list = response.xpath('//*[@id="thread_list"]/li')
        l = []
        for s in thread_list:
            try:
                attrs = s.css('a.j_th_tit')
                x = ThreadItem(get_attr_value(attrs, 'href'), get_attr_value(attrs, 'title'))
                x.author = find_class_text(s, "tb_icon_author", tag="span")
                x.create_time = format_time(find_class_text(s, "is_show_create_time"))
                x.last_reply_time = format_time(find_class_text(s, "threadlist_reply_date"))
                x.reply_num = int(find_class_text(s, "threadlist_rep_num"))
                x.desc = find_class_text(s, "threadlist_abs", tag="div").strip()
                # logging.log(4, "thread_title: %s", title)
                # logging.log(4, "author: %s", s.find("span", **{"class": "tb_icon_author"}).text.strip())
                # logging.log(4, "create_time: %s", s.find("span", **{"class": "is_show_create_time"}).text.strip())
                # logging.log(4, "last_reply_time: %s", s.find("span", **{"class": "threadlist_reply_date"}).text.strip())
                # print(x)
                l.append(x)
            except Exception as e:
                if len(s.css(".icon-top")) != 0:
                    logging.log(5, "tieba top_list_folder found")
                elif len(s.css(".j_click_stats")) != 0:
                    logging.log(5, "tieba j_click_stats found")
                else:
                    logging.warning("exception: %s", e)
                    logging.warning("broken thread: %s", s)
                continue

        for x in l:
            self.db.documents.update(
                {'name': x.id},
                {
                    '$set': {
                        'title': x.title,
                        'author': x.author,
                        'desc': x.desc,
                        # 'catalog': x.catalog,
                    },
                    '$setOnInsert': {
                        'name': x.id,
                        'createtime': x.create_time,
                    }
                },
                upsert=True
            )
            self.db.head.insert_one({
                'name': x.id,
                'updatetime': x.last_reply_time,
                'reply': x.reply_num,
            })
        print([(x.id, x.reply_num) for x in l])
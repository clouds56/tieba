#!/usr/bin/python3

import html2text
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
class ThreadItem(scrapy.Item):
    id = scrapy.Field()
    title = scrapy.Field()
    #catalog = scrapy.Field()
    author = scrapy.Field()
    create_time = scrapy.Field()
    last_reply_time = scrapy.Field()
    reply_num = scrapy.Field()
    desc = scrapy.Field()


class ReplyItem(scrapy.Item):
    id = scrapy.Field()
    thread_id = scrapy.Field()
    author = scrapy.Field()
    author_level = scrapy.Field()
    text = scrapy.Field()
    reply_time = scrapy.Field()
    reply_reply_num = scrapy.Field()


def find_class_text(selector, classname, tag=''):
    return selector.css('%s.%s' % (tag, classname)).xpath('.//text()').extract_first()


def get_attr_value(selector, attr):
    return selector.xpath('@%s' % attr).extract_first()


class TiebaSpider(scrapy.Spider):
    name = "tieba"

    db = MongoClient('localhost', 3269).test
    ht = html2text.HTML2Text()

    custom_settings = {
        'DOWNLOAD_DELAY': 0.25,
    }

    start_urls = ["http://" + hostname + '/f?%s' % urllib.parse.urlencode({'kw': "双梦镇"}), ]

    def parse(self, response):
        thread_list = response.xpath('//*[@id="thread_list"]/li')
        l = []
        for s in thread_list:
            try:
                item = ThreadItem()
                attrs = s.css('a.j_th_tit')
                item['id'] = get_attr_value(attrs, 'href')
                item['title'] = get_attr_value(attrs, 'title')
                item['author'] = find_class_text(s, "tb_icon_author", tag="span")
                item['create_time'] = format_time(find_class_text(s, "is_show_create_time"))
                item['last_reply_time'] = format_time(find_class_text(s, "threadlist_reply_date"))
                item['reply_num'] = int(find_class_text(s, "threadlist_rep_num"))
                item['desc'] = find_class_text(s, "threadlist_abs", tag="div").strip()
                # logging.log(4, "thread_title: %s", title)
                # logging.log(4, "author: %s", s.find("span", **{"class": "tb_icon_author"}).text.strip())
                # logging.log(4, "create_time: %s", s.find("span", **{"class": "is_show_create_time"}).text.strip())
                # logging.log(4, "last_reply_time: %s", s.find("span", **{"class": "threadlist_reply_date"}).text.strip())
                print(dict(item))
                yield scrapy.Request(urllib.parse.urljoin(response.url, item['id']), self.parse_thread)
                yield item
                l.append(item)
            except Exception as e:
                if len(s.css(".icon-top")) != 0:
                    logging.log(5, "tieba top_list_folder found")
                elif len(s.css(".j_click_stats")) != 0:
                    logging.log(5, "tieba j_click_stats found")
                else:
                    logging.warning("exception: %s", e)
                    logging.warning("broken thread: %s", s)
                continue

        print([(x['id'], x['reply_num']) for x in l])

    def parse_thread(self, response):
        for x in response.xpath('//*[@id="j_p_postlist"]/div'):
            try:
                item = ReplyItem()
                item['thread_id'] = urllib.parse.urlsplit(response.url).path
                item['id'] = x.css('div.p_reply').xpath('@data-field').re_first('"pid":(.*?),')
                item['author'] = x.css('.d_name a::text').extract_first()
                item['author_level'] = x.css('.d_badge_lv::text').extract_first()
                item['text'] = self.ht.handle(x.css('div.d_post_content').extract_first()).strip('\n')
                item['reply_time'] = x.css('.tail-info')[-1].xpath('text()').extract_first()
                item['reply_reply_num'] = x.css('div.p_reply').xpath('@data-field').re_first('"total_num":(\d+)')
                yield item
            except Exception as e:
                logging.warning("exception: %s", e)
                continue

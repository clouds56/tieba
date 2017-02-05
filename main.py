#!/usr/bin/python3

import http.client
import socket
import logging
import urllib.parse
import dateutil.parser
import pytz
from time import sleep
from datetime import datetime
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from pprint import pprint
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


def urlsplit(url):
    s = url.split("://", 1)
    if len(s) == 1 and url[:2] == "//":
        s = url.split("//", 1)
    h = s[1].split("/", 1)
    if len(h) == 1:
        return h[0], ''
    else:
        return h[0], '/%s' % h[1]


def get(conn, url):
    try:
        logging.info("get: %s", url)
        conn.request("GET", url, headers=headers)
        res = conn.getresponse()
        logging.debug("response: %d %s", res.status, res.reason)
        if res.status == 200:
            return res.read().decode()
        elif res.status >= 400:
            logging.warning("%s not found", url)
        elif 300 <= res.status < 400:
            loc = res.getheader('location')
            logging.debug("redirect to: %s", loc)
            h, loc = urlsplit(loc)
            logging.debug("split loc to: %s, %s", h, loc)
            if h != hostname:
                logging.warning("hostname change: %s", h)
                return
            if loc == url:
                logging.warning("url loop: %s", loc)
                return
            return get(conn, loc)
        elif 200 < res.status < 300:
            logging.warning("%s 2xx status", url)
        else:
            logging.warning("%s unknown status", url)
    finally:
        conn.close()


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


def find_class_text(soup, classname, tag="span"):
    s = soup.find(tag, **{"class": classname})
    if s:
        return s.text.strip()
    else:
        return ''


def parse_list(html):
    soup = BeautifulSoup(html, "html.parser")
    logging.debug("parse_list: %s", html[:50].encode())
    catalog = soup.title.text.strip()
    if catalog[-6:] == "吧_百度贴吧":
        catalog = catalog[:-6]
    logging.debug("title: %s -> %s", soup.title.text.strip(), catalog)
    thread_list = soup.find(id="thread_list").findAll("li", recursive=False)
    l = []
    for s in thread_list:
        try:
            attrs = s.find("a", **{"class": "j_th_tit"}).attrs
            x = ThreadItem(attrs['href'], attrs['title'], catalog=catalog)
            x.author = find_class_text(s, "tb_icon_author")
            x.create_time = format_time(find_class_text(s, "is_show_create_time"))
            x.last_reply_time = format_time(find_class_text(s, "threadlist_reply_date"))
            x.reply_num = int(find_class_text(s, "threadlist_rep_num"))
            x.desc = find_class_text(s, "threadlist_abs", tag="div")
            # logging.log(4, "thread_title: %s", title)
            # logging.log(4, "author: %s", s.find("span", **{"class": "tb_icon_author"}).text.strip())
            # logging.log(4, "create_time: %s", s.find("span", **{"class": "is_show_create_time"}).text.strip())
            # logging.log(4, "last_reply_time: %s", s.find("span", **{"class": "threadlist_reply_date"}).text.strip())
            logging.log(5, "%s", x)
            l.append(x)
        except Exception as e:
            if s.find(text="贴吧游戏"):
                logging.log(5, "tieba game thread found")
            elif s.find(title="置顶"):
                logging.log(5, "tieba top_list_folder found")
            elif s.find(title="推广"):
                logging.log(5, "tieba j_click_stats found")
            else:
                logging.warning("exception: %s", e)
                logging.warning("broken thread: %s", s)
            continue
    return l


def main():
    db = MongoClient('localhost', 3269).test
    conn = http.client.HTTPConnection(hostname, timeout=10)
    # html = open("example.html").read()
    while True:
        try:
            logging.debug("new iteration")
            html = get(conn, '/f?%s' % urllib.parse.urlencode({'kw': "双梦镇"}))
            l = parse_list(html)
            for x in l:
                db.documents.update(
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
                db.head.insert_one({
                    'name': x.id,
                    'updatetime': x.last_reply_time,
                    'reply': x.reply_num,
                })
            print([(x.id, x.reply_num) for x in l])
        except http.client.HTTPException as e:
            logging.warning("%s: %s", (type(e), e))
        except socket.timeout as e:
            logging.warning("%s: %s", (type(e), e))
        except KeyboardInterrupt:
            logging.warning("Ctrl+C pressed")
            break
        except Exception as e:
            logging.error("Unexpected exception %s: %s", (type(e), e))
            break
        finally:
            conn.close()
            logging.debug("sleep for %d seconds", sleeptime)
            sleep(sleeptime)


if __name__ == '__main__':
    main()

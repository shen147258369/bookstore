# coding=utf-8

from lxml import etree
import re
import requests
import random
import time
import logging
from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError
from bson.binary import Binary
from typing import Tuple, List, Dict, Any

user_agent = [
    "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 "
    "Safari/534.50",
    "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 "
    "Safari/534.50",
    "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:38.0) Gecko/20100101 Firefox/38.0",
    "Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; .NET4.0C; .NET4.0E; .NET CLR 2.0.50727; .NET CLR "
    "3.0.30729; .NET CLR 3.5.30729; InfoPath.3; rv:11.0) like Gecko",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)",
    "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0)",
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1",
    "Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1",
    "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11",
    "Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 "
    "Safari/535.11",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Maxthon 2.0)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; TencentTraveler 4.0)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; The World)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SE 2.X MetaSr 1.0; SE 2.X MetaSr 1.0; .NET "
    "CLR 2.0.50727; SE 2.X MetaSr 1.0)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; 360SE)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Avant Browser)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)",
    "Mozilla/5.0 (iPhone; U; CPU iPhone OS 4_3_3 like Mac OS X; en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) "
    "Version/5.0.2 Mobile/8J2 Safari/6533.18.5",
    "Mozilla/5.0 (iPod; U; CPU iPhone OS 4_3_3 like Mac OS X; en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) "
    "Version/5.0.2 Mobile/8J2 Safari/6533.18.5",
    "Mozilla/5.0 (iPad; U; CPU OS 4_3_3 like Mac OS X; en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) "
    "Version/5.0.2 Mobile/8J2 Safari/6533.18.5",
    "Mozilla/5.0 (Linux; U; Android 2.3.7; en-us; Nexus One Build/FRF91) AppleWebKit/533.1 (KHTML, like Gecko) "
    "Version/4.0 Mobile Safari/533.1",
    "MQQBrowser/26 Mozilla/5.0 (Linux; U; Android 2.3.7; zh-cn; MB200 Build/GRJ22; CyanogenMod-7) "
    "AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1",
    "Opera/9.80 (Android 2.3.4; Linux; Opera Mobi/build-1107180945; U; en-GB) Presto/2.8.149 Version/11.10",
    "Mozilla/5.0 (Linux; U; Android 3.0; en-us; Xoom Build/HRI39) AppleWebKit/534.13 (KHTML, like Gecko) "
    "Version/4.0 Safari/534.13",
    "Mozilla/5.0 (BlackBerry; U; BlackBerry 9800; en) AppleWebKit/534.1+ (KHTML, like Gecko) Version/6.0.0.337 "
    "Mobile Safari/534.1+",
    "Mozilla/5.0 (hp-tablet; Linux; hpwOS/3.0.0; U; en-US) AppleWebKit/534.6 (KHTML, like Gecko) "
    "wOSBrowser/233.70 Safari/534.6 TouchPad/1.0",
    "Mozilla/5.0 (SymbianOS/9.4; Series60/5.0 NokiaN97-1/20.0.019; Profile/MIDP-2.1 Configuration/CLDC-1.1) "
    "AppleWebKit/525 (KHTML, like Gecko) BrowserNG/7.1.18124",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows Phone OS 7.5; Trident/5.0; IEMobile/9.0; HTC; Titan)",
    "UCWEB7.0.2.37/28/999",
    "NOKIA5700/ UCWEB7.0.2.37/28/999",
    "Openwave/ UCWEB7.0.2.37/28/999",
    "Mozilla/4.0 (compatible; MSIE 6.0; ) Opera/UCWEB7.0.2.37/28/999",
    # iPhone 6：
    "Mozilla/6.0 (iPhone; CPU iPhone OS 8_0 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) Version/8.0 "
    "Mobile/10A5376e Safari/8536.25",
]


def get_user_agent():
    headers = {"User-Agent": random.choice(user_agent)}
    return headers


class Scraper:
    def __init__(self):
        # MongoDB连接配置
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client['bookstore']
        
        # 定义集合
        self.tags_col = self.db['tags']
        self.books_col = self.db['books']
        self.progress_col = self.db['progress']
        
        # 初始化集合
        self._init_collections()
        
        self.pattern_number = re.compile(r"\d+\.?\d*")
        logging.basicConfig(filename="scraper.log", level=logging.ERROR)

    def _init_collections(self) -> None:
        """初始化集合和索引"""
        # 创建索引（幂等操作）
        self.books_col.create_index("id", unique=True)
        self.tags_col.create_index("tag", unique=True)
        self.progress_col.create_index("_id", unique=True)

        # 初始化进度文档
        if not self.progress_col.find_one({"_id": "global"}):
            self.progress_col.insert_one({
                "_id": "global",
                "current_tag": "",
                "current_page": 0
            })

    def get_current_progress(self) -> Tuple[str, int]:
        doc = self.progress_col.find_one({"_id": "global"})
        return doc.get("current_tag", ""), doc.get("current_page", 0)

    def save_current_progress(self, current_tag: str, current_page: int) -> None:
        self.progress_col.update_one(
            {"_id": "global"},
            {"$set": {
                "current_tag": current_tag,
                "current_page": current_page
            }},
            upsert=True
        )

    def start_grab(self) -> bool:
        self.grab_tag()
        current_tag, current_page = self.get_current_progress()
        tags = self.get_tag_list()
        for i in range(0, len(tags)):
            no = 0
            if i == 0 and current_tag == tags[i]:
                no = current_page
            while self.grab_book_list(tags[i], no):
                no = no + 20
        return True

    # def create_tables(self):
    #     conn = sqlite3.connect(self.database)
    #     try:
    #         conn.execute("CREATE TABLE tags (tag TEXT PRIMARY KEY)")
    #         conn.commit()
    #     except sqlite3.Error as e:
    #         logging.error(str(e))
    #         conn.rollback()

    #     try:
    #         conn.execute(
    #             "CREATE TABLE book ("
    #             "id TEXT PRIMARY KEY, title TEXT, author TEXT, "
    #             "publisher TEXT, original_title TEXT, "
    #             "translator TEXT, pub_year TEXT, pages INTEGER, "
    #             "price INTEGER, currency_unit TEXT, binding TEXT, "
    #             "isbn TEXT, author_intro TEXT, book_intro text, "
    #             "content TEXT, tags TEXT, picture BLOB)"
    #         )
    #         conn.commit()
    #     except sqlite3.Error as e:
    #         logging.error(str(e))
    #         conn.rollback()

    #     try:
    #         conn.execute(
    #             "CREATE TABLE progress (id TEXT PRIMARY KEY, tag TEXT, page integer )"
    #         )
    #         conn.execute("INSERT INTO progress values('0', '', 0)")
    #         conn.commit()
    #     except sqlite3.Error as e:
    #         logging.error(str(e))
    #         conn.rollback()

    def grab_tag(self) -> bool:
        url = "https://book.douban.com/tag/?view=cloud"
        r = requests.get(url, headers=get_user_agent())
        h: etree.Element = etree.HTML(r.text)
        
        tags: List[str] = h.xpath('//td/a/@href')  # 类型标注修正
        
        bulk_ops = []
        for tag in tags:
            t = tag.strip("/tag")
            bulk_ops.append(
                UpdateOne(
                    {"tag": t},
                    {"$setOnInsert": {"tag": t}},
                    upsert=True
                )
            )
        try:
            if bulk_ops:
                self.tags_col.bulk_write(bulk_ops)
            return True
        except PyMongoError as e:
            logging.error(str(e))
            return False

    def grab_book_list(self, tag: str = "小说", pageno: int = 1) -> bool:
        url = f"https://book.douban.com/tag/{tag}?start={(pageno - 1) * 20}&type=T"
        r = requests.get(url, headers=get_user_agent())
        h: etree.Element = etree.HTML(r.text)
        book_ids = h.xpath('//li[@class="subject-item"]/div[@class="pic"]/a/@href')
        book_ids = [re.search(r"/subject/(\d+)/", id).group(1) for id in book_ids if re.search(r"/subject/(\d+)/", id)]
        for book_id in book_ids:
            self.crow_book_info(book_id)
        next_page = h.xpath('//span[@class="next"]/a')
        has_next = bool(next_page)
        return has_next

    def get_tag_list(self) -> List[str]:
        """获取待处理的标签列表"""
        pipeline = [
            {"$match": {"tag": {"$gte": "$current_tag"}}},
            {"$sort": {"tag": 1}},
            {"$project": {"_id": 0, "tag": 1}}
        ]
        results = self.tags_col.aggregate(pipeline)
        return [doc["tag"] for doc in results]

    def crow_book_info(self, book_id: str) -> bool:
        if self.books_col.find_one({"id": book_id}):
            return True

        url = f"https://book.douban.com/subject/{book_id}/"
        r = requests.get(url, headers=get_user_agent())
        h: etree.Element = etree.HTML(r.text)

        title = h.xpath('//span[@property="v:itemreviewed"]/text()')[0]
        book_info_nodes = h.xpath('//div[@id="info"]/text()')
        book_info = {}
        for line in book_info_nodes:
            if ":" in line:
                key, value = line.split(":", 1)
                book_info[key.strip()] = value.strip()

        pages = h.xpath('//span[@property="v:pages"]/text()')
        pages = int(pages[0]) if pages else 0

        price_text = h.xpath('//span[@property="v:price"]/text()')
        price_text = price_text[0] if price_text else ""
        match = self.pattern_number.search(price_text)
        price = float(match.group()) if match else 0
        unit = price_text.replace(str(price), "")

        author_intro = h.xpath('//div[@class="intro"]/p/text()')
        author_intro = "\n".join(author_intro) if author_intro else ""

        book_intro = h.xpath('//div[@class="related_info"]/div[@class="intro"]/p/text()')
        book_intro = "\n".join(book_intro) if book_intro else ""

        content = h.xpath('//div[@class="related_info"]/div[@class="indent"]/div[@class="intro"]/p/text()')
        content = "\n".join(content) if content else ""

        tags = h.xpath('//div[@id="db-tags-section"]/div[@class="indent"]/span/a/text()')
        tags = " ".join(tags)

        picture = h.xpath('//a[@class="nbg"]/img/@src')
        picture = requests.get(picture[0]).content if picture else None

        # 构建MongoDB文档
        book_doc: Dict[str, Any] = {
            "id": book_id,
            "title": title,
            "author": book_info.get("作者"),
            "publisher": book_info.get("出版社"),
            "original_title": book_info.get("原作名"),
            "translator": book_info.get("译者"),
            "pub_year": book_info.get("出版年"),
            "pages": pages,
            "price": price,
            "currency_unit": unit,
            "binding": book_info.get("装帧"),
            "isbn": book_info.get("ISBN"),
            "author_intro": author_intro,
            "book_intro": book_intro,
            "content": content,
            "tags": [t.strip() for t in tags.split('\n') if t.strip()],
            "pictures": [Binary(picture)] if picture else []
        }

        try:
            self.books_col.update_one(
                {"id": book_id},
                {"$setOnInsert": book_doc},
                upsert=True
            )
            return True
        except PyMongoError as e:
            logging.error(f"Insert failed: {str(e)}")
            return False

if __name__ == "__main__":
    scraper = Scraper()
    scraper.start_grab()
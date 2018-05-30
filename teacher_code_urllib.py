import hashlib
import pickle
import re
import ssl
import zlib
from urllib.error import URLError
from urllib.request import urlopen
import pymysql
import logging

from redis import Redis

# 通过指定的字符集对页面进行解码（不是每个网站都将字符集设置为utf-8）
def decode_page(page_byte,charsets=('utf-8',)):
    html = None
    for charset in charsets:
        try:
            html = page_byte.decode(charset)
            break
        except UnicodeDecodeError as e:
            print("UnicodeDecodeError: ",e)
            logging.error('[Decode]',e)
    return html

# 获取页面的html代码（通过递归实现指定次数的重试操作）
def get_page_html(root_html,*,retry_times=3,charsets=('utf-8',)):
    html = None
    try:
        html = decode_page(urlopen(root_html).read(),charsets)
    except URLError as e:
        logging.error('[URL]',e)
        print("URLError:",e)
        if retry_times > 0:
            get_page_html(root_html,retry_times=retry_times-1, charsets=charsets)
    return html

# 从页面中提取需要的部分（通常链接也可以通过正则表达式进行指定）
def get_match_part(html,pattern_str,pattern_ignore_case=re.I):
    pattern_regex = re.compile(pattern_str,pattern_ignore_case)
    return pattern_regex.findall(html) if html else []

# 开始执行爬虫程序并对指定数据进行持久化操作
def crawler(root_html,match_pattern,*,max_depth=-1):
    # -------- mysql -------------
    conn = pymysql.connect(
        host='localhost',
        port=3306,
        database='crawler',
        user='root',
        password='5201314',
        charset='utf8'
    )
    # -------------- redis -------------------
    # client = Redis(host='47.106.134.92', port=6379, password='5201314')
    try:
        with conn.cursor() as cursor:
            url_list = [root_html]
            visited_list = {root_html:0}
            while url_list:
                current_url = url_list.pop(0)
                depth = visited_list[current_url]
                if depth != max_depth:
                    html = get_page_html(current_url,charsets=('utf-8','gbk','gb2312','ascii'))
                    if html:
                        link_list = get_match_part(html,match_pattern)
                        param_list = []
                        for link in link_list:
                            if link not in visited_list and (link.startswith("http://") or link.startswith("https://")):
                                visited_list[link] = depth + 1
                                html = get_page_html(link,charsets=('utf-8','gbk','gb2312','ascii'))
                                if html:
                                    print(html)
                                    headings = get_match_part(html,r"^<h1>(.*)<span")
                                    # if headings:
                                    # 给url加摘要
                                    hasher = hashlib.md5()
                                    hasher.update(link.encode('utf-8')) # 字符处理成字节  pickle -- 字节序列 json -- 字符序列
                                    # 序列化网页数据，并将其压缩
                                    # pickle.dumps -- 先将页面序列化
                                    # zlib -- 再对数据进行压缩
                                    # zlib.decompress -- 解压缩
                                    # pickle.loads -- 反序列化
                                    zlib_html = zlib.compress(pickle.dumps(html))
                                    # 将压缩好的数据存到redis中
                                    # client.set(hasher.hexdigest(),zlib_html)
                                    param_list.append((link,hasher.hexdigest(),zlib_html))
                        cursor.executemany('insert into tb_result(rurl,rdigest,rpage) values(%s,%s,%s)',
                                           param_list)
                        conn.commit()


    except Exception as e:
        print("Exception: ",e)
    finally:
        conn.close()





def main():
    # SSL相关问题。在使用urlopen打开一个HTTPS链接时会验证一次SSL证书，
    # 如果不做出处理会产生错误提示“SSL: CERTIFICATE_VERIFY_FAILED”，
    # 可以通过设置全局的取消证书验证的方式解决问题
    ssl._create_default_https_context = ssl._create_unverified_context
    crawler('http://sports.sohu.com/nba_a.shtml',
            r'<a[^>]+test=a\s[^>]*href=["\'](.*?)["\']',
            max_depth=2)

if __name__ == '__main__':
    main()
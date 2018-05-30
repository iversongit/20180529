import re
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

def main():
    headers = {'user-agent':'Baiduspider'} # 设置请求头
    # 隐藏身份,设置代理服务器
    proxies = {
        'http': 'http://122.114.31.177:808',  # 不安全
        # 'https': '115.203.177.114:30636', # 安全
    }
    base_url  = 'https://www.zhihu.com/' # 站点根路径
    seed_url = urljoin(base_url,'explore')
    resp = requests.get(seed_url,
                        headers=headers,
                        proxies=proxies)
    # print(resp.text) # 知乎用utf-8编码 故不解码
    soup = BeautifulSoup(resp.text,'lxml')
    href_regex = re.compile(r'^/question')
    link_set = set()
    for a_tag in soup.find_all('a',{'href':href_regex}):
        if 'href' in a_tag.attrs:
            href = a_tag.attrs['href']
            full_url = urljoin(base_url,href) # 可以自动比较url，本来就完整便不用拼接
            link_set.add(full_url)

    print(len(link_set))
    print(link_set)

if __name__ == '__main__':
    main()
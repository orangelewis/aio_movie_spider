# -*- coding: utf-8 -*-
__author__ = 'CL'
__date__ = '2019/3/14 6:27 PM'

import aiohttp
import asyncio
import re
import aiomysql
from bs4 import BeautifulSoup
from urllib.parse import quote
from fake_useragent import UserAgent
from spider.xici_ip_pool import GetIP
import spider.aio_db as aio_db
import spider.setting as setting

ua = UserAgent()
get_ip = GetIP()

stopping = False
douban_url = []
maoyan_url = []
maoyan_search_url = []
imdb_url = []


offset = 0
search_year = setting.search_year
end_year = setting.end_year


async def fetch_index(url,session):

    try:
        async with session.get(url,headers = {'User-Agent': ua.random},proxy = get_ip.get_random_ip()) as resp:
            print("index url status: {}".format(resp.status))
            if resp.status in [200,201]:
                data = await resp.text()
                return data
    except Exception as e:
        print(e)

async def fetch(url,session):

    try:
        async with session.get(url,headers = {'User-Agent': ua.random}) as resp:
            print("url status: {}".format(resp.status))
            if resp.status in [200,201]:
                data = await resp.text()
                return data
    except Exception as e:
        print(e)


async def consumer_douban(pool):
    async with aiohttp.ClientSession() as session:
        a = douban_url
        while not stopping:

            if len(douban_url) == 0:
                await asyncio.sleep(0.5)
                continue

            douban_info = douban_url.pop()

            if douban_info["type"] == "list":
                print("正在爬取list")
                url = douban_info["index_url"]
                html = await fetch(url, session)
                asyncio.ensure_future(douban_list_handler(pool, html))
                pass
            elif douban_info["type"] == "detail":
                print("正在爬取detail~~~~~~~~~~~~~~~~~~~~~~~~~~~")

                url = douban_info["douban_url"]
                douban_id = douban_info["douban_id"]
                title = douban_info["title"]
                html = await fetch(url, session)
                asyncio.ensure_future(douban_handler(pool,douban_id,title,html))
            await asyncio.sleep(2)

async def douban_handler(pool,douban_id,title,html):
    soup = BeautifulSoup(html, 'html.parser')
    # 是否上映，未上映删除
    if "尚未上映" in soup.text:
        print("尚未上映，删除记录")

        await aio_db.delet_raw(pool, douban_id)
        return False
    # 是否开画，未开画删除
    if "暂无评分" in soup.text:
        print("尚未开画，删除记录")

        await aio_db.delet_raw(pool, douban_id)
        return False

    # 如果是未播出则删除记录
    if "尚未播出" in soup.text:
        print("为播出，删除记录")
        await aio_db.delet_raw(pool, douban_id)
        return False

    info = soup.find_all('div', {'id': 'info'})[0]

    # 如果是电视剧则删除记录
    if "集数" in info.text:
        print("这是电视剧，删除记录")
        await aio_db.delet_raw(pool, douban_id)
        return False

    # 大陆是上映时间
    year = 0
    releaseDates = soup.find_all('span', {'property': 'v:initialReleaseDate'})
    for date in releaseDates:
        text = date.contents[0]
        if "中国大陆" in text:
            year = int(text[0:4])
            pass

    if year == 0:
        # 大陆未上映
        print("大陆未上映，删除")
        await aio_db.delet_raw(pool, douban_id)
        return

    # 制作国家
    countries_pattern = "地区:(.*?)\n"
    countries = re.findall(countries_pattern, info.text)[0].split('/')
    for country in countries:
        await aio_db.add_country(pool, country, douban_id)

    # 类型
    genres = soup.find_all('span', {'property': 'v:genre'})
    for genre in genres:
        g = genre.contents[0]
        await aio_db.add_genre(pool, g, douban_id)
        pass
    runtime = 0
    if len(soup.find_all('span', {'property': 'v:runtime'})) != 0:
        runtime = int(soup.find_all('span', {'property': 'v:runtime'})[0].attrs['content'])
    # 日期
    dates = soup.find_all('span', {'property': 'v:initialReleaseDate'})
    falg = False
    douban_date = ""
    for date in dates:
        date = date.text
        if "中国大陆" not in date:
            break
        douban_date = date[0:10]
        falg = True
    if falg == False:
        print("《{}》未上映".format(title))
        await aio_db.delet_raw(pool, douban_id)
        return False
    # 豆瓣评分和评论人数
    score = float(soup.find_all('strong', {'property': 'v:average'})[0].contents[0])
    votes = int(soup.find_all('span', {'property': 'v:votes'})[0].contents[0])
    await aio_db.update_douban(pool, douban_id, score, votes, runtime, year)
    #爬取猫眼

    maoyan_search_url.append({"douban_id":douban_id,"title":title,"douban_date":douban_date,"type":"search"})

    # asyncio.ensure_future(consumer_maoyan_search(pool))
    #爬取imdb
    # imdb_pattern = 'IMDb链接:(.*?)$'
    # imdb_id = re.findall(imdb_pattern, info.text)
    # if len(imdb_id) != 0 :
    #     print("爬取imdb评分和评论人数")
    #     imdb_url.append({"douban_id":douban_id,"im_url":imdb_id[0]})
    #     asyncio.ensure_future(consumer_imdb(pool))

    pass

async def douban_list_handler(pool,html):
    global offset
    global search_year
    global end_year
    json_text = html
    if json_text == '{"data":[]}':
        offset = 0
        search_year = search_year - 1
        return

    import json
    items = json.loads(json_text)
    for item in items['data'] :
        title = item['title']
        print("正在爬取{}".format(title))
        #创建目录
        url1 = item['url']
        id_pattern = "t/(\d*?)/"
        id = re.findall(id_pattern, url1)[0]
        #创建新电影数据
        db =await aio_db.init_raw(pool,title,id)
        if db == False:
            s =1
            continue
        #爬取豆瓣详情
        douban_url.append({"douban_id":id , "douban_url":url1,"title":title,"type":"detail"})
        a = douban_url
    offset = offset + 20
    index_url = "https://movie.douban.com/j/new_search_subjects?sort=T&range=0,10&tags=%E7%94%B5%E5%BD%B1&start={0}&year_range={1},{1}".format(
        offset, search_year)
    await asyncio.sleep(5)
    douban_url.append({"index_url": index_url, "type": "list"})

    pass

# async def consumer_imdb(pool):
#     async with aiohttp.ClientSession() as session:
#         while not stopping:
#
#             if len(imdb_url) ==0:
#                 await asyncio.sleep(0.5)
#                 continue
#
#             imdb_info = imdb_url.pop()
#             im_url = imdb_info["im_url"]
#             douban_id = imdb_info["douban_id"]
#             asyncio.ensure_future(imdb_handler(pool,session,douban_id,im_url))
#
# async def imdb_handler(pool,session,douban_id,im_url):
#     r = await aio_db.verificte_exit(pool,douban_id)
#     if r == False:
#         return False
#     imdb_id = im_url.split()[0]
#     url = "https://www.imdb.com/title/{}".format(imdb_id)
#     html = await fetch(url,session)
#     soup = BeautifulSoup(html, 'html.parser')
#
#     # socre and reviews
#     score = float(soup.find_all('span', {'itemprop': 'ratingValue'})[0].contents[0])
#     reviews = soup.find_all('span', {'itemprop': 'ratingCount'})[0].contents[0]
#     if "," in reviews:
#         reviews = reviews.replace(',', '')
#
#     reviews = int(reviews)
#
#     await aio_db.update_imdb(pool,douban_id, score, reviews, imdb_id)
#     await asyncio.sleep(2)
#
#     pass





async def consumer_maoyan_search(pool):
    async with aiohttp.ClientSession() as session:
        while not stopping:
            if len(maoyan_search_url) == 0:
                await asyncio.sleep(0.5)
                continue

            maoyan_search_info = maoyan_search_url.pop()
            if maoyan_search_info["type"] =="search":
                title = maoyan_search_info["title"]
                url = "https://piaofang.maoyan.com/search?key=" + quote(title)
                html = await fetch(url, session)
                douban_id = maoyan_search_info["douban_id"]
                douban_date = maoyan_search_info["douban_date"]
                asyncio.ensure_future(maoyan_search_handler(pool,douban_id,title,douban_date,html))
            elif maoyan_search_info["type"] =="detail":
                id = maoyan_search_info["id"]
                url_box = "https://piaofang.maoyan.com/movie/{}".format(id)
                html = await fetch(url_box, session)
                douban_id = maoyan_search_info["douban_id"]
                asyncio.ensure_future(maoyan_detail_handler(pool,douban_id,id,html))
                pass
            await asyncio.sleep(3)

async def maoyan_search_handler(pool,douban_id,title,douban_date,html):
    r = await aio_db.verificte_exit(pool, douban_id)
    if r == False:
        return False

    print("正在搜索{}的猫眼页面".format(title))

    await asyncio.sleep(1)
    # 获取猫眼id
    soup = BeautifulSoup(html, 'html.parser')
    data_1 = soup.find_all('article', {'class': 'indentInner canTouch'})
    flag = False
    for item in data_1:
        date = item.contents[5].contents[0]
        if douban_date in date:
            id = item.attrs['data-url']
            id_pattern = "\d{1,10}"
            id = re.findall(id_pattern, id)[0]
            flag = True

            break
    if flag == False:
        return False
    maoyan_url.append({"id":id,"douban_id":douban_id,"type":"detail"})
    # asyncio.ensure_future(consumer_maoyan(pool))


async def maoyan_detail_handler(pool,douban_id,id,html):
    print("检验是否存在")
    r = await aio_db.verificte_exit(pool, douban_id)
    if r == False:
        return False
    print("开始爬猫眼信息")


    soup = BeautifulSoup(html, 'html.parser')
    data_1 = soup.find_all('span', {'class': 'rating-num'})
    # 评分
    if len(data_1) == 0:
        score = "0"
        return False
    else:
        score = data_1[0].string
    # 评论
    reviews_count = 0
    a = soup.find_all('p', {'class': 'detail-score-count'})
    if len(soup.find_all('p', {'class': 'detail-score-count'})) == 0:
        return False
    reviews = soup.find_all('p', {'class': 'detail-score-count'})[0].text[0:-5]
    if reviews[-1] == "万":
        reviews_count = int(reviews[0:-1]) * 10000
    else:
        reviews_count = int(reviews)
    # 中国票房
    total_box = 0.0
    modul_box = soup.find_all('span', {'class': 'detail-num'})
    if len(modul_box) == 0:
        total_box = "0"
    else:

        num = modul_box[0].text
        unit = soup.find_all('span', {'class': 'detail-unit'})[0].text
        if unit == "万":
            total_box = float(num) * 10000
        if unit == "亿":
            total_box = float(num) * 100000000

    # 美国票房
    us_box = 0.0

    if len(soup.find_all('div', {'class': 'NAmerican-show'})) != 0:
        box = soup.find_all('div', {'class': 'item'})[1].contents[3].text
        us_box = float(box[0:-1]) * 10000

    print("更新猫眼信息")

    await aio_db.update_maoyan(pool, douban_id, id, score, reviews_count, total_box, us_box)
    return True
    pass




async def main(loop):


    pool = await aiomysql.create_pool(host = setting.db_host , port = setting.db_port ,user = setting.db_user ,
                                      password = setting.db_password,
                                      db = setting.db_name,loop = loop ,
                                      charset = "utf8" , autocommit = True)

    async with aiohttp.ClientSession() as session:
        global offset
        global search_year
        global end_year
        index_url = "https://movie.douban.com/j/new_search_subjects?sort=T&range=0,10&tags=%E7%94%B5%E5%BD%B1&start={0}&year_range={1},{1}".format(
                 offset, search_year )
        douban_url.append({"index_url":index_url,"type":"list"})
        await asyncio.gather(consumer_douban(pool),consumer_maoyan_search(pool))
        #结束爬取
        if search_year == end_year:
            while not stopping:

                if len(douban_url) != 0 and len(maoyan_url) != 0 and len(maoyan_search_url) != 0:
                    await asyncio.sleep(60)
                    continue

                else:
                    print("结束爬取")
                    pool.close()
                    await pool.wait_closed()
                    loop.close()
        # asyncio.ensure_future(consumer_douban(pool))
        # asyncio.ensure_future(consumer_maoyan_search(pool))



if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    asyncio.ensure_future(main(loop))
    loop.run_forever()
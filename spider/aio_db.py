# -*- coding: utf-8 -*-
__author__ = 'CL'
__date__ = '2019/3/15 2:33 AM'


async def init_raw(pool,title,id):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            insert_sql = "insert movies(title,douban_id) VALUES('{0}','{1}')".format(title, id)
            try:
                if await cur.execute(insert_sql):
                    print("新加条目")
                    return True
            except Exception as e:
                print(e)
                print("重复了")
                return False


async def delet_raw(pool,douban_id):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            print("deleting ")
            delete_sql = """
                        delete from movies where douban_id='{0}'
                    """.format(douban_id)
            try:
                if await cur.execute(delete_sql):
                    await conn.commit()
                    print("删除成功")
                    return True
            except Exception as e:
                print(e)
                return False

async def update_douban(pool,douban_id,douban_score,douban_ratingCount,runtime,year):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                sql = "update movies SET douban_score='{0}', douban_ratingCount='{1}', " \
                      "runtime = '{2}', " \
                      "year = '{3}'" \
                      " where douban_id = '{4}'".format(
                        douban_score,douban_ratingCount,runtime,year,douban_id
                    )
                if await cur.execute(sql):
                    await conn.commit()
                    print("更新豆瓣信息")
                    return True
            except Exception as e:
                print(e)
                return False


async def add_genre(pool,genre,douban_id):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                sql = "insert genre_movies(genre,douban_id) VALUES('{0}','{1}')".format(
                        genre,douban_id
                    )
                if await cur.execute(sql):
                    await conn.commit()
                    print("新加类型")
                    return True
            except Exception as e:
                print(e)
            pass


async def add_country(pool,country,douban_id):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                sql = "insert country_movies(country,douban_id) VALUES('{0}','{1}')".format(
                        country,douban_id
                    )
                if await cur.execute(sql):
                    await conn.commit()
                    print("新加制作国家")
                    return True
            except Exception as e:
                print(e)
                return False

async def update_maoyan(pool,douban_id,maoyan_id,maoyan_score,maoyan_ratingCount,china_box,us_box):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                sql = "update movies SET maoyan_id = '{0}',maoyan_score='{1}', maoyan_ratingCount='{2}', " \
                      "china_gross= '{3}', " \
                      "us_gross= '{4}'" \
                      " where douban_id = '{5}'".format(
                        maoyan_id,maoyan_score,maoyan_ratingCount,china_box,us_box,douban_id
                    )
                if await cur.execute(sql):
                    await conn.commit()
                    print("更新猫眼信息")
                    return True
            except Exception as e:
                print(e)
                return False
            pass

async def verificte_exit(pool,douban_id):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                sql = "select 1 from movies where douban_id = '{}' limit 1;".format(douban_id)

                if await cur.execute(sql):
                    await conn.commit()
                    a = await cur.fetchall()
                    return True
                else:
                    return False
            except Exception as e:
                print(e)
                return False


async def update_imdb(pool,douban_id,imdb_score,imdb_ratingCount,imdb_id):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                sql = "update movies SET imdb_score = '{0}',imdb_ratingCount = '{1}',imdb_id = '{2}'"\
                      "where douban_id = '{3}'".format(
                        imdb_score,imdb_ratingCount,imdb_id,douban_id
                    )
                if await cur.execute(sql):
                    await conn.commit()
                    print("更新imdb信息")
                    return True
            except Exception as e:
                print(e)
                return False
            pass
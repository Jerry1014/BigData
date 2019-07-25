# -*- coding:utf-8 -*-
import json
import re
import threading
from random import randint
from time import sleep

import requests

from FackUA import FakeUA


def search_and_get_song_id(key_word):
    song_name_set = set()
    # 测试用延时
    sleep(1)
    my_fake_ua = FakeUA()
    page_num = 1
    search_url = 'https://c.y.qq.com/soso/fcgi-bin/client_search_cp?ct=24&qqmusic_ver=1298&new_json=1&remoteplace=txt.yqq.center&searchid=42491747917371012&t=0&aggr=1&cr=1&catZhida=1&lossless=0&flag_qc=0&p=%d&n=10&w=%s&g_tk=5381&loginUin=0&hostUin=0&format=json&inCharset=utf8&outCharset=utf-8&notice=0&platform=yqq.json&needNewCode=0'

    while True:
        headers = {'User-Agent': my_fake_ua.random, 'Referer': 'https://y.qq.com/portal/search.html'}
        response = requests.get(search_url % (page_num, key_word), headers=headers)
        response_json = json.loads(response.text)
        if response_json['message'] == "no results":
            break
        for i in response_json['data']['song']['list']:
            song_name = i['name'].split('(')[0]
            if song_name not in song_name_set:
                song_name_set.add(song_name)
                yield i['id'], i['mid']
        page_num += 1


def get_lyric(song, lyric_buffer):
    # 测试用延时
    my_fake_ua = FakeUA()
    request_lrc_url = "https://c.y.qq.com/lyric/fcgi-bin/fcg_query_lyric_yqq.fcg?nobase64=1&musicid=%s&-=jsonp1&g_tk=5381&loginUin=0&hostUin=0&format=json&inCharset=utf8&outCharset=utf-8&notice=0&platform=yqq.json&needNewCode=0"
    referer_lrc_url = "https://y.qq.com/n/yqq/song/%s.html"

    headers = {'User-Agent': my_fake_ua.random, 'Referer': referer_lrc_url % song[1]}
    response = requests.get(request_lrc_url % song[0], headers=headers)

    try:
        lyric = re.findall('\d\d](?!(?=OP|\[))(.+?)(?=$|&|\[)',
                           json.loads(response.text)['lyric'].replace('&#10;', '').replace('&#32;', ' ').replace(
                               '&#40;', ' '))
        lyric_buffer.append(lyric)
        sleep(randint(0, 3))
    except KeyError:
        pass


if __name__ == '__main__':
    max_thread_num = 10
    thread_pool = []
    my_search_word = "陈奕迅"
    all_lyric = []
    try:
        get_song = search_and_get_song_id(my_search_word)
        while True:
            song_info = next(get_song)
            t = threading.Thread(target=get_lyric, args=(song_info, all_lyric))
            thread_pool.append(t)
            t.start()

            if len(thread_pool) > max_thread_num:
                for t in thread_pool:
                    if not t.is_alive():
                        thread_pool.remove(t)
    except StopIteration as e:
        if len(thread_pool) > 0:
            for t in thread_pool:
                if not t.is_alive():
                    thread_pool.remove(t)
    except:
        pass
    with open(my_search_word + '全部歌词.txt', 'w', encoding='utf-8') as f:
        for i in all_lyric:
            for j in i:
                f.write(j + '\n')
            f.write('\n')

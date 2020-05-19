import math
from sqlalchemy import create_engine
from aa.util import DingMessage, bobao, write_content_to_file
from aa.boson_ import setiment_score
import multiprocessing
import datetime
import time
import re
import jieba
import jieba.analyse
import jieba.posseg as psg
from gensim import corpora, models
import functools
import pandas as pd
import warnings
from kafka import KafkaProducer
import json
warnings.filterwarnings('ignore')
user = 'fanhaojie'
passwd = 'Chenfan@123'
host = '10.228.86.203'
port = '11101'
dbname = 'test'
engine1 = create_engine("mysql+pymysql://%s:%s@%s:%s/%s?charset=utf8"%(user,passwd,host,port,dbname))

engine = create_engine('postgresql://fhj@10.228.88.171:5432/weibo')

import redis
redis_pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
redis_conn = redis.Redis(connection_pool=redis_pool)

from clickhouse_driver import Client
client2 = Client(host='10.228.81.237', user='default', database='putao', password='eqrymyvW')
producer = KafkaProducer(bootstrap_servers=['10.228.88.171:9093'],
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))
print(producer)
sql = 'select * from weibo_qingganfenshu_score5 limit 40'
data = pd.read_sql(sql,engine1)
# click_house = client2.execute("select * from putao.f_weibo_blog_comment limit 10",types_check=True)
# col = client2.execute('DESCRIBE TABLE putao.f_weibo_blog_comment')
# col = pd.DataFrame(col)
# data = pd.DataFrame(click_house,columns=col[0].tolist())
# # print(data.info())
# data = data[['blog_id','comment_id','comment_content']]

# aa = DingMessage(all_message=len(data))
#
# write_content_to_file(filename='message_produce_num', content=str(len(data)))


data_list = [data.ix[i].to_dict() for i in data.index.values]
#weibo_extract_words_kafka
data_set_name = 'web_score'
for msg in data_list:
    # 以 __str__(self), 作为key
    msg_name = str(msg)
    # 判断是否重复
    if redis_conn.sismember(data_set_name, msg_name):
        continue
    else:
        # data_list.append(data)
        redis_conn.sadd(data_set_name, msg_name)
        producer.send('qingganfenxi_score', msg)
        print(msg)

producer.flush()
producer.close()

"""
import json
import urllib.request

#1、构建url
#2、构建一下请求头部o_red_extract_words_ding
def bobao(content):
    url = "https://oapi.dingtalk.com/robot/send?access_token=ab55f76f329af91b4ca7beec3f3c9165df17d6d58209e67584c31028683542b3"

    header = {
        "Content-Type": "application/json",
        "Charset": "UTF-8"
    }
    #3、构建请求数据
    data = {
        "msgtype": "text",
        "text": {
            "content":content
        },
        "at": {
             "isAtAll": True     #@全体成员（在此可设置@特定某人）
        }
    }
    # 4、对请求的数据进行json封装
    sendData = json.dumps(data)  # 将字典类型数据转化为json格式
    sendData = sendData.encode("utf-8")  # python3的Request要求data为byte类型

    # 5、发送请求
    request = urllib.request.Request(url=url, data=sendData, headers=header)

    # 6、将请求发回的数据构建成为文件格式

    opener = urllib.request.urlopen(request)
    # 7、打印返回的结果
#     # print(opener.read())
# """
#
# data = bobao(content=aa.default_format())








































































# data_list = [data.ix[i].to_dict() for i in data.index.values]
# #
# data_set_name = "qingganfenxi_score"
# for msg in data_list:
#     # 以 __str__(self), 作为key
#     msg_name = str(msg)
#     # 判断是否重复
#     if redis_conn.sismember(data_set_name, msg_name):
#         continue
#     else:
#         # data_list.append(data)
#         redis_conn.sadd(data_set_name, msg_name)
#         producer.send('qingganfenxi_score', msg)
#         print(msg)
#
# producer.flush()
# producer.close()
import math
from sqlalchemy import create_engine
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
import redis

warnings.filterwarnings('ignore')
user = 'fanhaojie'
passwd = 'Chenfan@123'
host = '10.228.86.203'
port = '11101'
dbname = 'test'
engine1 = create_engine("mysql+pymysql://%s:%s@%s:%s/%s?charset=utf8" % (user, passwd, host, port, dbname))

engine = create_engine('postgresql://fhj@10.228.88.171:5432/weibo')

# 创建连接
redis_pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
redis_conn = redis.Redis(connection_pool=redis_pool)

from clickhouse_driver import Client

client2 = Client(host='10.228.81.237', user='default', database='putao', password='eqrymyvW')
producer = KafkaProducer(bootstrap_servers=['10.228.88.171:9093'],
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))
print(producer)

sql = 'select * from f_weibo_monitoring_comments'
data = pd.read_sql(sql, engine)

data_set_name = "jiankong_qingganfenxi_score2"

data_list = [data.ix[i].to_dict() for i in data.index.values]

for msg in data_list:
    # 以 __str__(self), 作为key
    msg_name = str(msg)
    # 判断是否重复
    if redis_conn.sismember(data_set_name, msg_name):
        continue
    else:
        # data_list.append(data)
        redis_conn.sadd(data_set_name, msg_name)
        producer.send('weibo_qingganfenxi_jiankong_score', msg)
        print(msg)

producer.flush()
producer.close()
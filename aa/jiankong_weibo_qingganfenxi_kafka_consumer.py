import pandas as pd
import json
import math
import time
import datetime
import jieba.posseg as psg
import jieba
import re
import functools

import jieba.analyse
from clickhouse_driver import Client
from sqlalchemy import create_engine
from kafka import KafkaConsumer
from aa.boson_ import setiment_score

import redis
#创建Redis连接
redis_pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
redis_conn = redis.Redis(connection_pool=redis_pool)

#连接clickhouse数据库
engine = create_engine('postgresql://fhj@10.228.88.171:5432/weibo')
client2 = Client(host='10.228.86.203', user='default', database='weibo_his', password='nEB7+b3X')
def get_stopword_list():
    # 停用词表存储路径，每一行为一个词，按行读取进行加载
    # 进行编码转换确保匹配准确率
    stop_word_path = r'C:\Users\hjfan\Desktop\keywords\stopword.txt'
    stopword_list = [sw.replace('\n', '') for sw in open(stop_word_path,'r',encoding='UTF-8').readlines()]
    return stopword_list

# 分词方法，调用结巴接口
def seg_to_list(sentence, pos=False):
    if not pos:
        # 不进行词性标注的分词方法
        seg_list = jieba.cut(sentence)
    else:
        # 进行词性标注的分词方法
        seg_list = psg.cut(sentence)
    return seg_list


# 去除干扰词
def word_filter(seg_list, pos=False):
    stopword_list = get_stopword_list()
    filter_list = []
    # 根据POS参数选择是否词性过滤
    ## 不进行词性过滤，则将词性都标记为n，表示全部保留
    for seg in seg_list:
        if not pos:
            word = seg
            flag = 'n'
        else:
            word = seg.word
            flag = seg.flag
        if not flag.startswith('n'):
            continue
        # 过滤停用词表中的词，以及长度为<2的词
        if not word in stopword_list and len(word) > 1:
            filter_list.append(word)

    return filter_list


# 数据加载，pos为是否词性标注的参数，corpus_path为数据集路径
def load_data(pos=False, corpus_path='corpus.txt'):
    # 调用上面方式对数据集进行处理，处理后的每条数据仅保留非干扰词
    doc_list = []
    for line in open(corpus_path, 'r',encoding='UTF-8'):
        content = line.strip()
        seg_list = seg_to_list(content, pos)
        filter_list = word_filter(seg_list, pos)
        doc_list.append(filter_list)

    return doc_list


# idf值统计方法
def train_idf(doc_list):
    idf_dic = {}
    # 总文档数
    tt_count = len(doc_list)

    # 每个词出现的文档数
    for doc in doc_list:
        for word in set(doc):
            idf_dic[word] = idf_dic.get(word, 0.0) + 1.0

    # 按公式转换为idf值，分母加1进行平滑处理
    for k, v in idf_dic.items():
        idf_dic[k] = math.log(tt_count / (1.0 + v))

    # 对于没有在字典中的词，默认其仅在一个文档出现，得到默认idf值
    default_idf = math.log(tt_count / (1.0))
    return idf_dic, default_idf


#  排序函数，用于topK关键词的按值排序
def cmp(e1, e2):
    import numpy as np
    res = np.sign(e1[1] - e2[1])
    if res != 0:
        return res
    else:
        a = e1[0] + e2[0]
        b = e2[0] + e1[0]
        if a > b:
            return 1
        elif a == b:
            return 0
        else:
            return -1

# TF-IDF类
class TfIdf(object):
    # 四个参数分别是：训练好的idf字典，默认idf值，处理后的待提取文本，关键词数量
    def __init__(self, idf_dic, default_idf, word_list, keyword_num):
        self.word_list = word_list
        self.idf_dic, self.default_idf = idf_dic, default_idf
        self.tf_dic = self.get_tf_dic()
        self.keyword_num = keyword_num

    # 统计tf值
    def get_tf_dic(self):
        tf_dic = {}
        for word in self.word_list:
            tf_dic[word] = tf_dic.get(word, 0.0) + 1.0

        tt_count = len(self.word_list)
        for k, v in tf_dic.items():
            tf_dic[k] = float(v) / tt_count

        return tf_dic

    # 按公式计算tf-idf
    def get_tfidf(self):
        tfidf_dic = {}
        for word in self.word_list:
            idf = self.idf_dic.get(word, self.default_idf)
            tf = self.tf_dic.get(word, 0)

            tfidf = tf * idf
            tfidf_dic[word] = tfidf

        tfidf_dic.items()
        # 根据tf-idf排序，去排名前keyword_num的词作为关键词
        for k, v in sorted(tfidf_dic.items(), key=functools.cmp_to_key(cmp), reverse=True)[:self.keyword_num]:
            yield k


def tfidf_extract(word_list, pos=False, keyword_num=10):
    doc_list = load_data(pos)
    idf_dic, default_idf = train_idf(doc_list)
    tfidf_model = TfIdf(idf_dic, default_idf, word_list, keyword_num)
    for i in tfidf_model.get_tfidf():
        yield i

consumer = KafkaConsumer(
                        'weibo_qingganfenxi_jiankong',
                        bootstrap_servers=['10.228.88.171:9093'],
                        group_id='my-group-1',
                        auto_offset_reset='earliest',
                        value_deserializer=lambda x: json.loads(x),
)
data_set_name = "jiankong_qgfx"
data_limit = 20
data_list = []
for i in consumer:
    data = i.value
    print(data)
    re_ = re.sub('\[.*?\]|回复(.*):|src(.*)>|["lem]', '', data['comment_text'])
    china_ = ''.join(re.findall(r'[\u4e00-\u9fa5]', re_))
    pos = True
    seg_list = seg_to_list(data['comment_text'], pos)
    # print('+++++++++',seg_list)
    filter_list = word_filter(seg_list, pos)
    score = setiment_score(sententce=china_)
    data_frame = pd.DataFrame(columns=['comments_labels','comment_id','comment_score'])
    for aa in tfidf_extract(filter_list):
        ac = {"comments_labels": aa, "comment_score": score}
        data.update(ac)
        cc = pd.DataFrame([data])
        data_frame = pd.concat([cc, data_frame], axis=0, ignore_index=True)
    data_frame = data_frame[['comment_id', 'comments_labels', 'comment_score']]
    # 更改列名称
    data_frame.rename(columns={'comments_labels': 'comment_labels'},inplace=True)
    # __str__(self) 作为key
    data_frame_name = str(data_frame)
    # 判断是否重复
    if redis_conn.sismember(data_set_name, data_frame_name):
        continue
    else:
        data_list.append(data_frame)
        redis_conn.sadd(data_set_name, data_frame_name)
    print(data_frame)

    if len(data_list) >= data_limit:
        for data_frame in data_list:
            pd.io.sql.to_sql(data_frame, 'f_weibo_monitoring_comments_sa', engine, schema='public',if_exists='append', index=False)
            # client2.execute('INSERT INTO f_weibo_blog_comment_an VALUES', data_frame, types_check=True)
        data_list = []
#
if len(data_list) > 0:
    for data_frame in data_list:
        pd.io.sql.to_sql(data_frame, 'f_weibo_monitoring_comments_sa', engine, schema='public',if_exists='append', index=False)
        # client2.execute('INSERT INTO f_weibo_blog_comment_an VALUES', data_frame, types_check=True)
    data_list = []
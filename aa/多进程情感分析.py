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
warnings.filterwarnings('ignore')
user = 'fanhaojie'
passwd = 'Chenfan@123'
host = '10.228.86.203'
port = '11101'
dbname = 'test'
engine1 = create_engine("mysql+pymysql://%s:%s@%s:%s/%s?charset=utf8"%(user,passwd,host,port,dbname))

engine = create_engine('postgresql://fhj@10.228.88.171:5432/weibo')

from clickhouse_driver import Client
client2 = Client(host='127.0.0.1', user='default', database='putao', password='eqrymyvW')

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

def write_queue(queue):
    sql = 'select * from test.weibo_qingganfenshu_score3333 limit 10'
    data = pd.read_sql(sql,engine1)
    # deata = pd.read_excel('score5.xlsx')
    print(data)
    # print(data.info())
    # data.to_excel('score5.xlsx')
    # # 循环写入数据
    # # aa = client2.execute("select * from putao.f_weibo_blog_comment as a join(select * from  putao.f_weibo_blog where platform_cid = '1549362863' and publish_time BETWEEN addMonths(now(),-1) and now()) as b on a.blog_id = b.blog_id",types_check=True)
    # #
    # # data = pd.DataFrame(aa)
    # data = pd.read_excel('qingganfenxi_test.xlsx')[:20]
    # sql = 'select * from test.weibo_red_words limit 100'
    # data = pd.read_sql(sql)
    data_list = [data.ix[i].to_dict() for i in data.index.values]
    for i in data_list:
        if queue.full():
            print("队列已满!")
        queue.put(i)

def read_queue(queue, pid):
    # 循环读取队列消息
    # count = 1
    while True:
        result = queue.get()
        re_ = re.sub('\[.*?\]|回复(.*):|src(.*)>|["lem]', '', result['comment_content'])
        china_ = ''.join(re.findall(r'[\u4e00-\u9fa5]', re_))
        pos = True
        seg_list = seg_to_list(result['comment_content'], pos)
        filter_list = word_filter(seg_list, pos)
        score = setiment_score(sententce=china_)
        # print('+++++++++++++',filter_list)
        update_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        data_frame = pd.DataFrame([], columns=['comment_id', 'keywords', 'score', 'update_time'])
        for aa in tfidf_extract(filter_list):
            ac = {"keywords":aa,"update_time":update_time,"score":score}
            result.update(ac)
            cc = pd.DataFrame([result])
            cc = cc[['comment_id', 'keywords', 'score', 'update_time']]
            cc.rename(columns={"keywords": "keyword","comment_id":"comments_id"}, inplace=True)
            # cc = cc.values.tolist()
            print(cc)
            # client2.execute('INSERT INTO f_weibo_blog_comment_an VALUES', cc, types_check=True)
            #     data_frame = pd.concat([cc,data_frame],axis=0,ignore_index=True)
        # data_frame.rename(columns={"keywords": "keyword","comment_id":"comments_id"}, inplace=True)
        # print(data_frame)
        # data_frame = data_frame.values.tolist()
        # print(data_frame)
        # client2.execute('INSERT INTO f_weibo_blog_comment_an VALUES', data_frame, types_check=True)
        # pd.io.sql.to_sql(data_frame, 'f_weibo_blog_comment_an', engine, schema='public', if_exists='append',index=False)
if __name__ == '__main__':
    queue = multiprocessing.Queue(200)
    p1 = multiprocessing.Process(target=write_queue, args=(queue,))
    # 等待p1写数据进程执行结束后，再往下执行
    p2 = multiprocessing.Process(target=read_queue, args=(queue, 1))
    p3 = multiprocessing.Process(target=read_queue, args=(queue, 2))
    # p4 = multiprocessing.Process(target=read_queue, args=(queue, 3))
    # p5 = multiprocessing.Process(target=read_queue, args=(queue, 4))
    # p6 = multiprocessing.Process(target=read_queue, args=(queue, 5))
    # p7 = multiprocessing.Process(target=read_queue, args=(queue, 6))
    # p8 = multiprocessing.Process(target=read_queue, args=(queue, 7))
    # p9 = multiprocessing.Process(target=read_queue, args=(queue, 8))
    # p10 = multiprocessing.Process(target=read_queue, args=(queue, 9))
    # p11 = multiprocessing.Process(target=read_queue, args=(queue, 10))
    # p12 = multiprocessing.Process(target=read_queue, args=(queue, 11))
    # p13 = multiprocessing.Process(target=read_queue, args=(queue, 12))
    # p14 = multiprocessing.Process(target=read_queue, args=(queue, 13))
    # p15 = multiprocessing.Process(target=read_queue, args=(queue, 14))
    # p16 = multiprocessing.Process(target=read_queue, args=(queue, 15))
    # p17 = multiprocessing.Process(target=read_queue, args=(queue, 16))
    # p18 = multiprocessing.Process(target=read_queue, args=(queue, 17))

    #
    p1.start()
    p2.start()
    p3.start()
    # p4.start()
    # p5.start()
    # p6.start()
    # p7.start()
    # p8.start()
    # p9.start()
    # p10.start()
    # p11.start()
    # p12.start()
    # p13.start()
    # p14.start()
    # p15.start()
    # p16.start()
    # p17.start()
    # p18.start()
    p1.join()
    p2.join()
    p3.join()
    print("=============")
    # p4.join()
    # p5.join()
    # p6.join()
    # p7.join()
    # p8.join()
    # p9.join()
    # p10.join()
    # p11.join()
    # p12.join()
    # p13.join()
    # p14.join()
    # p15.join()
    # p16.join()
    # p17.join()
    # p18.join()
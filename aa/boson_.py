from collections import defaultdict
import codecs
import jieba
import os
import re

"""jieba分词"""

def seg_word(sentence):
    #jieba对文本进行分词
    seg_list = jieba.cut(sentence)
    seg_reuslt = []
    for w in seg_list:
        seg_reuslt.append(w)
    #读取停用词文件
    stopwords = set()
    fr = codecs.open('stopwords.txt','r',encoding='utf-8')
    for word in fr:
        stopwords.add(word.strip())
    fr.close()
    return list(filter(lambda x: x not in stopwords,seg_reuslt))


# print(seg_word(sentence='今天的天气真好啊！'))

"""词语分类"""
def classify_words(word_dict):
    #找出情感词，否定词、程度副词
    #读取情感字典的文件
    sen_file = open('BosonNLP_sentiment_score.txt','r+',encoding='utf-8')
    #获取文件的内容
    sen_list = sen_file.readlines()
    #创建情感字典
    sen_dict = defaultdict()
    for s in sen_list:
        try:
            sen_dict[s.split(' ')[0]] = s.split(' ')[1]
        except Exception as e:
            print(s)
            print(e)
    #读取否定词文件

    not_word_file = open('notdic.txt','r+',encoding='utf-8')
    not_word_list = not_word_file.readlines()
    #读取程度副词文件
    degree_file = open('degree.txt','r+',encoding='utf-8')
    degree_list = degree_file.readlines()
    degree_dic = defaultdict()
    #词语存为键，值为分数
    for d in degree_list:
        degree_dic[d.split(' ')[0]] = d.split(' ')[1]
    sen_word = dict()
    not_word = dict()
    degree_word = dict()
    for word in word_dict.keys():
        if word in sen_dict.keys() and word not in not_word_list and word not in degree_dic.keys():
            # 找出分词结果中在情感字典中的词
            sen_word[word_dict[word]] = sen_dict[word]
        elif word in not_word_list and word not in degree_dic.keys():
            # 分词结果中在否定词列表中的词
            not_word[word_dict[word]] = -1
        elif word in degree_dic.keys():
            # 分词结果中在程度副词中的词
            degree_word[word_dict[word]] = degree_dic[word]
    sen_file.close()
    degree_file.close()
    not_word_file.close()
        # 将分类结果返回
    return sen_word, not_word, degree_word
def list_to_dict(word_list):
    """将分词后的列表转为字典，key为单词，value为单词在列表中的索引，索引相当于词语在文档中出现的位置"""
    data = {}
    for x in range(0, len(word_list)):
        data[word_list[x]] = x
    return data


def get_init_weight(sen_word, not_word, degree_word):
    # 权重初始化为1
    W = 1
    # 将情感字典的key转为list
    sen_word_index_list = list(sen_word.keys())
    if len(sen_word_index_list) == 0:
        return W
    # 获取第一个情感词的下标，遍历从0到此位置之间的所有词，找出程度词和否定词
    for i in range(0, sen_word_index_list[0]):
        if i in not_word.keys():
            W *= -1
        elif i in degree_word.keys():
            # 更新权重，如果有程度副词，分值乘以程度副词的程度分值
            W *= float(degree_word[i])
    return W


def socre_sentiment(sen_word, not_word, degree_word, seg_result):
    """计算得分"""
    # 权重初始化为1
    W = 1
    score = 0
    # 情感词下标初始化
    sentiment_index = -1
    # 情感词的位置下标集合
    sentiment_index_list = list(sen_word.keys())
    # 遍历分词结果(遍历分词结果是为了定位两个情感词之间的程度副词和否定词)
    for i in range(0, len(seg_result)):

    # 若是程度副词
        if i in degree_word.keys():
            W *= degree_word[i]
        # 若是否定词
        elif i in not_word.keys():
            # print(i)
            W *= -1
        elif i in sen_word.keys():
            score += float(W) * float(sen_word[i])
            W = 1
    return score


# 计算得分
def setiment_score(sententce):
    # 1.对文档分词
    seg_list = seg_word(sententce)
    # 2.将分词结果列表转为dic，然后找出情感词、否定词、程度副词
    sen_word, not_word, degree_word = classify_words(list_to_dict(seg_list))
    # 3.计算得分
    score = socre_sentiment(sen_word, not_word, degree_word, seg_list)
    return score


# 测试
# print(setiment_score(sententce='就这样吧'))

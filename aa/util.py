import json
import os
import urllib.request

# 1、构建url
# 2、构建一下请求头部
def bobao(content):
    #情感分析机器人
    url = "https://oapi.dingtalk.com/robot/send?access_token=e8276568bc3df0f8188710faa7532231095018b050adad1a52115e57007cdfe9"

    header = {"Content-Type": "application/json", "Charset": "UTF-8"}
    # 3、构建请求数据
    data = {
        "msgtype": "text",
        "text": {"content": content},
        "at": {"isAtAll": True},  # @全体成员（在此可设置@特定某人）
    }
    # 4、对请求的数据进行json封装
    sendData = json.dumps(data)  # 将字典类型数据转化为json格式
    sendData = sendData.encode("utf-8")  # python3的Request要求data为byte类型

    # 5、发送请求
    request = urllib.request.Request(url=url, data=sendData, headers=header)

    # 6、将请求发回的数据构建成为文件格式

    opener = urllib.request.urlopen(request)
    # 7、打印返回的结果
    # print(opener.read())


class DingMessage:
    def __init__(self, all_message: int, consumed: int = 0, in_sql: int = 0):
        """
        all_message: 总共有多少条数据,生产了多少条数据
        consumed: 消费了多少条数据
        in_sql: 入库了多少条数据
        waitting: 剩余多少条数据
        """
        self.all_message = all_message
        self.consumed = consumed
        self.in_sql = in_sql
        self.waitting = self.all_message - self.consumed
        self.prefix_words = ["生产了", "消费了", "入库了", "剩余"]

    def default_format(self, prefix_words=None, indent=" ", suffix="条数据") -> str:
        # suffix： 后缀
        if prefix_words == None:
            prefix_words = self.prefix_words
        message_nums = [self.all_message, self.consumed, self.in_sql, self.waitting]
        message_nums = list(map(str, message_nums))
        ret = []
        for prefix, num in zip(prefix_words, message_nums):
            ret.append(prefix + num + suffix)

        return indent.join(ret) + '!'

    def add_in_sql(self, num: int):
        self.in_sql += num

    def add_consumed(self, num):
        self.consumed += num
        self.waitting -= num


dingding_filename = "message_produce_num"


def write_content_to_file(content="", path=".", filename=dingding_filename):
    if filename == None:
        print("Must has a filename")
        return
    file_abs_path = os.path.abspath(os.path.join(path, filename))
    try:
        if not os.path.exists(path):
            os.makedirs(path)
        with open(file_abs_path, "w+") as file:
            file.write(content)
        return True
    except:
        return False


def read_content_from_file(path=".", filename=dingding_filename):
    if filename == None:
        print("Must has a filename")
        return
    try:
        file_abs_path = os.path.abspath(os.path.join(path, filename))
        return open(file_abs_path).read()
    except:
        return None


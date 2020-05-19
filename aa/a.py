import json
import os
import urllib.request

# 1、构建url
# 2、构建一下请求头部
def bobao(content):
    url = "https://oapi.dingtalk.com/robot/send?access_token=09ac4be869c6be7f1c640d9b450a9b288d6b37a779cff53fe7a6eb613e0536de"

    header = {"Content-Type": "application/json", "Charset": "UTF-8"}
    # 3、构建请求数据
    data = {
        "msgtype": "text",
        "text": {"content": content},
        "at": {
             "atMobiles": [
                 "17343143306"
             ],
             "isAtAll": False
         }  # @全体成员（在此可设置@特定某人）
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

bobao('连接超时!')


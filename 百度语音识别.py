import pyaudio
# -*- coding: utf-8 -*-
"""
实时流式识别
需要安装websocket-client库
使用方式 python realtime_asr.py 16k-0.pcm
"""
import websocket
import pyaudio

import threading
import time
import uuid
import json
import logging
import string
import sys
import re

if len(sys.argv) < 2:
    pcm_file = "16k-0.pcm"
else:
    pcm_file = sys.argv[1]

logger = logging.getLogger()

"""

1. 连接 ws_app.run_forever()
2. 连接成功后发送数据 on_open()
2.1 发送开始参数帧 send_start_params()
2.2 发送音频数据帧 send_audio()
2.3 库接收识别结果 on_message()
2.4 发送结束帧 send_finish()
3. 关闭连接 on_close()

库的报错 on_error()
"""


def send_start_params(ws):
    """
    开始参数帧
    :param websocket.WebSocket ws:
    :return:
    """
    req = {
        "type": "START",
        "data": {
            "appid": 32070102,  # 网页上的appid
            "appkey": "1y7QlXXsL3GN8XRw9svhzg11",  # 网页上的appid对应的appkey
            "dev_pid": 15372,  # 识别模型
            "cuid": "yourself_defined_user_id",  # 随便填不影响使用。机器的mac或者其它唯一id，百度计算UV用。
            "sample": 16000,  # 固定参数
            "format": "pcm"  # 固定参数
        }
    }
    body = json.dumps(req)
    ws.send(body, websocket.ABNF.OPCODE_TEXT)
    logger.info("send START frame with params:" + body)

def send_audio(ws):
    """
    实时获取麦克风音频数据并发送
    :param  websocket.WebSocket ws:
    :return:
    """
    chunk_ms = 160  # 160ms的录音
    chunk_len = int(16000 * 2 / 1000 * chunk_ms)

    # 设置音频输入参数
    pa = pyaudio.PyAudio()
    stream = pa.open(format=pyaudio.paInt16,
                     channels=1,
                     rate=16000,
                     input=True,
                     frames_per_buffer=chunk_len)

    try:
        while True:
            # 实时读取麦克风音频数据
            pcm = stream.read(chunk_len)
            # 发送音频数据
            ws.send(pcm, websocket.ABNF.OPCODE_BINARY)
            time.sleep(chunk_ms / 1000.0)  # ws.send 也有点耗时，这里没有计算
    except KeyboardInterrupt:
        # 捕获 Ctrl+C 信号，停止读取音频并发送结束帧
        send_finish(ws)
    finally:
        # 关闭音频输入流
        stream.stop_stream()
        stream.close()
        pa.terminate()


def send_finish(ws):
    """
    发送结束帧
    :param websocket.WebSocket ws:
    :return:
    """
    req = {
        "type": "FINISH"
    }
    body = json.dumps(req)
    ws.send(body, websocket.ABNF.OPCODE_TEXT)
    logger.info("send FINISH frame")


def send_cancel(ws):
    """
    发送取消帧
    :param websocket.WebSocket ws:
    :return:
    """
    req = {
        "type": "CANCEL"
    }
    body = json.dumps(req)
    ws.send(body, websocket.ABNF.OPCODE_TEXT)
    logger.info("send Cancel frame")


def on_open(ws):
    """
    连接后发送数据帧
    :param  websocket.WebSocket ws:
    :return:
    """

    def run(*args):
        """
        发送数据帧
        :param args:
        :return:
        """
        send_start_params(ws)
        send_audio(ws)
        send_finish(ws)
        logger.debug("thread terminating")

    threading.Thread(target=run).start()


def on_message(ws, message):
    """
    接收服务端返回的消息
    :param ws:
    :param message: json格式，自行解析
    :return:
    """
    logger.info("Response: " + message)
    # 解析消息中的 JSON 数据
    data = json.loads(message)

    # 取出 result 字段
    result = data['result']
    print(result)

def on_error(ws, error):
    """
    库的报错，比如连接超时
    :param ws:
    :param error: json格式，自行解析
    :return:
        """
    logger.error("error: " + str(error))


def on_close(ws):
    """
    Websocket关闭
    :param websocket.WebSocket ws:
    :return:
    """
    logger.info("ws close ...")
    # ws.close()


if __name__ == "__main__":
    logging.basicConfig(format='[%(asctime)-15s] [%(funcName)s()][%(levelname)s] %(message)s')
    logger.setLevel(logging.INFO)  # 调整为logging.INFO，日志会少一点
    logger.info("begin")
    # websocket.enableTrace(True)
    uri = "ws://vop.baidu.com/realtime_asr" + "?sn=" + str(uuid.uuid1())
    logger.info("uri is "+ uri)
    ws_app = websocket.WebSocketApp(uri,
                                    on_open=on_open,  # 连接建立后的回调
                                    on_message=on_message,  # 接收消息的回调
                                    on_error=on_error,  # 库遇见错误的回调
                                    on_close=on_close)  # 关闭后的回调
    ws_app.run_forever()
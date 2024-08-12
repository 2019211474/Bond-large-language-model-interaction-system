import wave
import threading
import http.client
import os
import time
from modelscope.pipelines import pipeline
from modelscope.utils.constant import Tasks
import torch
# 录音参数设置
CHANNELS = 1
RATE = 16000
CHUNK = 1024

if torch.cuda.is_available():
    device_id = "cuda:0"
else:
    device_id = "cpu"


# 第一次运行时需要连接一下网络
inference_pipeline = pipeline(
    task=Tasks.auto_speech_recognition,
    model='damo/speech_paraformer-large-vad-punc_asr_nat-zh-cn-16k-common-vocab8404-pytorch',
    model_revision="v1.2.4",
    device=device_id
)


def asr(audioFile_path) :
    # 读取音频文件
    rec_result = inference_pipeline(audio_in=audioFile_path)
    rec_return = rec_result["text"]

    return rec_return

if __name__ == '__main__':

    # 记录开始时间
    start_time = time.time()
    # 编写您的程序或函数

    # 音频文件
    audioFile = "/home/yuankun/project/债券关系大模型问答/output.wav"
    result = asr(audioFile)
    print(type(result))
    print(result)

    # 记录结束时间
    end_time = time.time()
    # 计算运行时间
    elapsed_time = end_time - start_time
    print(f"程序运行时间：{elapsed_time} 秒")


from flask import Flask, Response, request, jsonify, stream_with_context, render_template, session
import requests
import os
import uuid
from recorder import Recoder
import client_asr
import client_tts
import re
from play_audio import Player

recoder=Recoder()
player_last=Player()

app = Flask(__name__)
app.secret_key = "123456"
app.config['SESSION_TYPE'] = 'filesystem'
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/stream', methods=['GET', 'POST'])
def stream():

    print('收到前端SEND指令')

    global player_last

    if player_last:
        try:
            player_last.refresh_sound()
            print("调用成功")
            player_last.stop()
            del player_last
        except Exception as e:
            print("except")
            print(e)
            pass


    if 'session_id' not in session:
        session_id = str(uuid.uuid4())
        session['session_id'] = session_id
    session_id = session['session_id']
    text = request.args.get('text')
    params = {'text':text,'session_id': session_id}
    print(text, session_id)
    response = requests.post(url='http://10.200.90.59:8887/api/v1/', params=params, stream=True)
    # response = requests.post(url='http://10.200.90.59:8887/api/v1/', json={'text': text})
    # response = get_result(params)
    player_current = Player()
    player_current.t1.start()
    player_current.t2.start()


    def generator_result():
        read_sentence=""
        pattern = r"[，。！？?!,]"
        for chunk in response.iter_content(chunk_size=20, decode_unicode=True):
            if chunk:
                yield chunk
                read_sentence += chunk.strip()

                if len(re.findall(pattern,read_sentence))>=1:
                    remain_sentence = re.split(pattern, read_sentence)[-1]
                    read_sentence = read_sentence.replace(remain_sentence, "")
                    player_current.add(read_sentence)
                    read_sentence=remain_sentence

            else:
                client_tts.client_tts(read_sentence,"zhigui")



    player_last = player_current

    return Response(stream_with_context(generator_result()), content_type=response.headers["Content-Type"])


# def get_result(params):
#
#     url = 'http://10.200.90.59:8887/api/v1/'
#     response = requests.get(url, params=params)
#
#     return response

@app.route('/cancel', methods=['GET', 'POST'])
def cancel():
    print('收到前端CANCEL指令')
    # 执行的逻辑
    return '200'

@app.route('/recordStart', methods=['POST','GET'])
def recordStart():

    print('收到前端recordStart指令')

    global player_last
    if player_last:
        try:
            player_last.stop()
        except Exception as e:
            print("except")
            print(e)
            pass
    print("没有毛病")
    recoder.start()
    # 执行的逻辑
    return '200'

@app.route('/recordStop', methods=['POST','GET'])
def recordStop():
    print('收到前端recordStop指令')
    recoder.stop()
    file_list=os.listdir()
    if "output.wav" in file_list:
        record_content=client_asr.client_asr("./output.wav")
        return record_content
    else:
        return "未识别，请重新录音"
    # 执行的逻辑


@app.route('/recordLeave', methods=['POST','GET'])
def recordLeave():
    print('收到前端recordLeave指令')
    recoder.refresh()
    # 执行的逻辑
    return '200'


if __name__ == '__main__':
    app.run()
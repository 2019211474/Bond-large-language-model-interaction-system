import sys
sys.path.append("/home/yuankun/debt/")
from src.utils import Neo4jConnect
# 初始化Neo4j的连接
Neo4jConnect(config_path="./utils/config.yml")
from src.service import Neo4jService
from flask import Flask, request, Response, jsonify
from blueprint import neo4j_blueprint
from src.债券关系大模型问答.debt_llm import DebtLLM

app: Flask = Flask(__name__)

# 注册子路由
app.register_blueprint(neo4j_blueprint)

ns = Neo4jService()
dllm = DebtLLM()


@app.route("/api/v1/", methods=["GET", "POST"])

def get_result():

    if request.method == 'POST':
        params = request.args
        print("-----------post",params)
        user_query = params.get('text')
        output = dllm.process_control(ns, user_query)
        if type(output) is str:
            output_type = "string"
        else:
            output = dict(output)
            output_type = "neo4j_data"

        response_data = {
            'output': output,
            'output_type': output_type
        }
        return jsonify(response_data)


# 用户如果要放弃本次查询，需要设置初始化按钮并执行该方法
@app.route("/api/v2/", methods=["POST"])
def refresh_chat():
    dllm.refresh()

if __name__ == "__main__":

    app.run(host="10.200.90.59", port=8887)

# 收到语音转为文本后，只需调用这一个方法即可，str代表返回字符串, data代表返回neo4j数据，查询成功后自动初始化
# @app.route("/api/v1/", methods=["POST","GET"])
# @app.route("/api/v1/", methods=["POST"])
#
# def get_result():
#     if request.method == 'POST':
#         params = request.args
#         user_query = params.get('text')
#         output = dllm.process_control(ns, user_query)
#         if type(output) is str:
#             output_type = "string"
#         else:
#             output = dict(output)
#             output_type = "neo4j_data"
#
#         response_data = {
#             'output': output,
#             'output_type': output_type
#         }
#
#         print(response_data)
#         return jsonify(output, response_data)
#
#     elif request.method == 'GET':
#         data = request.data
#         print(data)
#         return ''

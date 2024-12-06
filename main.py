from crypt import methods
from dataclasses import field
import requests
from flask import Flask, request, jsonify
import mysql.connector
import json
import dashscope
from openai import OpenAI
from pydantic.v1.schema import field_type_schema

from db import DataSourceQuery  # 导入DataSourceQuery类
app = Flask( __name__ )
# def get_db_connection():
#     return mysql.connector.connect(
#         host='103.71.69.160',
#         port=3306,
#         user='root',
#         password='Wing1Q2W#E',
#         database='AI_test_tool'
#     )
@app.route('/dify/rules',methods=['POST'])#给dify传参数
def dify_rules():
    try:
        req_data = request.get_json()
        datasource_type = req_data.get('datasourcetype')
        datasource_name = req_data.get('datasourcename')
        schema_name = req_data.get('schemaname')
        table_name = req_data.get('tablename')
        if not all([datasource_type, datasource_name, schema_name, table_name]):
            return jsonify({
                "success": False,
                "errcode": 3,
                "message": "缺少必要参数",
                "data": []
            }), 400
        config = {
            "datasourcetype": datasource_type,
            "datasourcename": datasource_name,
            "schemaname": schema_name,
            "tablename": table_name
        }
        query_tool = DataSourceQuery(config)
        field_data_json=query_tool.field_data_json()#查询100条数据
        field_type_json=query_tool.field_type_json()#查询字段信息
        # 替换为你的 Dify 工作流的 URL
        workflow_url = 'https://dify.edstars.com.cn/v1'
        # 替换为你的 API 密钥
        api_key = 'YOUR_API_KEY'
        # 设置请求头
        headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json'
        }
        # 设置传递给工作流的参数
        data = {
            'field_type_json': {field_type_json},
            'field_data_json': {field_data_json},
            'table_name': {table_name},
            'schema_name': {schema_name}
        }
        # 发送 POST 请求调用工作流
        response = requests.post(workflow_url, headers=headers, json=data)
        # 检查响应状态码
        if response.status_code == 200:
            print('工作流调用成功')
            print('响应数据:', response.json())
        else:
            print('工作流调用失败')
            print('状态码:', response.status_code)
            print('响应内容:', response.text)
    except Exception as e:
        return jsonify({
            "success": False,
            "errcode": 2,
            "message": str(e),
            "data": []
        }), 500


# @app.route( '/data/fields', methods=['POST'] )
# def data_fields():
#     try:
#         req_data = request.get_json()
#         datasource_type = req_data.get( 'datasourcetype' )
#         datasource_name = req_data.get( 'datasourcename' )
#         schema_name = req_data.get( 'schemaname' )
#         table_name = req_data.get( 'tablename' )
#
#         if not all( [datasource_type, datasource_name, schema_name, table_name] ):
#             return jsonify( {
#                 "success": False,
#                 "errcode": 3,
#                 "message": "缺少必要参数",
#                 "data": []
#             } ), 400
#         config = {
#             "datasourcetype": datasource_type,
#             "datasourcename": datasource_name,
#             "schemaname": schema_name,
#             "tablename": table_name
#         }
#         ds_query = DataSourceQuery(config)
#
#     except Exception as e:
#         return jsonify( {
#             "success": False,
#             "errcode": 2,
#             "message": str( e ),
#             "data": []
#         } ), 500
#
# # @app.route( '/data/rules', methods=['POST'] )
# def generate_ai_rules():
#     try:
#         req_data = request.get_json()
#         tablename = req_data.get( 'tablename' )
#         schema_name = req_data.get('schemaname')
#
#         if not all( [tablename, schema_name] ):
#             return jsonify( {
#                 "success": False,
#                 "errcode": 3,
#                 "message": "缺少必要参数",
#                 "data": []
#             } ), 400
#         field
#         client = OpenAI(
#             api_key="sk-f1846423eb074c81925d5674d35a5846",
#             base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
#         )
#
#         prompt = f"""
#         根据以下表格信息生成AI规则：
#         表名: {tablename}
#         表注释: {table_comment}
#         字段信息: {json.dumps( validation_rule )}
#
#         规则要求：
#         1. 根据字段类型和注释生成相应的验证规则。
#         2. 常见的规则包括但不限于：NotNull, MinValue, MaxValue, LengthRange, Format等。
#         3. 返回格式如下：
#         [
#             {{
#                 "id": "{validation_rule[0]['id']}",
#                 "name": "{validation_rule[0]['name']}",
#                 "type": "{validation_rule[0]['type']}",
#                 "comment": "{validation_rule[0]['comment']}",
#                 "Ai_rule": [
#                     {{
#                         "field_name": "price_type",
#                         "field_type": "NotNull",
#                         "NotNull": {{}}
#                     }}
#                 ],
#                 "field_yuan": {json.dumps( validation_rule[0].get( 'field_yuan', [] ) )}
#             }},
#             ...
#         ]
#         """
#
#         completion = client.chat.completions.create(
#             model="qwen-plus",
#             messages=[
#                 {'role': 'system', 'content': 'You are a helpful assistant.'},
#                 {'role': 'user', 'content': prompt}
#             ],
#         )
#
#         content = completion.choices[0].message.content.strip()
#
#         # 提取JSON内容
#         try:
#             start_index = content.find( '[' )
#             end_index = content.rfind( ']' ) + 1
#             ai_rules_str = content[start_index:end_index]
#             ai_rules = json.loads( ai_rules_str )
#         except (IndexError, json.JSONDecodeError) as e:
#             return jsonify( {
#                 "success": False,
#                 "errcode": 2,
#                 "message": f"无法解析AI规则: {str( e )}",
#                 "data": []
#             } ), 500
#
#         response = {
#             "success": True,
#             "errcode": 0,
#             "message": "成功",
#             "data": ai_rules
#         }
#
#         return jsonify( response )
#
#     except Exception as e:
#         return jsonify( {
#             "success": False,
#             "errcode": 2,
#             "message": str( e ),
#             "data": []
#         } ), 500
#
# @app.route('/data/sample', methods=['POST'])  # 新增接口,查询表前100条数据
# def get_sample_data():
#     try:
#         req_data = request.get_json()
#         config = {
#             "datasourcetype": req_data.get('datasourcetype'),
#             "datasourcename": req_data.get('datasourcename'),
#             "schemaname": req_data.get('schemaname'),
#             "tablename": req_data.get('tablename')
#         }
#
#         if not all(config.values()):
#             return jsonify({
#                 "success": False,
#                 "errcode": 3,
#                 "message": "缺少必要参数",
#                 "data": []
#             }), 400
#
#         query_tool = DataSourceQuery(config)
#         sample_data = query_tool.field_data_json()
#
#         response = {
#             "success": True,
#             "errcode": 0,
#             "message": "成功",
#             "data": sample_data
#         }
#
#         return jsonify(response)
#
#     except Exception as e:
#         return jsonify({
#             "success": False,
#             "errcode": 2,
#             "message": str(e),
#             "data": []
#         }), 500
# @app.route('/data/ruleszhun', methods=['POST'])
# def generate_ai_rules_with_data():
#     try:
#         req_data = request.get_json()
#         config = {
#             "datasourcetype": req_data.get('datasourcetype'),
#             "datasourcename": req_data.get('datasourcename'),
#             "schemaname": req_data.get('schemaname'),
#             "tablename": req_data.get('tablename')
#         }
#         table_comment = req_data.get('table_comment')
#         validation_rule = req_data.get('validation_rule')
#
#         if not all(config.values()) or not table_comment or not validation_rule:
#             return jsonify({
#                 "success": False,
#                 "errcode": 3,
#                 "message": "缺少必要参数",
#                 "data": []
#             }), 400
#
#         # 查询前100条数据
#         query_tool = DataSourceQuery(config)
#         sample_data = query_tool.field_data_json()
#
#         prompt = f"""
#         根据以下表格信息生成AI规则：
#         表名: {config['tablename']}
#         表注释: {table_comment}
#         字段信息: {json.dumps(validation_rule)}
#         参考表数据:{json.dumps(sample_data)}
#         规则要求：
#         1. 根据字段类型和注释生成相应的验证规则。
#         2. 常见的规则包括但不限于：NotNull, MinValue, MaxValue, LengthRange, Format等。
#         3. 返回格式如下：
#         [
#             {{
#                 "id": "{validation_rule[0]['id']}",
#                 "name": "{validation_rule[0]['name']}",
#                 "type": "{validation_rule[0]['type']}",
#                 "comment": "{validation_rule[0]['comment']}",
#                 "Ai_rule": [
#                     {{
#                         "field_name": "price_type",
#                         "field_type": "NotNull",
#                         "NotNull": {{}}
#                     }}
#                 ],
#                 "field_yuan": {json.dumps(validation_rule[0].get('field_yuan', []))}
#             }},
#             ...
#         ]
#         """
#
#         response = dashscope.Generation.call(
#             api_key="sk-f1846423eb074c81925d5674d35a5846",
#             model="qwen-plus",
#             messages=[
#                 {'role': 'system', 'content': 'You are a helpful assistant.'},
#                 {'role': 'user', 'content': prompt}
#             ],
#             result_format='message'
#         )
#
#         content = response.output.choices[0].message.content.strip()
#
#         # 提取JSON内容
#         try:
#             start_index = content.find('[')
#             end_index = content.rfind(']') + 1
#             ai_rules_str = content[start_index:end_index]
#             ai_rules = json.loads(ai_rules_str)
#         except (IndexError, json.JSONDecodeError) as e:
#             return jsonify({
#                 "success": False,
#                 "errcode": 2,
#                 "message": f"无法解析AI规则: {str(e)}",
#                 "data": []
#             }), 500
#
#         response_json = {
#             "success": True,
#             "errcode": 0,
#             "message": "成功",
#             "data": ai_rules
#         }
#
#         return jsonify(response_json)
#
#     except Exception as e:
#         return jsonify({
#             "success": False,
#             "errcode": 2,
#             "message": str(e),
#             "data": []
#         }), 500
if __name__ == '__main__':
    app.run( host='127.0.0.1', port=3001 )




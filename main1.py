from flask import Flask, request, jsonify
import mysql.connector
import json
from openai import OpenAI

app = Flask(__name__)

def get_db_connection():
    return mysql.connector.connect(
        host='103.71.69.160',
        port=3306,
        user='root',
        password='Wing1Q2W#E',
        database='AI_test_tool'
    )

@app.route('/data/fields', methods=['POST'])
def data_fields():
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

        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT columns, primary_keys, distribution_key, partition_key
            FROM datamap_table_metadata 
            WHERE datasource_type = %s 
              AND datasource_name = %s 
              AND schema_name = %s 
              AND table_name = %s
        """
        cursor.execute(query, (datasource_type, datasource_name, schema_name, table_name))
        result = cursor.fetchone()

        if result and result['columns'] is not None:
            # 安全地解析 JSON 字符串
            try:
                columns = json.loads(result['columns'])
            except json.JSONDecodeError as e:
                return jsonify({
                    "success": False,
                    "errcode": 2,
                    "message": f"无法解析列数据: {str(e)}",
                    "data": []
                }), 500

            primary_keys = set(result['primary_keys'].split(',')) if result['primary_keys'] else set()
            distribution_keys = set(result['distribution_key'].split(',')) if result['distribution_key'] else set()
            partition_keys = set(result['partition_key'].split(',')) if result['partition_key'] else set()
            for column in columns:
                keys_info = []
                if column['name'] in primary_keys:
                    keys_info.append({"field_name": "primary_keys", "field_type": "primary_keys"})
                if column['name'] in distribution_keys:
                    keys_info.append({"field_name": "distribution_key", "field_type": "distribution_key"})
                if column['name'] in partition_keys:
                    keys_info.append({"field_name": "partition_key", "field_type": "partition_key"})
                column['field_yuan'] = keys_info

            response = {
                "success": True,
                "errcode": 0,
                "message": "成功",
                "data": columns
            }
        else:
            response = {
                "success": False,
                "errcode": 1,
                "message": "未找到相关表信息",
                "data": []
            }

        cursor.close()
        conn.close()

        return jsonify(response)

    except Exception as e:
        return jsonify({
            "success": False,
            "errcode": 2,
            "message": str(e),
            "data": []
        }), 500

@app.route('/data/rules', methods=['POST'])
def generate_ai_rules():
    try:
        req_data = request.get_json()
        tablename = req_data.get('tablename')
        table_comment = req_data.get('table_comment')
        validation_rule = req_data.get('validation_rule')

        if not all([tablename, table_comment, validation_rule]):
            return jsonify({
                "success": False,
                "errcode": 3,
                "message": "缺少必要参数",
                "data": []
            }), 400

        client = OpenAI(
            api_key="sk-f1846423eb074c81925d5674d35a5846",
            base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
        )

        prompt = f"""
        根据以下表格信息生成AI规则：
        表名: {tablename}
        表注释: {table_comment}
        字段信息: {json.dumps(validation_rule)}

        规则要求：
        1. 根据字段类型和注释生成相应的验证规则。
        2. 常见的规则包括但不限于：NotNull, MinValue, MaxValue, LengthRange, Format等。
        3. 返回格式如下：
        [
            {{
                "id": "{validation_rule[0]['id']}",
                "name": "{validation_rule[0]['name']}",
                "type": "{validation_rule[0]['type']}",
                "comment": "{validation_rule[0]['comment']}",
                "Ai_rule": [
                    {{
                        "field_name": "price_type",
                        "field_type": "NotNull",
                        "NotNull": {{}}
                    }}
                ],
                "field_yuan": {json.dumps(validation_rule[0].get('field_yuan', []))}
            }},
            ...
        ]
        """

        completion = client.chat.completions.create(
            model="qwen-plus",
            messages=[
                {'role': 'system', 'content': 'You are a helpful assistant.'},
                {'role': 'user', 'content': prompt}
            ],
        )

        content = completion.choices[0].message.content.strip()

        # 提取JSON内容
        try:
            start_index = content.find('[')
            end_index = content.rfind(']') + 1
            ai_rules_str = content[start_index:end_index]
            ai_rules = json.loads(ai_rules_str)
        except (IndexError, json.JSONDecodeError) as e:
            return jsonify({
                "success": False,
                "errcode": 2,
                "message": f"无法解析AI规则: {str(e)}",
                "data": []
            }), 500

        response = {
            "success": True,
            "errcode": 0,
            "message": "成功",
            "data": ai_rules
        }

        return jsonify(response)

    except Exception as e:
        return jsonify({
            "success": False,
            "errcode": 2,
            "message": str(e),
            "data": []
        }), 500

@app.route('/data/table_info', methods=['POST'])
def get_table_info():
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

        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT table_name, table_comment
            FROM datamap_table_metadata 
            WHERE datasource_type = %s 
              AND datasource_name = %s 
              AND schema_name = %s 
              AND table_name = %s
        """
        cursor.execute(query, (datasource_type, datasource_name, schema_name, table_name))
        result = cursor.fetchone()

        if result:
            response = {
                "success": True,
                "errcode": 0,
                "message": "成功",
                "data": {
                    "tablename": result['table_name'],
                    "table_comment": result['table_comment']
                }
            }
        else:
            response = {
                "success": False,
                "errcode": 1,
                "message": "未找到相关表信息",
                "data": {}
            }

        cursor.close()
        conn.close()

        return jsonify(response)

    except Exception as e:
        return jsonify({
            "success": False,
            "errcode": 2,
            "message": str(e),
            "data": {}
        }), 500

@app.route('/task/addtask', methods=['POST'])
def add_task():
    try:
        req_data = request.get_json()

        task_name = req_data.get('task_name')
        task_type = req_data.get('task_type')
        rules = req_data.get('rules')
        period_time = req_data.get('period_time')
        task_status = req_data.get('task_status')
        task_execute_type = req_data.get('task_execute_type')
        creater = req_data.get('creater')
        create_at = req_data.get('create_at')
        update_at = req_data.get('update_at')

        if not all([
            task_name, task_type, rules, period_time, task_status,
            task_execute_type, creater, create_at, update_at
        ]):
            return jsonify({
                "success": False,
                "errcode": 3,
                "message": "缺少必要参数",
                "data": []
            }), 400

        conn = get_db_connection()
        cursor = conn.cursor()

        query = """
            INSERT INTO execute_tasks (
                task_name, task_type, rules, period_time, task_status,
                task_execute_type, creater, create_at, update_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query, (
            task_name, task_type, json.dumps(rules), period_time, task_status,
            task_execute_type, creater, create_at, update_at
        ))

        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({
            "success": True,
            "errcode": 0,
            "message": "任务添加成功",
            "data": {}
        })

    except Exception as e:
        return jsonify({
            "success": False,
            "errcode": 2,
            "message": str(e),
            "data": {}
        }), 500
if __name__ == '__main__':
    app.run( host='127.0.0.1', port=3001 )




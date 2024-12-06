import pymysql
from decimal import Decimal
import json
from flask import Flask, request, jsonify

class DataSourceQuery:
    def __init__(self, config):
        self.config = config
        self.connection = None

    def connect(self):
        if self.config["datasourcetype"] == "mysql":
            self.connection = pymysql.connect(
                host='103.71.69.160',
                user='root',
                password='Wing1Q2W#E',
                db=self.config["schemaname"],
                port=3306,
                cursorclass=pymysql.cursors.DictCursor
            )
        else:
            raise ValueError("Unsupported data source type")

    def field_data_json(self):
        if not self.connection:
            self.connect()

        table_name = self.config["tablename"]
        full_table_name = f"{table_name}"

        try:
            with self.connection.cursor() as cursor:
                sql = f"SELECT * FROM {full_table_name} LIMIT 100"
                cursor.execute(sql)
                result = cursor.fetchall()

                # Convert Decimal to string
                for row in result:
                    for key, value in row.items():
                        if isinstance(value, Decimal):
                            row[key] = str(value)

                return result
        finally:
            self.connection.close()

    def field_type_json(self):
        if not self.connection:
            self.connect()
        datasource_type = self.config["datasourcetype"]
        datasource_name = self.config["datasourcename"]
        schema_name = self.config["schemaname"]
        table_name = self.config["tablename"]

        try:
            with self.connection.cursor() as cursor:
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
                self.connection.close()

                return jsonify(response)

        except Exception as e:
            return jsonify({
                "success": False,
                "errcode": 2,
                "message": str(e),
                "data": []
            }), 500

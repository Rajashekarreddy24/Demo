from dashboard.columns_extract import server_connection
from rest_framework import status
from rest_framework.generics import CreateAPIView
import ast, base64
from sqlalchemy import text
from dashboard.models import *
from rest_framework.response import Response
from .serializers import ChartCopilot
from dashboard.Connections import table_name_from_query
import json, requests

API_KEY = 'c2stcHJvai1VdUEwSVM0d1NMYlJGM2V6VFV0a1QzQmxia0ZKc1RxcFJhM3J4WVJmSklBM0dLSXA='

# Create your views here.
class GetServerTablesList(CreateAPIView):
    serializer_class = ChartCopilot
    
    def post(self, request):
        serializer = self.serializer_class(data=request.data)
        if serializer.is_valid(raise_exception=True):
            query_set_id = serializer.validated_data['id']
            user_prompt = serializer.validated_data['prompt']
            
            if QuerySets.objects.filter(queryset_id=query_set_id).exists():
                qs = QuerySets.objects.get(queryset_id=query_set_id)
                sd = ServerDetails.objects.get(id=qs.server_id)
                server_type = ServerType.objects.get(id=sd.server_type).server_type
                
                server_conn =server_connection(sd.username,sd.password,sd.database,sd.hostname,sd.port,sd.service_name,server_type.upper(),sd.database_path)
                if server_conn['status']==200:
                    engine=server_conn['engine']
                    cur=server_conn['cursor']
                    db_type = server_type
                else:
                    return Response(server_conn,status=server_conn['status'])
                # if server_type == 'POSTGRESQL':
                #     conn = connect_postgresql_server(sd.username, sd.password, sd.database, sd.hostname, sd.port)
                # elif server_type == 'ORACLE':
                #     conn = connect_oracle_server(sd.username, sd.password, sd.hostname, sd.port, sd.service_name)
                # elif server_type == 'MYSQL':
                #     conn = connect_mysql_server(sd.username, sd.password, sd.hostname, sd.port, sd.database)
                # else:
                #     return Response({"message": "Unsupported server type"}, status=status.HTTP_400_BAD_REQUEST)
                
                # if conn['status'] == 200:
                #     engine = conn['engine']
                #     cur = conn['cursor']
                #     db_type = server_type
                # else:
                #     return Response({"message": conn['message']}, status=status.HTTP_400_BAD_REQUEST)
                
                d = fetch_tables_from_query(qs)
                tables = d['tables']
                result = get_table_meta_data(engine, tables, db_type)
                op = ast.literal_eval(str(result['data']))
                
                aa = can_build_chart(op, user_prompt)
                if aa != "YES":
                    return Response({
                        "data": "The question <b>{}</b> doesn't seem to match any of the keywords in the provided table metadata. <br><br>Could you please rephrase the question and ask again ?".format(str(user_prompt))
                    })
                
                # Initial attempt to correct the format
                formatted_data = correct_format(op)
                if formatted_data['status'] == "error":
                    # Retry getting GPT chart suggestions if correct_format fails
                    final_res = get_gpt_chart_suggestions(op, user_prompt)
                    if 'error' in final_res:
                        return Response({"message": final_res['error']}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
                    
                    # Try to correct the format again with new suggestions
                    text = final_res['choices'][0]['message']['content']
                    data = json.loads(text)
                    formatted_data = correct_format(data)
                    if formatted_data['status'] == "error":
                        return Response({"message": "Error generating chart suggestions. Please try again later."}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
                    else:
                        data = formatted_data['data']
                else:
                    data = formatted_data['data']
                    
                final_data = []
                for chart in data['charts']:
                    new_data = {
                        "chart_title": chart['chart_title'],
                        "chart_type": chart['chart_type'],
                        "database_id": qs.server_id,
                        "queryset_id": qs.queryset_id,
                        "col": chart['col'],
                        "row": chart['row'],
                        "filter_id": [],
                        "columns": [{
                            "column": chart['col'][0][0],
                            "type": chart['col'][0][1]
                        }],
                        "rows": [{
                            "column": chart['row'][0][0],
                            "type": chart['row'][0][2]
                        }],
                        "datasource_quertsetid": "",
                        "sheetfilter_querysets_id": ""
                    }
                    final_data.append(new_data)
                return Response({"data": final_data})
            else:
                return Response({'message': "Invalid QuerySet ID"}, status=status.HTTP_404_NOT_FOUND)
        else:
            return Response(serializer.error_messages, status=status.HTTP_400_BAD_REQUEST)


def correct_format(data):
    try:
        for chart in data['charts']:
            for row in chart['row']:
                if len(row) == 3 and row[1] in ["avg", "sum", "min", "max", "count"] and row[2] == "":
                    row[1], row[2] = "aggregate", row[1]
                elif len(row) == 2 and row[1] in ["avg", "sum", "min", "max", "count"]:
                    row.append(row[1])
                    row[1] = "aggregate"
        return {"status": "success", "data": data}
    except Exception as e:
        return {"status": "error"}
    
def decode_string(encoded_string):
    decoded_bytes = base64.b64decode(encoded_string.encode('utf-8'))
    decoded_string = decoded_bytes.decode('utf-8')
    return decoded_string

def fetch_tables_from_query(query):
    try:
        d = table_name_from_query(query)
        return d
    except Exception as e:
        return str(e)

def get_table_meta_data(engine, tables_list, database_type):
    try:
        if database_type == 'POSTGRESQL':
            placeholders = ', '.join([':{}'.format(i) for i in range(len(tables_list))])
            query = text(f'''SELECT table_name, column_name, data_type
                             FROM information_schema.columns
                             WHERE table_name IN ({placeholders})''')
            params = {str(i): table for i, table in enumerate(tables_list)}
            cursor = engine.connect()
            c = cursor.execute(query, params)
            rows = c.fetchall()
            meta_data = [row for row in rows]
            return {"data": meta_data}
        
        elif database_type == 'ORACLE':
            placeholders = ', '.join([':{}'.format(i) for i in range(len(tables_list))])
            query = text(f'''SELECT table_name, column_name, data_type
                             FROM all_tab_columns
                             WHERE table_name IN ({placeholders})''')
            params = {str(i): table.upper() for i, table in enumerate(tables_list)}
            cursor = engine.connect()
            c = cursor.execute(query, params)
            rows = c.fetchall()
            meta_data = [row for row in rows]
            return {"data": meta_data}
        
        elif database_type == 'MYSQL':
            return "Connection is Pending"
        else:
            return "Selected Existing Connections"
    except Exception as e:
        return str(e)

format_response = """{
    # use this keys INTEGER, VARCHAR, BOOLEAN, TIMESTAMPTZ for "datatype"
    # INTEGER=['numeric','int','float','number','double precision','smallint','integer','bigint','decimal','numeric','real','smallserial','serial','bigserial','binary_float','binary_double']
    # VARCHAR=['varchar','bp char','text','varchar2','NVchar2','long','char','Nchar','character varying']
    # BOOLEAN=['bool','boolean']
    # TIMESTAMPTZ=['date','time','datetime','timestamp','timestamp with time zone','timestamp without time zone','timezone','time zone'] 
    # Create a Json Body with Specific requiremnt Mentioned accordingly and check String Values in Columns and Numbers in Rows, Check Datatypes mentioned above and Dont Mention existing same column_name in single charts rows and columns
  "charts": [
    {
      "chart_type": "Chart Type",
      "chart_title": "Chart Description",
        # Dont Repeat same column name used in row and col list
      "row": [
        # Accept ONly INTEGER Datatype Columns in row list
        # Get Row data List should contain, index 0 with column_name, pass "aggregate" in index 1 and index 2 with any of this sum or avg or count or min or max
        [column_name,"aggregate",sum/avg/count/min/max],[column_name,"aggregate",sum/avg/count/min/max]
      ],
      # Accept Only VARCHAR, BOOLEANS AND TIMESTAMPTZ Datatype Columns in col list 
      "col": [ 
        [column_name,column datatype,""],[column_name,column datatype,""]
      ]
    }
  ]
}"""    

def can_build_chart(meta_data, prompt):
    if prompt is None:
        return "YES"
    else:
        message = [{'role': 'user', 'content': f"Check whether we can build a chart based on {prompt} and {meta_data}. If we can build, respond with YES; if not, NO"}]
        try:
            KEY = decode_string(API_KEY)
            response = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {KEY}"
                },
                json={
                    "model": "gpt-3.5-turbo-0125",
                    "messages": message,
                    "temperature": 0.7
                }
            )
            # response.raise_for_status()
            response_json = response.json()
            return response_json['choices'][0]['message']['content']
        except requests.exceptions.RequestException as e:
            return {"error": "Error checking chart feasibility. Please try again later."}

def get_gpt_chart_suggestions(meta_data, prompt):
    if prompt is None:
        message = [{'role': 'user', 'content': f"Suggest me some proper charts based on the meta data provided {meta_data} in this format {format_response}"}]
    else:
        message = [{'role': 'user', 'content': f"Build {prompt} on the meta data provided {meta_data} in this format {format_response}"}]
    
    try:
        KEY = decode_string(API_KEY)
        response = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {KEY}"
            },
            json={
                "model": "gpt-3.5-turbo-0125",
                "messages": message,
                "temperature": 0.7
            }
        )
        response.raise_for_status()
        response_json = response.json()
        return response_json
    except requests.exceptions.RequestException as e:
        return {"error": "Error generating chart suggestions. Please try again later."}

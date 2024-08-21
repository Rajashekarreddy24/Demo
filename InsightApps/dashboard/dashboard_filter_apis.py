from rest_framework.generics import CreateAPIView
from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status
from django.db import transaction
from dashboard.views import test_token
import psycopg2,cx_Oracle
from dashboard import models,serializers    
import pandas as pd
from sqlalchemy import text,inspect
import numpy as np
from .models import *
import ast,re,itertools
from datetime import datetime
import boto3
import json
import requests
from project import settings
import io
from dashboard.columns_extract import server_connection
from django.core.paginator import Paginator
import sqlglot
import logging
from dashboard import roles,previlages

quotes = {
    'postgresql': ('"', '"'),
    'oracle': ('"', '"'),
    'mysql': ('`', '`'),
    'sqlite': ('"', '"'),
    'microsoftsqlserver': ('[', ']')
}
date_format_syntaxes = {
    'postgresql': lambda column: f"""to_char("{str(column)}", 'yyyy-mm-dd')""",
    'oracle': lambda column: f"""TO_CHAR("{str(column)}", 'YYYY-MM-DD')""",
    'mysql': lambda column: f"""DATE_FORMAT(`{str(column)}`, '%Y-%m-%d')""",
    'sqlite': lambda column: f"""strftime('%Y-%m-%d', "{str(column)}")""",
    'microsoftsqlserver': lambda column: f"""FORMAT([{str(column)}], 'yyyy-MM-dd')"""
}
class DashboardQSColumnAndSheetsPreview(CreateAPIView):
    serializer_class = serializers.DashboardpreviewSerializer

    def post(self, request, token):
        tok1 = test_token(token)
        if tok1['status'] != 200:
            return Response({"message": tok1['message']}, status=status.HTTP_404_NOT_FOUND)
        
        serializer = self.serializer_class(data=request.data)
        if not serializer.is_valid():
            return Response({'message': 'serializer error'}, status=status.HTTP_200_OK)
        
        dashboard_id = serializer.validated_data["dashboard_id"]

        if not dashboard_data.objects.filter(id=dashboard_id).exists():
            return Response({'message': "Invalid Dashboard ID"}, status=status.HTTP_404_NOT_FOUND)
        
        database_id = dashboard_data.objects.get(id=dashboard_id).server_id
        user_id = tok1['user_id']

        sheet_names = []
        f = get_dashboard_sheets(dashboard_id)
        for i in f:
            sheet_names.append({"id": sheet_data.objects.get(id=i).id, "name": sheet_data.objects.get(id=i).sheet_name})
        
        try:
            server_details = ServerDetails.objects.get(user_id=user_id, id=database_id)
            ServerType1 = ServerType.objects.get(id=server_details.server_type)
            dtype = ServerType1.server_type
            
            server_conn = server_connection(
                server_details.username, server_details.password, server_details.database,
                server_details.hostname, server_details.port, server_details.service_name,
                ServerType1.server_type.upper(), server_details.database_path
            )
            if server_conn['status'] == 200:
                engine = server_conn['engine']
                cursor = server_conn['cursor']
            else:
                return Response(server_conn, status=server_conn['status'])
        except ServerDetails.DoesNotExist:
            return Response({'message': 'Server details not found'}, status=status.HTTP_404_NOT_FOUND)
        except ServerType.DoesNotExist:
            return Response({'message': 'Server type not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'message': str(e)}, status=status.HTTP_400_BAD_REQUEST)

        try:
            qr = ''
            q_id = dashboard_data.objects.get(id=dashboard_id, user_id=user_id)
            query_id = q_id.queryset_id
            joining = QuerySets.objects.get(queryset_id=query_id, user_id=user_id)
            query_set = joining.custom_query
            try:
                datasource_id = DataSource_querysets.objects.get(queryset_id=query_id, user_id=user_id)
                datasource_query = datasource_id.custom_query
                qr += datasource_query
            except:
                qr += query_set
            
            if dtype.lower()== "microsoftsqlserver":
                data = cursor.execute(str(qr))
                samp = cursor.description
            else:
                data = cursor.execute(text(qr))
                samp = data.cursor.description
            type_code_to_name = get_columns_list(samp, dtype)
            print(type_code_to_name)
            columns_list = [
                {
                    "column_name": col[0],
                    "column_dtype": type_code_to_name.get(col[1], 'UNKNOWN')
                }
                for col in samp
            ]

            response_data = {
                "columns": columns_list,
            }

            return Response({"response_data": response_data, "sheets": sheet_names, "dashboard_id": dashboard_id, "server_id": database_id}, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({"message": str(e)}, status=status.HTTP_404_NOT_FOUND)

        

        
def get_dashboard_sheets(dashboard_id):
    try:
        dd = dashboard_data.objects.get(id=dashboard_id)
        sheet_ids = dd.sheet_ids
        return eval(sheet_ids)
    except:
        return Response({"message": "Invalid Dasbaord ID"}, status=status.HTTP_404_NOT_FOUND)
        
class DashboardFilterSave(CreateAPIView):
    serializer_class = serializers.Dashboardfilter_save
    def post(self, request, token):
        role_list=roles.get_previlage_id(previlage=[previlages.create_dashboard_filter])
        tok1 = roles.role_status(token,role_list)
        if tok1['status'] != 200:
            return Response({"message": tok1['message']}, status=status.HTTP_404_NOT_FOUND)
        serializer = self.serializer_class(data=request.data)
        if not serializer.is_valid(raise_exception=True):
            return Response({'message': 'serializer error'}, status=status.HTTP_204_NO_CONTENT)
        
        dashboard_id = serializer.validated_data["dashboard_id"]
        filter_name = serializer.validated_data["filter_name"]
        selected_column = serializer.validated_data["column"]
        sheets = serializer.validated_data["sheets"]
        datatype = serializer.validated_data["datatype"]       
        user_id = tok1['user_id']
        if dashboard_data.objects.filter(id = dashboard_id,user_id=user_id).exists():
           
            dash_filter = DashboardFilters.objects.create(
                user_id = user_id,
                dashboard_id = dashboard_id,
                sheet_id_list = sheets,
                filter_name = filter_name,
                column_name = selected_column,
                column_datatype = datatype
            )
            
            return Response({"dashboard_filter_id":dash_filter.id,
                            "dashboard_id":dashboard_id,
                            "filter_name":filter_name,
                            "selected_column":selected_column,
                            "sheets":sheets,
                            "datatype":datatype
                            })
        else:
            return Response({"message":"dashboard id not found"},status=status.HTTP_404_NOT_FOUND)
    
    serializer_get_clas = serializers.Dashboard_datapreviewSerializer
    def get(self,request,token):
        
        tok1 = test_token(token)
        if tok1['status'] != 200:
            return Response({"message": tok1['message']}, status=status.HTTP_404_NOT_FOUND)
        serializer = self.serializer_get_clas(data=request.data)
        if not serializer.is_valid(raise_exception=True):
            return Response({'message': 'serializer error'}, status=status.HTTP_204_NO_CONTENT)
        
        filter_id = serializer.validated_data["id"]
        if DashboardFilters.objects.filter(id= filter_id).exists():
            dash_filter = DashboardFilters.objects.get(id= filter_id)
            return Response({"dashboard_filter_id":dash_filter.id,
                                "dashboard_id":dash_filter.dashboard_id,
                                "filter_name":dash_filter.filter_name,
                                "selected_column":dash_filter.column_name,
                                "sheets":dash_filter.sheet_id_list,
                                "datatype":dash_filter.column_datatype
                                })
        else:
            return Response({"message":"dashboard filter id not found"},status=status.HTTP_404_NOT_FOUND)
        
    serializer_put_class = serializers.Dashboardfilter_save
    def put(self, request, token):
        role_list=roles.get_previlage_id(previlage=[previlages.edit_dashboard_filter])
        tok1 = roles.role_status(token,role_list)
        if tok1['status'] != 200:
            return Response({"message": tok1['message']}, status=status.HTTP_404_NOT_FOUND)
        serializer = self.serializer_put_class(data=request.data)
        if not serializer.is_valid(raise_exception=True):
            return Response({'message': 'serializer error'}, status=status.HTTP_204_NO_CONTENT)
        
        dashboard_filter_id = serializer.validated_data["dashboard_filter_id"]
        dashboard_id = serializer.validated_data["dashboard_id"]
        filter_name = serializer.validated_data["filter_name"]
        selected_column = serializer.validated_data["column"]
        sheets = serializer.validated_data["sheets"]
        datatype = serializer.validated_data["datatype"]       
        user_id = tok1['user_id']
        if not DashboardFilters.objects.filter(id = dashboard_filter_id).exists():
            return Response({"message":"dashboard filter id not found"},status=status.HTTP_404_NOT_FOUND)
        
        if dashboard_data.objects.filter(id = dashboard_id,user_id=user_id).exists():
            DashboardFilters.objects.filter(
                id = dashboard_filter_id
                ).update(
                user_id = user_id,
                dashboard_id = dashboard_id,
                sheet_id_list = sheets,
                filter_name = filter_name,
                column_name = selected_column,
                column_datatype = datatype,
                updated_at = datetime.now()
            )

            return Response({"dashboard_filter_id":dashboard_filter_id,
                            "dashboard_id":dashboard_id,
                            "filter_name":filter_name,
                            "selected_column":selected_column,
                            "sheets":sheets,
                            "datatype":datatype
                            })
        else:
            return Response({"message":"dashboard id not found"},status=status.HTTP_404_NOT_FOUND)
        
    # serializer_delete_clas = serializers.Dashboard_datapreviewSerializer
    # def delete(self,request,token,filter_id):
        
    #     tok1 = test_token(token)
    #     if tok1['status'] != 200:
    #         return Response({"message": tok1['message']}, status=status.HTTP_404_NOT_FOUND)
    #     # # serializer = self.serializer_delete_clas(data=request.data)
    #     # if not serializer.is_valid(raise_exception=True):
    #     #     return Response({'message': 'serializer error'}, status=status.HTTP_204_NO_CONTENT)
        
    #     # filter_id = serializer.validated_data["id"]
    #     if DashboardFilters.objects.filter(id= filter_id).exists():
    #         DashboardFilters.objects.get(id= filter_id).delete()
    #         return Response({"message":"Filter Deleted Successful"})
    #     else:
    #         return Response({"message":"dashboard filter id not found"},status=status.HTTP_404_NOT_FOUND)
        

class FinalDashboardFilterData(CreateAPIView):
    serializer_class = serializers.SheetDataSerializer

    def post(self, request, token):
        tok1 = test_token(token)
        if tok1['status'] != 200:
            return Response({"message": tok1['message']}, status=status.HTTP_404_NOT_FOUND)

        serializer = self.serializer_class(data=request.data)
        if not serializer.is_valid():
            return Response({'message': 'serializer error'}, status=status.HTTP_204_NO_CONTENT)

        filter_ids = serializer.validated_data["id"]
        input_lists = serializer.validated_data["input_list"]

        if len(filter_ids) != len(input_lists):
            return Response({'message': 'Filter IDs and input lists count mismatch'}, status=status.HTTP_400_BAD_REQUEST)

        filter_ids = [fid for fid, il in zip(filter_ids, input_lists) if il]
        input_lists = [il for il in input_lists if il]

        if not filter_ids or not input_lists:
            return Response({'message': 'No valid filters provided'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            dashboard_id = DashboardFilters.objects.get(id=filter_ids[0]).dashboard_id
            database_id = dashboard_data.objects.get(id=dashboard_id).server_id
        except DashboardFilters.DoesNotExist:
            return Response({'message': 'Invalid filter ID'}, status=status.HTTP_404_NOT_FOUND)
        except dashboard_data.DoesNotExist:
            return Response({'message': 'Invalid dashboard ID'}, status=status.HTTP_404_NOT_FOUND)

        user_id = tok1['user_id']

        try:
            server_details = ServerDetails.objects.get(user_id=user_id, id=database_id)
            ServerType1 = ServerType.objects.get(id=server_details.server_type)
            dtype = ServerType1.server_type
            server_conn = server_connection(
                server_details.username, server_details.password, server_details.database,
                server_details.hostname, server_details.port, server_details.service_name,
                ServerType1.server_type.upper(), server_details.database_path
            )
            if server_conn['status'] == 200:
                engine = server_conn['engine']
                cursor = server_conn['cursor']
            else:
                return Response(server_conn, status=server_conn['status'])
        except ServerDetails.DoesNotExist:
            return Response({'message': 'Server details not found'}, status=status.HTTP_404_NOT_FOUND)
        except ServerType.DoesNotExist:
            return Response({'message': 'Server type not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'message': str(e)}, status=status.HTTP_400_BAD_REQUEST)

        try:
            filter_details = []
            for filter_id in filter_ids:
                dash_filter = DashboardFilters.objects.get(id=filter_id)
                filter_details.append({
                    "filter_id": filter_id,
                    "dashboard_id": dash_filter.dashboard_id,
                    "sheet_list": eval(dash_filter.sheet_id_list),
                    "column_name": dash_filter.column_name,
                    "datatype": dash_filter.column_datatype
                })
                
            sheet_ids = set()
            for filter_detail in filter_details:
                sheet_ids.update(filter_detail['sheet_list'])
            sheet_ids = list(sheet_ids)

            sheet_details = get_sheet_details(sheet_ids, user_id)
            sheet_mapping = {item["sheetfilter_queryset_id"]: item["sheet_id"] for item in sheet_details}
            sheetfilter_queryset_ids = [item["sheetfilter_queryset_id"] for item in sheet_details]

            details = []
            for sfid in sheetfilter_queryset_ids:
                try:
                    queryset_obj = SheetFilter_querysets.objects.get(Sheetqueryset_id=sfid)
                    sheet_id = sheet_mapping.get(sfid)
                    details.append({
                        "sheet_id": sheet_id,
                        "Sheetqueryset_id": queryset_obj.Sheetqueryset_id,
                        "custom_query": queryset_obj.custom_query,
                        "columns": queryset_obj.columns,
                        "rows": queryset_obj.rows
                    })
                except Exception as e:
                    return Response(f'{e}', status=status.HTTP_404_NOT_FOUND)

            sql_queries = []
            for detail in details:
                custom_query = detail.get("custom_query", "")
                sheetq_id = detail["Sheetqueryset_id"]
                sheet1_id = detail["sheet_id"]
               
                where_clauses = []
                for i, filter_detail in enumerate(filter_details):
                    if sheet1_id in filter_detail["sheet_list"]:
                        column = filter_detail["column_name"]
                        input_list = input_lists[i]
                        if filter_detail["datatype"] == "TIMESTAMPTZ":
                            f = transform_list(input_list)
                            formatted_list = tuple(f)
                            input1 = str(formatted_list).replace(',)', ')')
                            where_clauses.append(f"TO_CHAR(\"{column}\", 'YYYY-MM-DD') IN {input1}")
                        else:
                            try:
                                formatted_list = tuple(int(item) for item in input_list)
                            except ValueError:
                                f = transform_list(input_list)
                                formatted_list = tuple(f)
                            input1 = str(formatted_list).replace(',)', ')')
                            where_clauses.append(f'"{column}" IN {input1}')
                final_query = custom_query.strip()
                
                if 'GROUP BY' in final_query.upper():
                    parts = re.split(r'(\sGROUP\sBY\s)', final_query, flags=re.IGNORECASE)
                    main_query = parts[0]
                    group_by_clause = parts[1] + parts[2]
                else:
                    main_query = final_query
                    group_by_clause = ''

                if 'WHERE' in main_query.upper():
                    main_query += " AND " + " AND ".join(where_clauses)
                else:
                    main_query += " WHERE " + " AND ".join(where_clauses)

                final_query = main_query + " " + group_by_clause

                try:
                    final_query = convert_query(final_query, dtype.lower())
                    colu = cursor.execute(text(final_query))
                    if dtype.lower() == "microsoftsqlserver":
                        colu = cursor.execute(str(final_query))
                        col_list = [column[0] for column in cursor.description]  
                    else:
                        colu = cursor.execute(text(final_query))
                        col_list = [column for column in colu.keys()]
                    col_data = []
                    
                    for row in colu.fetchall():
                        col_data.append(list(row))
                    
                    a11 = []
                    rows11=[]
                    kk=ast.literal_eval(detail['columns'])
                    result = {}
                    for i in kk:
                        a = i.replace(' ','')
                        a = a.replace('"',"")
                        if a in col_list:
                            ind = col_list.index(a)
                            result['column'] = col_list[ind]
                            result['result'] = [item[ind] for item in col_data] 
                        a11.append(result)

                    
                    for i in ast.literal_eval(detail['rows']):
                        result1={}
                        a = i.replace(' ','')
                        a =a.replace('"',"") 
                        if a in col_list:
                            ind = col_list.index(a)
                            result1['column'] = col_list[ind]
                            result1['result'] = [item[ind] for item in col_data]
                        rows11.append(result1)
                    
                    sheet_id11 = sheet_data.objects.get(id = sheet1_id,sheet_filt_id = sheetq_id)
                    sql_queries.append({
                        "sheet_id": sheet1_id,
                        "Sheetqueryset_id": sheetq_id,
                        "final_query": final_query,
                        "columns": a11,
                        "rows": rows11,
                        "chart_id":sheet_id11.chart_id
                    })
                except Exception as e:
                    return Response({'message': "Invalid Input Data for Column"}, status=status.HTTP_406_NOT_ACCEPTABLE)

            return Response(sql_queries, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({"message": str(e)}, status=status.HTTP_404_NOT_FOUND)


class DashboardFilterColumnDataPreview(CreateAPIView):
    serializer_class = serializers.Dashboard_datapreviewSerializer
    def post(self, request, token):
        tok1 = test_token(token)
        if tok1['status'] != 200:
            return Response({"message": tok1['message']}, status=status.HTTP_404_NOT_FOUND)
        serializer = self.serializer_class(data=request.data)
        if not serializer.is_valid(raise_exception=True):
            return Response({'message': 'serializer error'}, status=status.HTTP_204_NO_CONTENT)
        filter_id = serializer.validated_data["id"]
        search_term = serializer.validated_data["search"]
        if not DashboardFilters.objects.filter(id = filter_id).exists():
            return Response({'message':"Invalid Dashboard Filter ID"},status=status.HTTP_404_NOT_FOUND)
        
        if not dashboard_data.objects.filter(id = DashboardFilters.objects.get(id = filter_id).dashboard_id).exists():
            return Response({'message':"Invalid Dashboard ID"},status=status.HTTP_404_NOT_FOUND)

        dashboard_id = DashboardFilters.objects.get(id = filter_id).dashboard_id
        database_id = dashboard_data.objects.get(id= dashboard_id).server_id
        column = DashboardFilters.objects.get(id = filter_id).column_name
        datatype = DashboardFilters.objects.get(id = filter_id).column_datatype
        user_id = tok1['user_id']
        try:
            server_details = ServerDetails.objects.get(user_id=user_id, id=database_id)
            ServerType1 = ServerType.objects.get(id=server_details.server_type)
            dtype = ServerType1.server_type.lower()
            server_conn=server_connection(server_details.username,server_details.password,server_details.database,server_details.hostname,server_details.port,server_details.service_name,ServerType1.server_type.upper(),server_details.database_path)
            if server_conn['status']==200:
                engine=server_conn['engine']
                cursor=server_conn['cursor']
            else:
                return Response(server_conn,status=server_conn['status'])
        except ServerDetails.DoesNotExist:
            return Response({'message': 'Server details not found'}, status=status.HTTP_404_NOT_FOUND)
        except ServerType.DoesNotExist:
            return Response({'message': 'Server type not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'message': str(e)}, status=status.HTTP_400_BAD_REQUEST)

        try:
            qr = ''
            q_id = dashboard_data.objects.get(id=dashboard_id,user_id = user_id)
            query_id = q_id.queryset_id
            joining= QuerySets.objects.get(queryset_id = query_id,user_id = user_id)
            query_set = joining.custom_query
            if DataSource_querysets.objects.filter(queryset_id = query_id,user_id = user_id).exists():
                datasource_id = DataSource_querysets.objects.get(queryset_id = query_id,user_id = user_id)
                datasource_query = datasource_id.custom_query
                qr += datasource_query
            else:
                qr += query_set
            if datatype ==  "TIMESTAMPTZ" or datatype == "DATE" or datatype == "NUMERIC":
                col1 =date_format_syntaxes[dtype](column)
                col_query = "SELECT DISTINCT {} FROM ({})temp".format(col1,qr)
            else:
                col_query = "SELECT DISTINCT {} FROM ({})temp".format(quotes[dtype][0]+column+quotes[dtype][1],qr)
            col_query = convert_query(col_query,dtype)
            if dtype.lower() == "microsoftsqlserver":
                data = cursor.execute(str(col_query))
            else:
                data = cursor.execute(text(col_query))
            col = data.fetchall()
            col_data = [j for i in col for j in i]
            for i in col:
                for j in i:
                    d1 = j
                    col_data.append(d1)
            
            # Apply search filter to col_data
            if search_term:
                col_data = [item for item in col_data if search_term.lower() in str(item).lower()]
            
            col_data = list(set(col_data))

            return Response({"col_data":col_data,"column_name":column}, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({"message": str(e)}, status=status.HTTP_404_NOT_FOUND)
        
class Dashboard_filters_list(CreateAPIView):
    serializer_class = serializers.dashboard_filter_list

    def post(self, request, token):
        role_list = roles.get_previlage_id(previlage=[previlages.view_dashboard_filter])
        tok1 = roles.role_status(token, role_list)
        if tok1['status'] != 200:
            return Response({"message": tok1['message']}, status=status.HTTP_404_NOT_FOUND)

        serializer = self.serializer_class(data=request.data)
        if not serializer.is_valid(raise_exception=True):
            return Response({'message': 'serializer error'}, status=status.HTTP_204_NO_CONTENT)

        dashboard_id = serializer.validated_data["dashboard_id"]
        try:
            dashboard_sheets = eval(dashboard_data.objects.get(id=dashboard_id).sheet_ids)
        except :
            return Response([], status=status.HTTP_200_OK)

        data = []
        if DashboardFilters.objects.filter(dashboard_id=dashboard_id).exists():
            dash_filter = DashboardFilters.objects.filter(dashboard_id=dashboard_id)
            for i in dash_filter:
                data.append({
                    "dashboard_filter_id": i.id,
                    "dashboard_id": i.dashboard_id,
                    "filter_name": i.filter_name,
                    "selected_column": i.column_name,
                    "sheets": i.sheet_id_list,
                    "datatype": i.column_datatype
                })

            for filter_data in data:
                sheet_ids = eval(filter_data["sheets"])  # Convert string representation of list to actual list
                filter_data["sheet_counts"] = {}

                for sheet_id in dashboard_sheets:
                    count = sheet_ids.count(sheet_id)
                    filter_data["sheet_counts"][sheet_id] = count

            return Response(data, status=status.HTTP_200_OK)
        else:
            return Response(data,status=status.HTTP_200_OK)
            
class DashboardFilterDetail(CreateAPIView):
    serializer_class = serializers.dashboard_filter_applied

    def post(self, request, token):
        tok1 = test_token(token)
        if tok1['status'] != 200:
            return Response({"message": tok1['message']}, status=status.HTTP_404_NOT_FOUND)
        
        serializer = self.serializer_class(data=request.data)
        serializer.is_valid(raise_exception=True)
        
        filter_id = serializer.validated_data["filter_id"]
       
        if not DashboardFilters.objects.filter(id=filter_id).exists():
            return Response({"message": "Dashboard filter not found"}, status=status.HTTP_404_NOT_FOUND)
        
        dash_filter = DashboardFilters.objects.get(id=filter_id)
        dash_id = dash_filter.dashboard_id
        dash_sheets = get_dashboard_sheets(dash_id)
        
        dash_filter.sheet_id_list = eval(dash_filter.sheet_id_list)
        sheets_data = [
            {"sheet_id": sheet_id, "selected": sheet_id in dash_sheets}
            for sheet_id in dash_filter.sheet_id_list
        ]
        
        data = {
            "dashboard_filter_id": dash_filter.id,
            "dashboard_id": dash_filter.dashboard_id,
            "filter_name": dash_filter.filter_name,
            "selected_column": dash_filter.column_name,
            "sheets": sheets_data,
            "datatype": dash_filter.column_datatype
        }
        
        return Response(data, status=status.HTTP_200_OK)
    
class DashboardFilterDelete(CreateAPIView):
    def delete(self, request, token,filter_id):
        role_list=roles.get_previlage_id(previlage=[previlages.delete_dashboard_filter])
        tok1 = roles.role_status(token,role_list)
        if tok1['status'] != 200:
            return Response({"message": tok1['message']}, status=status.HTTP_404_NOT_FOUND)

        if DashboardFilters.objects.filter(id=filter_id).exists():
            DashboardFilters.objects.get(id=filter_id).delete()
            return Response({"message": "Filter Deleted Successfully"})
        else:
            return Response({"message": "Dashboard filter ID not found"}, status=status.HTTP_404_NOT_FOUND)
   

    
def fetch_query(query, formatted_list, column):
    qr = ''
    query_upper = query.upper().strip()
    group_by_pos = query_upper.find('GROUP BY')
    if group_by_pos != -1:
        query_part = query[:group_by_pos].strip()
        group_by_clause = query[group_by_pos:].strip()
    else:
        query_part = query.strip()
        group_by_clause = None 

    where_condition = re.search(r'WHERE\s+([^;]+)', query_part, re.IGNORECASE)
    date_format = False

    try:
        datetime.strptime(str(formatted_list[0]), '%Y-%m-%d')
        date_format = True
    except ValueError:
        pass

    format1 = 'yyyy-mm-dd'
    try:
        if where_condition:
            if date_format:
                qr += query_part + f" and TO_CHAR(\"{column}\", '{format1}') in {formatted_list}"
            else:
                qr += query_part + f' and \"{column}\" in {formatted_list}' 
        else:
            if date_format:
                qr += query_part + f" where TO_CHAR(\"{column}\", '{format1}') in {formatted_list}"
            else:
                qr += query_part + f' where \"{column}\" in {formatted_list}'
    except Exception as e:
        print(f"Error: {e}")
    
    if group_by_clause:
        modified_query = qr + ' ' + group_by_clause
    else:
        modified_query = qr

    return modified_query


def transform_list(data):
    transformed_data = []
    for element in data:
        if isinstance(element, list):
            if len(element) == 1 and isinstance(element[0], str) and ',' in element[0]:
                transformed_element = element[0]
            else:
                transformed_element = "'" + "','".join(element) + "'"
        else:
            transformed_element = element
        
        transformed_data.append(transformed_element)

    return transformed_data

def get_sheet_details(sheet_ids, user_id):
    details = []

    for sheet_id in sheet_ids:
        try:
            sheet_data_obj = sheet_data.objects.get(id=sheet_id, user_id=user_id)
            sheetfilter_queryset_id = sheet_data_obj.sheet_filt_id
            chart_id = sheet_data_obj.chart_id
            sheet_data_source = sheet_data_obj.datasrc
            queryset_id = sheet_data_obj.queryset_id

            details.append({
                "sheet_id": sheet_id,
                "chart_id": chart_id,
                "sheetfilter_queryset_id": sheetfilter_queryset_id,
                "sheet_data_source": sheet_data_source,
                "queryset_id": queryset_id
            })
        except sheet_data.DoesNotExist:
            raise Exception(f"sheet_data with id {sheet_id} and user_id {user_id} does not exist.")

    return details

def convert_query(query,dtype):
 
    a = {'postgresql':'postgres','oracle':'oracle','mysql':'mysql','sqlite':'sqlite','microsoftsqlserver':'tsql'}
    if a[dtype]:
        res = a[dtype]

    else:
        res = 'invalid datatype'
    try:
        parsed_query = sqlglot.parse_one(query,read=res)
        converted_query = parsed_query.sql(dialect=res)
    except Exception as e:
        print(str(e),"YYYYYYYYYYYYY")
    

    return converted_query

def get_columns_list(samp, server_type):
    

    postgres_type_code_to_name = {
        16: 'BOOLEAN',
        20: 'BIGINT',
        23: 'INTEGER',
        1042: 'CHAR',
        1043: 'VARCHAR',
        1082: 'DATE',
        1114: 'TIMESTAMP',
        1184: 'TIMESTAMPTZ',
        1700: 'NUMERIC',
        2003: 'DECIMAL',
    }

    mysql_type_code_to_name = {
        0: 'DECIMAL',
        1: 'TINY',
        2: 'SHORT',
        3: 'LONG',
        4: 'FLOAT',
        5: 'DOUBLE',
        6: 'NULL',
        7: 'TIMESTAMP',
        8: 'LONGLONG',
        9: 'INT24',
        10: 'DATE',
        11: 'TIME',
        12: 'DATETIME',
        13: 'YEAR',
        14: 'NEWDATE',
        15: 'VARCHAR',
        16: 'BIT',
        245: 'JSON',
        246: 'NEWDECIMAL',
        247: 'ENUM',
        248: 'SET',
        249: 'TINY_BLOB',
        250: 'MEDIUM_BLOB',
        251: 'LONG_BLOB',
        252: 'BLOB',
        253: 'VAR_STRING',
        254: 'STRING',
        255: 'GEOMETRY'
    }

    sqlite_type_code_to_name = {
        'INTEGER': 'INTEGER',
        'TEXT': 'TEXT',
        'BLOB': 'BLOB',
        'REAL': 'REAL',
        'NUMERIC': 'NUMERIC',
    }

    mssql_type_code_to_name = {
        -7: 'BIT',
        -6: 'TINYINT',
        -5: 'BIGINT',
        4: 'INTEGER',
        5: 'SMALLINT',
        6: 'FLOAT',
        7: 'REAL',
        8: 'DOUBLE',
        -1: 'LONGVARCHAR',
        1: 'CHAR',
        12: 'VARCHAR',
        -2: 'BINARY',
        -3: 'VARBINARY',
        -4: 'LONGVARBINARY',
        91: 'DATE',
        92: 'TIME',
        93: 'TIMESTAMP',
        1111: 'OTHER',
        2000: 'JAVA_OBJECT',
        2001: 'DISTINCT',
        2002: 'STRUCT',
        2003: 'ARRAY',
        2004: 'BLOB',
        2005: 'CLOB',
        2006: 'REF',
        70: 'DATALINK',
        16: 'BOOLEAN',
        -8: 'ROWID',
        -9: 'NCHAR',
        -15: 'NVARCHAR',
        -16: 'LONGNVARCHAR',
        2011: 'NCLOB',
        2009: 'SQLXML'
    }

    if server_type == 'POSTGRESQL':
        return postgres_type_code_to_name
    elif server_type == 'MYSQL':
        return mysql_type_code_to_name
    elif server_type == 'SQLITE':
        return sqlite_type_code_to_name
    elif server_type.upper() == 'MICROSOFTSQLSERVER':
        return mssql_type_code_to_name
    else:
        type_code_to_name = {}

    



class DashboardFilterDelete(CreateAPIView):
    def delete(self, request, token,filter_id):
        role_list=roles.get_previlage_id(previlage=[previlages.delete_dashboard_filter])
        tok1 = roles.role_status(token,role_list)
        if tok1['status'] != 200:
            return Response({"message": tok1['message']}, status=status.HTTP_404_NOT_FOUND)

        if DashboardFilters.objects.filter(id=filter_id).exists():
            DashboardFilters.objects.get(id=filter_id).delete()
            return Response({"message": "Filter Deleted Successfully"})
        else:
            return Response({"message": "Dashboard filter ID not found"}, status=status.HTTP_404_NOT_FOUND)
   

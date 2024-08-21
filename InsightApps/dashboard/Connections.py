
from rest_framework.generics import CreateAPIView
from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status
from django.db import transaction
import psycopg2,cx_Oracle
from dashboard import models,serializers,roles,previlages,views,columns_extract
import pandas as pd
from sqlalchemy import text,inspect
import numpy as np
from .models import ServerDetails,ServerType,QuerySets,ChartFilters,DataSourceFilter
import ast,re,itertools
import datetime
import boto3
import json
import requests
from project import settings
import io
from django.core.paginator import Paginator
from django.db.models import Q
from itertools import chain



##############################################################################################################################

def table_name_from_query(quer_tb):
    join_list=['left','in','right','self','inner','join']
    if quer_tb.is_custom_sql==False:
        pattern = re.compile(r'FROM\s+"?(\w+)"?\."?(\w+)"?|JOIN\s+"?(\w+)"?\."?(\w+)"?', re.IGNORECASE)
        matches = pattern.findall(quer_tb.custom_query)
        al_patern=re.compile(r'(?:FROM|JOIN)\s+(?:"?\w+"?\.)?"?\w+"?\s+(\w+)', re.IGNORECASE) # to fetch table alias
        # al_patern=re.compile(r'(?:FROM|JOIN)\s+(?:"?\w+"?\.)?"?\w+"?\s+(\w+)\s*(?!LEFT|RIGHT|SELF|INNER|ON)\b',) 
        mt=al_patern.findall(quer_tb.custom_query)
        table_names = []
        schema_names = []
        for match in matches:
            schema, table = match[0], match[1]
            if schema and table:
                table_names.append(table)
                schema_names.append(schema)
            schema, table = match[2], match[3]
            if schema and table:
                table_names.append(table) # table_names
                schema_names.append(schema) # schema names
        matching_keywords = [keyword for keyword in mt if keyword in join_list]
        if matching_keywords!=[]:
            mt12 = []
        else:
            mt12 = mt
        data = {
            "tables":table_names,
            "schemas":schema_names,
            "table_alias":mt12
        }
        return data
    else:
        pattern1 = re.compile(r'(FROM|JOIN)\s+"?(\w+)"?\."?(\w+)"?', re.IGNORECASE)   
        matches1 = pattern1.findall(quer_tb.custom_query)
        schemas = []
        tables = []
        for match in matches1:
            if len(match) == 3:
                schemas.append(match[1])
                tables.append(match[2])
        pattern2 = re.compile(r'FROM\s+([^\s;]+)|JOIN\s+([^\s;]+)', re.IGNORECASE)
        matches2 = pattern2.findall(quer_tb.custom_query)
        schemas1=[]
        tables1 = []
        for match in matches2:
            tables1.append(match[0].lower())
        al_patern=re.compile(r'(?:FROM|JOIN)\s+(?:"?\w+"?\.)?"?\w+"?\s+(\w+)', re.IGNORECASE) # to fetch table alias
        mt=al_patern.findall(quer_tb.custom_query)
        matching_keywords = [keyword for keyword in mt if keyword in join_list]
        if matching_keywords!=[]:
            mt123=[]
        else:
            mt123=mt
        if schemas!=[]:
            data = {
                "tables":tables,
                "schemas":schemas,
                "table_alias":mt123
            }
        else:
            data = {
                "tables":tables1,
                "schemas":schemas1,
                "table_alias":mt123
            }
        return data
    

def litera_eval(data):
    if data==None or data=="":
        data1 = data
    else:
        data1=ast.literal_eval(data)
    return data1

##### Create a calculated field
class calculated_field(CreateAPIView):
    serializer_class=serializers.calculated_field

    def post(self,request,token):
        tok1 = views.test_token(token)
        if tok1['status']==200:
            serializer=self.get_serializer(data=request.data)
            if serializer.is_valid(raise_exception=True):  
                db_id=serializer.validated_data['db_id']
                field_name=serializer.validated_data['field_name']
                function=serializer.validated_data['function']
                try:
                    ser_db_data=models.ServerDetails.objects.get(id=db_id,is_connected=True)
                except:
                    return Response({'message':'server_details_id/server_type not exists'},status=status.HTTP_404_NOT_FOUND)
                if models.functions_tb.objects.filter(db_id=db_id,field_name=field_name).exists():
                    return Response({'message':'Field_name already exists'},status=status.HTTP_406_NOT_ACCEPTABLE)
                else:
                    pass
                models.functions_tb.objects.create(db_id=db_id,field_name=field_name,function_ip=function)
                return Response({'message':'Created successfully'},status=status.HTTP_200_OK)
            else:
                return Response({'message':'serializer value error'},status=status.HTTP_400_BAD_REQUEST)
        else:
            return Response(tok1,status=tok1['status'])
        

####### Fetching functions from a table 
@api_view(['GET'])
@transaction.atomic
def function_get(request,db_id,token):
    if request.method=='GET':
        tok1 = views.test_token(token)
        if tok1['status']==200:
            try:
                ser_db_data=models.ServerDetails.objects.get(id=db_id,is_connected=True)
            except:
                return Response({'message':'server_details_id/server_type not exists'},status=status.HTTP_404_NOT_FOUND)
            fun_tb=models.functions_tb.objects.filter(db_id=db_id).values()
            funlist=[]
            funip=[]
            for f1 in fun_tb:
                funlist.append(f1['field_name'])
                funip.append(f1['function_ip'])
            data=[{'field_name':field,'function':funct} for field,funct in zip(funlist,funip)]
            return Response(data,status=status.HTTP_200_OK)
        else:
            return Response(tok1,status=tok1['status'])
    else:
        return Response({'message':'Method not allowed'},status=status.HTTP_405_METHOD_NOT_ALLOWED)



##### Show_me from table,column
class show_me(CreateAPIView):
    serializer_class=serializers.show_me_input

    def post(self,request,token):
        role_list=roles.get_previlage_id(previlage=[previlages.create_sheet,previlages.view_sheet,previlages.edit_sheet])
        tok1 = roles.role_status(token,role_list)
        if tok1['status']==200:
            serializer=self.get_serializer(data=request.data)
            if serializer.is_valid(raise_exception=True):  
                db_id=serializer.validated_data['db_id']
                col=serializer.validated_data['col']
                row=serializer.validated_data['row']
                for i1 in row:
                    col.append(i1)
                column_name=[]
                data_types=[]
                for i1 in col:
                    column_name.append(i1[0])
                    data_types.append(i1[1])
            else:
                return Response({'message':"Serializer Error"},status=status.HTTP_406_NOT_ACCEPTABLE)
            # try:
            #     ser_db_data=models.ServerDetails.objects.get(id=db_id)
            # except:
            #     return Response({'message':'server_details_id/server_type not exists'},status=status.HTTP_404_NOT_FOUND)
            charts=[]
            ids=[]
            ints=["int","float","number","num","numeric","INT","FLOAT","NUMBER","NUM","NUMERIC"]
            dates=["datetime","date","DATETIME","DATE"]
            measures = [col for col, dtype in zip(column_name, data_types) if dtype in ints]
            datetime_dt = [col for col, dtype in zip(column_name, data_types) if dtype in dates]
            dimensions = [col for col, dtype in zip(column_name, data_types) if dtype not in ints]
            if len(measures)!=0 and len(dimensions)!=0:
                char=models.charts.objects.filter(min_measures__lte=len(measures),min_dimensions__lte=len(dimensions),max_dimensions__lte=len(dimensions),max_measures__lte=len(measures)).values()
            elif len(measures)!=0 and len(dimensions)==0:
                char=models.charts.objects.filter(min_measures__lte=len(measures),min_dimensions__gte=len(dimensions),max_dimensions__lte=len(dimensions),max_measures__lte=len(measures)).values()
            elif len(measures)==0 and len(dimensions)!=0:
                char=models.charts.objects.filter(min_measures__gte=len(measures),min_dimensions__lte=len(dimensions),max_dimensions__lte=len(dimensions),max_measures__lte=len(measures)).values()                        
            elif len(measures)==0 and len(dimensions)==0:
                char=models.charts.objects.filter(min_measures__gte=len(measures),min_dimensions__gte=len(dimensions),max_dimensions__lte=len(dimensions),max_measures__lte=len(measures)).values()
            elif len(measures)!=0 and len(datetime_dt)!=0: 
                char=models.charts.objects.filter(min_measures__lte=len(measures),min_dates__lte=len(datetime_dt),max_dates__lte=len(datetime_dt),max_measures__lte=len(measures)).values()  
            elif len(dimensions)!=0 and len(datetime_dt)!=0: 
                char=models.charts.objects.filter(min_dimensions__lte=len(dimensions),min_dates__lte=len(datetime_dt),max_dimensions__gte=len(dimensions),max_dates__gte=len(datetime_dt)).values()  
            elif len(measures)==0 and len(datetime_dt)!=0: 
                char=models.charts.objects.filter(min_measures__gte=len(measures),min_dates__lte=len(datetime_dt),max_measures__lte=len(measures),max_dates__lte=len(datetime_dt)).values()  
            elif len(dimensions)==0 and len(datetime_dt)!=0: 
                char=models.charts.objects.filter(min_dimensions__gte=len(dimensions),min_dates__lte=len(datetime_dt),max_dimensions__lte=len(dimensions),max_dates__lte=len(datetime_dt)).values() 
            else:
                pass 
            defl=[24,17,13,1,6]
            def2=["pie","AREA_CHARTS","line","bar","Table"]
            for i12,i2 in zip(defl,def2):
                ids.append(i12)
                charts.append(i2)
            for ch in char:
                ids.append(ch['id'])
                charts.append(ch['chart_type'])
            id_se=(set(ids))
            ch_se=(set(charts))
            data = [{"id":ids1,"charts":chart1} for ids1,chart1 in zip(list(id_se),list(ch_se))]
            return Response(data,status=status.HTTP_200_OK)
        else:
            return Response(tok1,status=tok1['status'])


##########    Sheet data saving,update,delete & Sheet Name update  ##################

def file_save_1(data,server_id,queryset_id,ip,dl_key):
    t1=datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    file_path = f'{t1}{server_id}{queryset_id}.txt'
    # with open(file_path, 'w') as file:
    #     json.dump(data, file, indent=4)
    json_data = json.dumps(data, indent=4)
    file_buffer = io.BytesIO(json_data.encode('utf-8'))
    s3 = boto3.client('s3', aws_access_key_id=settings.AWS_S3_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_S3_SECRET_ACCESS_KEY)
    file_key = f'insightapps/{ip}/{file_path}'
    if dl_key=="":
        s3.upload_fileobj(file_buffer, Bucket=settings.AWS_STORAGE_BUCKET_NAME, Key=file_key)
        file_url = f"https://{settings.AWS_STORAGE_BUCKET_NAME}.s3.amazonaws.com/{file_key}"
    else:
        s3.delete_object(Bucket=settings.AWS_STORAGE_BUCKET_NAME, Key=str(dl_key))
        s3.upload_fileobj(file_buffer, Bucket=settings.AWS_STORAGE_BUCKET_NAME, Key=file_key)
        file_url = f"https://{settings.AWS_STORAGE_BUCKET_NAME}.s3.amazonaws.com/{file_key}"
    data_fn={
        "file_key":file_key,
        "file_url":file_url
    }
    return data_fn


def image_save_1(image,ip,dl_key):
    if image==None or image=='':
        data_fn = {
            "file_key":None,
            "file_url":None
        }
        return data_fn
    else:
        t1=datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        file_path = f'{t1}{image}'
        s3 = boto3.client('s3', aws_access_key_id=settings.AWS_S3_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_S3_SECRET_ACCESS_KEY)
        file_key = f'insightapps/{ip}/{file_path}'
        if dl_key=="" or dl_key==None:
            s3.upload_fileobj(image, Bucket=settings.AWS_STORAGE_BUCKET_NAME, Key=file_key)
            file_url = f"https://{settings.AWS_STORAGE_BUCKET_NAME}.s3.amazonaws.com/{file_key}"
        else:
            s3.delete_object(Bucket=settings.AWS_STORAGE_BUCKET_NAME, Key=str(dl_key))
            s3.upload_fileobj(image, Bucket=settings.AWS_STORAGE_BUCKET_NAME, Key=file_key)
            file_url = f"https://{settings.AWS_STORAGE_BUCKET_NAME}.s3.amazonaws.com/{file_key}"
        data_fn={
            "file_key":file_key,
            "file_url":file_url
        }
        return data_fn


def sheet_s_u(serializer,u_id,sh_id,parameter):
    data=serializer.validated_data['data']
    chart_id=serializer.validated_data['chart_id']
    queryset_id=serializer.validated_data['queryset_id']
    server_id=serializer.validated_data['server_id']
    sheet_name=serializer.validated_data['sheet_name']
    filterId=serializer.validated_data['filterId']
    sheet_tag_name=serializer.validated_data['sheet_tag_name']
    file_id=serializer.validated_data['file_id']
    sheetfilter_querysets_id=serializer.validated_data['sheetfilter_querysets_id']
    if file_id==None or file_id=='':
        if models.ServerDetails.objects.filter(id=server_id,user_id=u_id).exists():
            pass
        else:
            return Response({'message':'server id not exists'},status=status.HTTP_404_NOT_FOUND)
    else:
        if models.FileDetails.objects.filter(id=file_id).exists():
            pass
        else:
            return Response({'message':'File id not exists'},status=status.HTTP_404_NOT_FOUND)
    if models.charts.objects.filter(id=chart_id).exists():
        pass
    else:
        return Response({'message':'Chart id not exists'},status=status.HTTP_404_NOT_FOUND)
    if parameter=="save":
        if file_id==None or file_id=='':
            if models.sheet_data.objects.filter(user_id=u_id,queryset_id=queryset_id,server_id=server_id,sheet_name=sheet_name).exists():
                return Response({'message':'Sheet Name already exists, please rename the sheet name'},status=status.HTTP_406_NOT_ACCEPTABLE)
            else:
                dl_key=""
                file_sv=file_save_1(data,server_id,queryset_id,ip='sheetdata',dl_key=dl_key)
                sh123=models.sheet_data.objects.create(user_id=u_id,chart_id=chart_id,queryset_id=queryset_id,server_id=server_id,filter_ids=filterId,sheet_filt_id=sheetfilter_querysets_id,
                                                    datapath=file_sv["file_key"],datasrc=file_sv["file_url"],created_at=datetime.datetime.now(),updated_at=datetime.datetime.now(),
                                                    sheet_name=sheet_name,sheet_tag_name=sheet_tag_name)
                return Response({"sheet_id":sh123.id,"sheet_name":sh123.sheet_name,'message':'Saved Successfully'},status=status.HTTP_200_OK)
        else:
            if models.sheet_data.objects.filter(user_id=u_id,queryset_id=queryset_id,file_id=file_id,sheet_name=sheet_name).exists():
                return Response({'message':'Sheet Name already exists, please rename the sheet name'},status=status.HTTP_406_NOT_ACCEPTABLE)
            else:
                dl_key=""
                file_sv=file_save_1(data,file_id,queryset_id,ip='sheetdata',dl_key=dl_key)
                sh123=models.sheet_data.objects.create(user_id=u_id,chart_id=chart_id,queryset_id=queryset_id,file_id=file_id,filter_ids=filterId,sheet_filt_id=sheetfilter_querysets_id,
                                                    datapath=file_sv["file_key"],datasrc=file_sv["file_url"],created_at=datetime.datetime.now(),updated_at=datetime.datetime.now(),
                                                    sheet_name=sheet_name,sheet_tag_name=sheet_tag_name)
            return Response({"sheet_id":sh123.id,"sheet_name":sh123.sheet_name,'message':'Saved Successfully'},status=status.HTTP_200_OK)
    elif parameter=="update":
        if file_id==None or file_id=='':
            if models.sheet_data.objects.filter(user_id=u_id,queryset_id=queryset_id,server_id=server_id,id=sh_id).exists():
                old= models.sheet_data.objects.get(user_id=u_id,queryset_id=queryset_id,server_id=server_id,id=sh_id)
                if old.sheet_name==sheet_name:
                    file_sv=file_save_1(data,server_id,queryset_id,ip='sheetdata',dl_key=old.datapath)
                    models.sheet_data.objects.filter(user_id=u_id,queryset_id=queryset_id,server_id=server_id,id=sh_id).update(
                        chart_id=chart_id,datapath=file_sv["file_key"],datasrc=file_sv["file_url"],updated_at=datetime.datetime.now(),filter_ids=filterId,
                        sheet_filt_id=sheetfilter_querysets_id,sheet_tag_name=sheet_tag_name)
                    return Response({'message':'Updated Successfully'},status=status.HTTP_200_OK)
                else:
                    if models.sheet_data.objects.filter(user_id=u_id,queryset_id=queryset_id,server_id=server_id,sheet_name=sheet_name).exists():
                        return Response({'message':'Sheet name already exists for this queryset, data and sheet name will not update'},status=status.HTTP_406_NOT_ACCEPTABLE)
                    else:
                        file_sv=file_save_1(data,server_id,queryset_id,ip='sheetdata',dl_key=old.datapath)
                        models.sheet_data.objects.filter(user_id=u_id,queryset_id=queryset_id,server_id=server_id,id=sh_id).update(sheet_name=sheet_name,filter_ids=filterId,
                            chart_id=chart_id,datapath=file_sv["file_key"],datasrc=file_sv["file_url"],updated_at=datetime.datetime.now(),sheet_filt_id=sheetfilter_querysets_id,
                            sheet_tag_name=sheet_tag_name)
                        return Response({'message':'Updated Successfully'},status=status.HTTP_200_OK)
            else:
                return Response({'message':'Sheet not exists'},status=status.HTTP_404_NOT_FOUND)
        else:
            if models.sheet_data.objects.filter(user_id=u_id,queryset_id=queryset_id,file_id=file_id,id=sh_id).exists():
                old= models.sheet_data.objects.get(user_id=u_id,queryset_id=queryset_id,file_id=file_id,id=sh_id)
                if old.sheet_name==sheet_name:
                    file_sv=file_save_1(data,file_id,queryset_id,ip='sheetdata',dl_key=old.datapath)
                    models.sheet_data.objects.filter(user_id=u_id,queryset_id=queryset_id,file_id=file_id,id=sh_id).update(
                        chart_id=chart_id,datapath=file_sv["file_key"],datasrc=file_sv["file_url"],updated_at=datetime.datetime.now(),filter_ids=filterId,
                        sheet_filt_id=sheetfilter_querysets_id,sheet_tag_name=sheet_tag_name)
                    return Response({'message':'Updated Successfully'},status=status.HTTP_200_OK)
                else:
                    if models.sheet_data.objects.filter(user_id=u_id,queryset_id=queryset_id,file_id=file_id,sheet_name=sheet_name).exists():
                        return Response({'message':'Sheet name already exists for this queryset, data and sheet name will not update'},status=status.HTTP_406_NOT_ACCEPTABLE)
                    else:
                        file_sv=file_save_1(data,file_id,queryset_id,ip='sheetdata',dl_key=old.datapath)
                        models.sheet_data.objects.filter(user_id=u_id,queryset_id=queryset_id,file_id=file_id,id=sh_id).update(sheet_name=sheet_name,filter_ids=filterId,
                            chart_id=chart_id,datapath=file_sv["file_key"],datasrc=file_sv["file_url"],updated_at=datetime.datetime.now(),sheet_filt_id=sheetfilter_querysets_id,
                            sheet_tag_name=sheet_tag_name)
                        return Response({'message':'Updated Successfully'},status=status.HTTP_200_OK)
            else:
                return Response({'message':'Sheet not exists'},status=status.HTTP_404_NOT_FOUND)
    else:
        pass

class sheet_saving(CreateAPIView):
    serializer_class=serializers.sheet_save_serializer

    @transaction.atomic
    def post(self,request,token):
        role_list=roles.get_previlage_id(previlage=[previlages.create_sheet,previlages.create_sheet_title])
        tok1 = roles.role_status(token,role_list)
        if tok1['status']==200:
            serializer=self.get_serializer(data=request.data)
            if serializer.is_valid(raise_exception=True):  
                shid=""
                sh12 = sheet_s_u(serializer,tok1['user_id'],shid,parameter="save")
                return sh12
            else:
                return Response({'message':'Serializer not valid'},status=status.HTTP_400_BAD_REQUEST)
        else:
            return Response(tok1,status=tok1['status'])
        

class sheet_update(CreateAPIView):
    serializer_class=serializers.sheet_save_serializer

    def post(self,request,token,shid):
        role_list=roles.get_previlage_id(previlage=[previlages.edit_sheet,previlages.edit_sheet_title])
        tok1 = roles.role_status(token,role_list)
        if tok1['status']==200:
            serializer=self.get_serializer(data=request.data)
            if serializer.is_valid(raise_exception=True):  
                sh12 = sheet_s_u(serializer,tok1['user_id'],shid,parameter="update")
                return sh12
            else:
                return Response({'message':'Serializer not valid'},status=status.HTTP_400_BAD_REQUEST)
        else:
            return Response(tok1,status=tok1['status'])
     

class sheet_retrieve(CreateAPIView):
    serializer_class=serializers.sheet_retrieve_serializer

    @transaction.atomic
    def post(self,request,shid,token):
        role_list=roles.get_previlage_id(previlage=[previlages.view_sheet])
        tok1 = roles.role_status(token,role_list)
        if tok1['status']==200:
            serializer=self.get_serializer(data=request.data)
            ch_list=[]
            if serializer.is_valid(raise_exception=True):  
                queryset_id=serializer.validated_data['queryset_id']
                server_id=serializer.validated_data['server_id']
                # sheet_name=serializer.validated_data['sheet_name'] 
                file_id=serializer.validated_data['file_id']  
                if file_id==None or file_id=='':
                    if models.ServerDetails.objects.filter(id=server_id,user_id=tok1['user_id']).exists():
                        pass
                    else:
                        return Response({'message':'server id not exists'},status=status.HTTP_404_NOT_FOUND)
                else:
                    if models.FileDetails.objects.filter(id=file_id).exists():
                        pass
                    else:
                        return Response({'message':'File id not exists'},status=status.HTTP_404_NOT_FOUND)
                try:
                    if file_id==None or file_id=='':
                        sheetdata=models.sheet_data.objects.get(user_id=tok1['user_id'],queryset_id=queryset_id,server_id=server_id,id=shid)
                        surl=sheetdata.datasrc
                    else:
                        sheetdata=models.sheet_data.objects.get(user_id=tok1['user_id'],queryset_id=queryset_id,file_id=file_id,id=shid)
                        surl=sheetdata.datasrc
                except:
                    return Response({'message':'sheet not exists/not valid'},status=status.HTTP_406_NOT_ACCEPTABLE)
                for ch in ast.literal_eval(sheetdata.filter_ids):
                    ch_filter=models.ChartFilters.objects.filter(filter_id=ch).values()
                    ch_list.append(ch_filter)
                flat_filters_data = [item for sublist in ch_list for item in sublist]
                if surl==None:
                    sheet_data=None
                else:
                    data=requests.get(sheetdata.datasrc)
                    sheet_data=data.json() 
                d1 = {
                    "sheet_id":sheetdata.id,
                    "sheet_name":sheetdata.sheet_name,
                    "chart_id":sheetdata.chart_id,
                    "sheet_tag_name":sheetdata.sheet_tag_name,
                    "sheet_data":sheet_data,
                    "sheet_filter_ids":sheetdata.filter_ids,
                    "sheet_filter_quereyset_ids":sheetdata.sheet_filt_id,
                    "filters_data":flat_filters_data
                }
                return Response(d1,status=status.HTTP_200_OK)
            else:
                return Response({'message':'Serializer not valid'},status=status.HTTP_400_BAD_REQUEST)
        else:
            return Response(tok1,status=tok1['status'])
    
###### Remove data from dashboard data file based on sheet id
def remove_sheet_id(data, sheet_id_to_remove):
    # new_data = []
    # for sublist in data:
    #     filtered_sublist = [item for item in sublist if item.get('sheetId') != sheet_id_to_remove]
    #     if filtered_sublist:
    #         new_data.append(filtered_sublist)
    # return new_data

    new_data = []
    for sublist in data:
        # Check if sublist contains dictionaries and filter
        filtered_sublist = [item for item in sublist if isinstance(item, dict) and item.get('sheetId') != sheet_id_to_remove]
        if filtered_sublist:  # Add to new_data only if not empty
            new_data.append(filtered_sublist)
    return new_data


@api_view(['DELETE'])
@transaction.atomic
def sheet_delete(request,server_id,queryset_id,sheet_id,token):
    if request.method=='DELETE':
        role_list=roles.get_previlage_id(previlage=[previlages.delete_sheet])
        tok1 = roles.role_status(token,role_list)
        if tok1['status']==200:
            if models.sheet_data.objects.filter(user_id=tok1['user_id'],queryset_id=queryset_id,server_id=server_id,id=sheet_id).exists():
                pass
            elif models.sheet_data.objects.filter(user_id=tok1['user_id'],queryset_id=queryset_id,file_id=server_id,id=sheet_id).exists():
                pass
            else:
                return Response({'message':'sheet not exists/not valid'},status=status.HTTP_406_NOT_ACCEPTABLE)
            
            shdt=models.sheet_data.objects.get(id=sheet_id)
            dsdt=models.dashboard_data.objects.filter(sheet_ids__contains=str(sheet_id)).values('sheet_ids','datasrc','id','datapath')
            l1=[]
            for i2 in dsdt:
                o_shid=i2['sheet_ids']  # old sheet ids
                a1=ast.literal_eval(o_shid)
                indx=a1.index(int(sheet_id))
                a1.pop(indx) # new sheet ids after removed
                dsh_dt=requests.get(i2['datasrc'])
                l1.append(dsh_dt.json())
                resul=remove_sheet_id(l1, sheet_id)
                # print(json.dumps(resul, indent=4))
                dl_key=i2['datapath']
                filesave=file_save_1(resul,server_id,queryset_id,ip='dashboard',dl_key=dl_key)
                models.dashboard_data.objects.filter(sheet_ids=o_shid).update(sheet_ids=a1,datasrc=filesave['file_url'],datapath=filesave['file_key'])
            s3 = boto3.client('s3', aws_access_key_id=settings.AWS_S3_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_S3_SECRET_ACCESS_KEY)
            s3.delete_object(Bucket=settings.AWS_STORAGE_BUCKET_NAME, Key=str(shdt.datapath))
            if shdt.sheet_filt_id=='' or shdt.sheet_filt_id==None:
                models.sheet_data.objects.filter(user_id=tok1['user_id'],queryset_id=queryset_id,server_id=server_id,id=sheet_id).delete()
                models.sheet_data.objects.filter(user_id=tok1['user_id'],queryset_id=queryset_id,file_id=server_id,id=sheet_id).delete()
            else:
                models.SheetFilter_querysets.objects.filter(user_id=tok1['user_id'],Sheetqueryset_id=shdt.sheet_filt_id).delete()
                models.sheet_data.objects.filter(user_id=tok1['user_id'],queryset_id=queryset_id,server_id=server_id,id=sheet_id).delete()
                models.sheet_data.objects.filter(user_id=tok1['user_id'],queryset_id=queryset_id,file_id=server_id,id=sheet_id).delete()
            return Response({'message':'Removed Successfully'},status=status.HTTP_200_OK)
        else:
            return Response(tok1,status=tok1['status'])
    else:
        return Response({'message':'Method not allowed'},status=status.HTTP_405_METHOD_NOT_ALLOWED)
        

class sheet_name_update(CreateAPIView):
    serializer_class=serializers.sheet_name_update_serializer

    def post(self,request,token):
        role_list=roles.get_previlage_id(previlage=[previlages.edit_sheet_title])
        tok1 = roles.role_status(token,role_list)
        if tok1['status']==200:
            serializer=self.get_serializer(data=request.data)
            if serializer.is_valid(raise_exception=True):  
                queryset_id=serializer.validated_data['queryset_id']
                server_id=serializer.validated_data['server_id']
                old_sheet_name=serializer.validated_data['old_sheet_name']
                new_sheet_name=serializer.validated_data['new_sheet_name']
                if models.ServerDetails.objects.filter(id=server_id,user_id=tok1['user_id']).exists():
                    pass
                else:
                    return Response({'message':'server id not exists'},status=status.HTTP_404_NOT_FOUND)
                if models.sheet_data.objects.filter(user_id=tok1['user_id'],queryset_id=queryset_id,server_id=server_id,sheet_name=old_sheet_name).exists():
                    models.sheet_data.objects.filter(user_id=tok1['user_id'],queryset_id=queryset_id,server_id=server_id,sheet_name=old_sheet_name).update(sheet_name=new_sheet_name,updated_at=datetime.datetime.now())
                    return Response({'message':'Sheet name updated successfully'},status=status.HTTP_200_OK)
                else:
                    return Response({'message':'Sheet not exists'},status=status.HTTP_404_NOT_FOUND)
            else:
                return Response({'message':'Serializer not valid'},status=status.HTTP_400_BAD_REQUEST)
        else:
            return Response(tok1,status=tok1['status'])


##########    Dashboard data saving,update,delete & Sheet Name update  ##################

def dashboard_s_u(serializer,u_id,ds_id,parameter):
    queryset_id=serializer.validated_data['queryset_id']
    server_id=serializer.validated_data['server_id']
    data=serializer.validated_data['data']
    sheet_ids=serializer.validated_data['sheet_ids']
    dashboard_tag_name=serializer.validated_data['dashboard_tag_name']
    dashboard_name=serializer.validated_data['dashboard_name']
    file_id=serializer.validated_data['file_id']
    role_ids=serializer.validated_data['role_ids']
    user_ids=serializer.validated_data['user_ids']
    height=serializer.validated_data['height']
    width=serializer.validated_data['width']
    grid=serializer.validated_data['grid']
    selected_sheet_ids=serializer.validated_data['selected_sheet_ids']
    if models.grid_type.objects.filter(grid_type=str(grid).upper()).exists():
        gr_tb=models.grid_type.objects.get(grid_type=str(grid).upper())
    else:
        return Response({'message':'grid type not exists'},status=status.HTTP_404_NOT_FOUND)
    # try:
    #     dashboarddata=models.dashboard_data.objects.get(dashboard_name=dashboard_name)
    #     if dashboarddata.user_ids==[] or dashboarddata.user_ids==None or dashboarddata.user_ids=='':
    #         a=[]
    #     else:
    #         a=[item for sublist in ast.literal_eval(dashboarddata.user_ids) for item in sublist]
    # except:
    #     a=[]
    server="server"
    queryset="query"
    if parameter=="save":
        if models.dashboard_data.objects.filter(user_id=u_id,dashboard_name=dashboard_name).exists() or models.dashboard_data.objects.filter(dashboard_name=dashboard_name,user_ids__contains=u_id):# and u_id in a:
            return Response({'message':'Dashboard Name already exists, please rename the Dashboard name'},status=status.HTTP_406_NOT_ACCEPTABLE)
        else:
            dl_key=""
            file_sv=file_save_1(data,server,queryset,ip='dashboard',dl_key=dl_key)
            ds_dt=models.dashboard_data.objects.create(user_id=u_id,sheet_ids=sheet_ids,queryset_id=queryset_id,server_id=server_id,role_ids=role_ids,user_ids=user_ids,file_id=file_id,
                                                datapath=file_sv["file_key"],datasrc=file_sv["file_url"],created_at=datetime.datetime.now(),updated_at=datetime.datetime.now(),
                                                dashboard_name=dashboard_name,dashboard_tag_name=dashboard_tag_name,selected_sheet_ids=selected_sheet_ids,
                                                height=height,width=width,grid_id=gr_tb.id)
            return Response({'dashboard_id':ds_dt.id,'message':'Saved Successfully'},status=status.HTTP_200_OK)
    elif parameter=="update":
        if models.dashboard_data.objects.filter(user_id=u_id,id=ds_id).exists() or models.dashboard_data.objects.filter(id=ds_id,user_ids__contains=u_id):# and u_id in a:
            old= models.dashboard_data.objects.get(id=ds_id)
            if old.dashboard_name==dashboard_name:
                file_sv=file_save_1(data,server,queryset,ip='dashboard',dl_key=old.datapath)
                models.dashboard_data.objects.filter(id=ds_id).update(role_ids=role_ids,user_ids=user_ids,file_id=file_id,selected_sheet_ids=selected_sheet_ids,
                sheet_ids=sheet_ids,datapath=file_sv["file_key"],datasrc=file_sv["file_url"],updated_at=datetime.datetime.now(),dashboard_tag_name=dashboard_tag_name,
                height=height,width=width,grid_id=gr_tb.id)
                return Response({'message':'Updated Successfully'},status=status.HTTP_200_OK)
            else:
                if models.dashboard_data.objects.filter(user_id=u_id,dashboard_name=dashboard_name).exists() or models.dashboard_data.objects.filter(dashboard_name=dashboard_name,user_ids__contains=u_id):# and u_id in a:
                    return Response({'message':'Dashboard Name already exists, please rename the Dashboard name'},status=status.HTTP_406_NOT_ACCEPTABLE)
                else:
                    file_sv=file_save_1(data,server,queryset,ip='dashboard',dl_key=old.datapath)
                    models.dashboard_data.objects.filter(id=ds_id).update(dashboard_name=dashboard_name,file_id=file_id,selected_sheet_ids=selected_sheet_ids,
                    sheet_ids=sheet_ids,datapath=file_sv["file_key"],datasrc=file_sv["file_url"],updated_at=datetime.datetime.now(),dashboard_tag_name=dashboard_tag_name,
                    role_ids=role_ids,user_ids=user_ids,height=height,width=width,grid_id=gr_tb.id)
                    return Response({'message':'Updated Successfully'},status=status.HTTP_200_OK)
        else:
            return Response({'message':'dashboard not exists'},status=status.HTTP_404_NOT_FOUND)
    else:
        pass


class dashboard_image(CreateAPIView):
    serializer_class=serializers.dashboard_image

    @transaction.atomic
    def post(self,request,token):
        tok1 = views.test_token(token)
        if tok1['status']==200:
            serializer=self.get_serializer(data=request.data)
            if serializer.is_valid(raise_exception=True):
                dashboard_id=serializer.validated_data['dashboard_id']
                imagepath=serializer.validated_data['imagepath']
                if models.dashboard_data.objects.filter(id=dashboard_id,user_id=tok1['user_id']).exists():
                    ds_board=models.dashboard_data.objects.get(id=dashboard_id,user_id=tok1['user_id'])
                    image_sv=image_save_1(imagepath,ip='dashboard/images/',dl_key=ds_board.imagepath)
                    models.dashboard_data.objects.filter(id=dashboard_id,user_id=tok1['user_id']).update(updated_at=datetime.datetime.now(),
                                                                                        imagepath=image_sv["file_key"],imagesrc=image_sv["file_url"])
                    return Response({'message':'Updated Successfully'},status=status.HTTP_200_OK)
                else:
                    return Response({'message':'dashboard not exists'},status=status.HTTP_404_NOT_FOUND)
            else:
                return Response({'message':'Serializer not valid'},status=status.HTTP_400_BAD_REQUEST)
        else:
            return Response(tok1,status=tok1['status'])



class dahshboard_save(CreateAPIView):
    serializer_class=serializers.dashboard

    @transaction.atomic
    def post(self,request,token):
        role_list=roles.get_previlage_id(previlage=[previlages.create_dashboard,previlages.create_dashboard_title])
        tok1 = roles.role_status(token,role_list)
        if tok1['status']==200:
            serializer=self.get_serializer(data=request.data)
            if serializer.is_valid(raise_exception=True):  
                ds_id=""
                sh12 = dashboard_s_u(serializer,tok1['user_id'],ds_id,parameter="save")
                return sh12
            else:
                return Response({'message':'Serializer not valid'},status=status.HTTP_400_BAD_REQUEST)
        else:
            return Response(tok1,status=tok1['status'])
        

class dashboard_update(CreateAPIView):
    serializer_class=serializers.dashboard

    @transaction.atomic
    def post(self,request,ds_id,token):
        role_list=roles.get_previlage_id(previlage=[previlages.edit_dasboard,previlages.edit_dashboard_title])
        tok1 = roles.role_status(token,role_list)
        if tok1['status']==200:
            if models.dashboard_data.objects.filter(id=ds_id).exists():
                dashboarddata=models.dashboard_data.objects.get(id=ds_id)
            else:
                return Response({'message':'dashboard not exists'},status=status.HTTP_404_NOT_FOUND)
            # a=[item for sublist in ast.literal_eval(dashboarddata.user_ids) for item in sublist]
            if models.dashboard_data.objects.filter(id=ds_id,user_id=str(tok1['user_id'])).exists() or models.dashboard_data.objects.filter(id=ds_id,user_ids__contains=tok1['user_id']):# and tok1['user_id'] in a:
                pass
            else:
                return Response({'message':'User Not assigned to this ROLE/Not Assigned'},status=status.HTTP_401_UNAUTHORIZED)
            serializer=self.get_serializer(data=request.data)
            if serializer.is_valid(raise_exception=True):  
                sh12 = dashboard_s_u(serializer,tok1['user_id'],ds_id,parameter="update")
                return sh12
            else:
                return Response({'message':'Serializer not valid'},status=status.HTTP_400_BAD_REQUEST)
        else:
            return Response(tok1,status=tok1['status'])
        


class dashboard_retrieve(CreateAPIView):
    serializer_class=serializers.dashboard_retrieve_serializer

    @transaction.atomic
    def post(self,request,token):
        role_list=roles.get_previlage_id(previlage=[previlages.view_dashboard])
        tok1 = roles.role_status(token,role_list)
        if tok1['status']==200:
            serializer=self.get_serializer(data=request.data)
            if serializer.is_valid(raise_exception=True):  
                dashboard_id=serializer.validated_data['dashboard_id']
                if models.dashboard_data.objects.filter(id=dashboard_id).exists():
                    dashboarddata=models.dashboard_data.objects.get(id=dashboard_id)
                    durl=dashboarddata.datasrc
                    gr_tb=models.grid_type.objects.get(id=dashboarddata.grid_id)
                else:
                    return Response({'message':'dashboard not exists'},status=status.HTTP_404_NOT_FOUND)
                # a=[item for sublist in ast.literal_eval(dashboarddata.user_ids) for item in sublist]
                if models.dashboard_data.objects.filter(id=dashboard_id,user_id=str(tok1['user_id'])).exists() or models.dashboard_data.objects.filter(id=dashboard_id,user_ids__contains=tok1['user_id']):# and tok1['user_id'] in a:
                    pass
                else:
                    return Response({'message':'User Not assigned to this ROLE/Not Assigned'},status=status.HTTP_401_UNAUTHORIZED)
                sheet_name=[]
                for shid in ast.literal_eval(dashboarddata.sheet_ids):
                    shdt=models.sheet_data.objects.get(id=shid)
                    sheet_name.append(shdt.sheet_name)
                if durl==None:
                    dashboard_data=None
                else:
                    data=requests.get(dashboarddata.datasrc)
                    dashboard_data=data.json() 
                d1 = {
                    "dashboard_id":dashboarddata.id,
                    "dashboard_name":dashboarddata.dashboard_name,
                    "dashboard_tag_name":dashboarddata.dashboard_tag_name,
                    "sheet_ids":litera_eval(dashboarddata.sheet_ids),
                    "selected_sheet_ids":litera_eval(dashboarddata.selected_sheet_ids),
                    "sheet_names":sheet_name,
                    "grid_type":gr_tb.grid_type,
                    "height":dashboarddata.height,
                    "width":dashboarddata.width,
                    "server_id":litera_eval(dashboarddata.server_id),
                    "queryset_id":litera_eval(dashboarddata.queryset_id),
                    "file_id":litera_eval(dashboarddata.file_id),
                    "dashboard_image":dashboarddata.imagesrc,
                    "dashboard_data":dashboard_data,
                    "role_ids":litera_eval(dashboarddata.role_ids),
                    "user_ids":litera_eval(dashboarddata.user_ids)
                }
                return Response(d1,status=status.HTTP_200_OK)
            else:
                return Response({'message':'Serializer not valid'},status=status.HTTP_400_BAD_REQUEST)
        else:
            return Response(tok1,status=tok1['status'])
    

@api_view(['DELETE'])
@transaction.atomic
def dashboard_delete(request,dashboard_id,token):
    if request.method=='DELETE':
        role_list=roles.get_previlage_id(previlage=[previlages.delete_dashboard])
        tok1 = roles.role_status(token,role_list)
        if tok1['status']==200:
            if models.dashboard_data.objects.filter(id=dashboard_id).exists():
                dashboarddata=models.dashboard_data.objects.get(id=dashboard_id)
            else:
                return Response({'message':'dashboard not exists'},status=status.HTTP_404_NOT_FOUND)
            # a=[item for sublist in ast.literal_eval(dashboarddata.user_ids) for item in sublist]
            if models.dashboard_data.objects.filter(id=dashboard_id,user_id=str(tok1['user_id'])).exists() or models.dashboard_data.objects.filter(id=dashboard_id,user_ids__contains=tok1['user_id']):# and tok1['user_id'] in a:
                pass
            else:
                return Response({'message':'User Not assigned to this ROLE/Not Assigned'},status=status.HTTP_401_UNAUTHORIZED)
            
            ds_data=models.dashboard_data.objects.get(id=dashboard_id)
            s3 = boto3.client('s3', aws_access_key_id=settings.AWS_S3_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_S3_SECRET_ACCESS_KEY)
            s3.delete_object(Bucket=settings.AWS_STORAGE_BUCKET_NAME, Key=str(ds_data.datapath))
            models.dashboard_data.objects.filter(id=dashboard_id).delete()
            return Response({'message':'Removed Successfully'},status=status.HTTP_200_OK)
        else:
            return Response(tok1,status=tok1['status'])
    else:
        return Response({'method':'Method not allowed'},status=status.HTTP_405_METHOD_NOT_ALLOWED)
        

class dashboard_name_update(CreateAPIView):
    serializer_class=serializers.dashboard_name_update_serializer

    def post(self,request,token):
        role_list=roles.get_previlage_id(previlage=[previlages.edit_dashboard_title])
        tok1 = roles.role_status(token,role_list)
        if tok1['status']==200:
            serializer=self.get_serializer(data=request.data)
            if serializer.is_valid(raise_exception=True):  
                queryset_id=serializer.validated_data['queryset_id']
                server_id=serializer.validated_data['server_id']
                old_dashboard_name=serializer.validated_data['old_dashboard_name']
                new_dashboard_name=serializer.validated_data['new_dashboard_name']
                if models.ServerDetails.objects.filter(id=server_id,user_id=tok1['user_id']).exists():
                    pass
                else:
                    return Response({'message':'server id not exists'},status=status.HTTP_404_NOT_FOUND)
                if models.dashboard_data.objects.filter(user_id=tok1['user_id'],queryset_id=queryset_id,server_id=server_id,dashboard_name=old_dashboard_name).exists():
                    models.dashboard_data.objects.filter(user_id=tok1['user_id'],queryset_id=queryset_id,server_id=server_id,dashboard_name=old_dashboard_name).update(dashboard_name=new_dashboard_name,updated_at=datetime.datetime.now())
                    return Response({'message':'Dashboard name updated successfully'},status=status.HTTP_200_OK)
                else:
                    return Response({'message':'Dashboard not exists'},status=status.HTTP_404_NOT_FOUND)
            else:
                return Response({'message':'Serializer not valid'},status=status.HTTP_400_BAD_REQUEST)
        else:
            return Response(tok1,status=tok1['status'])
        

def charts_dt(charts,tok1,server_id,file_id):
    if server_id=='' or server_id==None and file_id==None or file_id=='':
        database_name=''
    elif server_id==None or server_id=='':
        sdetails=models.FileDetails.objects.get(id=file_id)
        database_name=sdetails.display_name
    elif file_id==None or file_id=='':
        sdetails=models.ServerDetails.objects.get(id=server_id)
        database_name=sdetails.display_name
        
    cl_list=[]
    for ch in charts:
        durl=requests.get(ch['datasrc'])
        chid=models.charts.objects.get(id=ch['chart_id'])
        if chid.chart_type=="HIGHLIGHT_TABLES" or chid.chart_type=="Table":
            sheet_type="Table"
        else:
            sheet_type="Chart"
        data = {
            "database_name":database_name,
            "created_by":tok1['username'],
            "sheet_type":sheet_type,
            "chart":chid.chart_type,
            "chart_id":chid.id,
            "sheet_id":ch['id'],
            "sheet_tag_name":ch['sheet_tag_name'],
            "sheet_name":ch['sheet_name'],
            "server_id":ch['server_id'],
            "file_id":ch['file_id'],
            "queryset_id":ch['queryset_id'],
            "created":ch['created_at'].date(),
            "Modified":ch['updated_at'].date(),
            "sheet_data":durl.json()
        }
        cl_list.append(data)
    result={
        "database_name":database_name,
        "sheets":cl_list
    }
    return result


def dashboard_dt(charts,charts_user,tok1):
    cl_list=[]
    for ch in charts:
        gr_tb=models.grid_type.objects.get(id=ch['grid_id'])
        durl=requests.get(ch['datasrc'])
        data = {
            "created_by":tok1['username'],
            "sheet_ids":litera_eval(ch['sheet_ids']),
            "dashboard_id":ch['id'],
            "dashboard_name":ch['dashboard_name'],
            "dashboard_tag_name":ch['dashboard_tag_name'],
            "selected_sheet_ids":litera_eval(ch['selected_sheet_ids']),
            "server_id":litera_eval(ch['server_id']),
            "grid_type":gr_tb.grid_type,
            "height":ch['height'],
            "width":ch['width'],
            "file_id":litera_eval(ch['file_id']),
            "queryset_id":litera_eval(ch['queryset_id']),
            "dashboard_image":ch['imagesrc'],
            "database_name":None,
            "created":ch['created_at'].date(),
            "Modified":ch['updated_at'].date(),
            "dashboard_data":durl.json()
        }
        cl_list.append(data)
    for ch in charts_user:
        durl=requests.get(ch['datasrc'])
        gr_tb=models.grid_type.objects.get(id=ch['grid_id'])
        data = {
            "created_by":tok1['username'],
            "sheet_ids":litera_eval(ch['sheet_ids']),
            "dashboard_id":ch['id'],
            "dashboard_name":ch['dashboard_name'],
            "dashboard_tag_name":ch['dashboard_tag_name'],
            "selected_sheet_ids":litera_eval(ch['selected_sheet_ids']),
            "server_id":litera_eval(ch['server_id']),
            "file_id":litera_eval(ch['file_id']),
            "queryset_id":litera_eval(ch['queryset_id']),
            "dashboard_image":ch['imagesrc'],
            "grid_type":gr_tb.grid_type,
            "height":ch['height'],
            "width":ch['width'],
            "database_name":None,
            "created":ch['created_at'].date(),
            "Modified":ch['updated_at'].date(),
            "dashboard_data":durl.json()
        }
        cl_list.append(data)
    return cl_list


def query_sets(qrsets,tok1):
    qrsets_l=[]
    for qr in qrsets:
        if qr['server_id']==None or qr['server_id']=="" or qr['server_id']=='':
            ser_dt=models.FileDetails.objects.get(id=qr['file_id'])
        else:
            ser_dt=models.ServerDetails.objects.get(id=qr['server_id'])
        qrsets_filter=[]
        filter_ids=models.DataSource_querysets.objects.filter(queryset_id=qr['queryset_id']).values().order_by('-updated_at')
        qrsets_filter.append(dt for dt in filter_ids)
        data = {
            "database_name":ser_dt.display_name,
            "queryset_id":qr['queryset_id'],
            "created_by":tok1['username'],
            "queryset_name":qr['query_name'],
            "server_id":qr['server_id'],
            "is_custom_sql":qr['is_custom_sql'],
            "custom_query":qr['custom_query'],
            "created":qr['created_at'].date(),
            "modified":qr['updated_at'].date(),
            "datasource_filterdata":[item for sublist in qrsets_filter for item in sublist]
        }
        qrsets_l.append(data)
    return qrsets_l
    

def pagination(request,data,page_no,page_count):
    try:	
        paginator = Paginator(data,page_count)
        page = request.GET.get("page",page_no)
        object_list = paginator.page(page)
        re_data = list(object_list)
        data1 = {
            "status":200,
            "sheets":re_data,
            "total_pages":paginator.num_pages,
            "items_per_page":page_count,
            "total_items":paginator.count
        }
    except Exception as e:
        data1 = {
            "status":400,
            "data":e
        }
    return data1

## user all sheets based on quersetid,serverid, else all
class charts_fetch(CreateAPIView):
    serializer_class=serializers.charts_fetch_qr

    @transaction.atomic
    def post(self,request,token):
        role_list=roles.get_previlage_id(previlage=[previlages.view_sheet])
        tok1 = roles.role_status(token,role_list)
        if tok1['status']==200:
            serializer = self.serializer_class(data = request.data)
            if serializer.is_valid(raise_exception=True):
                queryset_id = serializer.validated_data['queryset_id']
                server_id = serializer.validated_data['server_id']
                search = serializer.validated_data['search']
                file_id = serializer.validated_data['file_id']
                if file_id==None or file_id=='':
                    if models.ServerDetails.objects.filter(id=server_id,user_id=tok1['user_id']).exists():
                        pass
                    else:
                        return Response({'message':'server id not exists'},status=status.HTTP_404_NOT_FOUND)
                else:
                    if models.FileDetails.objects.filter(id=file_id).exists():
                        pass
                    else:
                        return Response({'message':'File id not exists'},status=status.HTTP_404_NOT_FOUND)
                if file_id==None or file_id=='':
                    if models.QuerySets.objects.filter(user_id=tok1['user_id'],server_id=server_id,queryset_id=queryset_id).exists():
                        pass
                    else:
                        return Response({'message':'queryset id not exists'},status=status.HTTP_404_NOT_FOUND)
                else:
                    if models.QuerySets.objects.filter(user_id=tok1['user_id'],file_id=file_id,queryset_id=queryset_id).exists():
                        pass
                    else:
                        return Response({'message':'queryset id not exists'},status=status.HTTP_404_NOT_FOUND)
                if queryset_id=='' and server_id=='' or queryset_id=='' and file_id=='':
                    return Response({'message':'queryset_id, server_id fields are required'},status=status.HTTP_406_NOT_ACCEPTABLE)
                else:
                    pass
                if file_id==None or file_id=='':
                    if search=='':
                        charts = models.sheet_data.objects.filter(user_id=tok1['user_id'],server_id=server_id,queryset_id=queryset_id).values().order_by('updated_at')
                    else:
                        charts = models.sheet_data.objects.filter(user_id=tok1['user_id'],server_id=server_id,queryset_id=queryset_id,sheet_name__icontains=search).values().order_by('updated_at')
                else:
                    if search=='':
                        charts = models.sheet_data.objects.filter(user_id=tok1['user_id'],file_id=file_id,queryset_id=queryset_id).values().order_by('updated_at')
                    else:
                        charts = models.sheet_data.objects.filter(user_id=tok1['user_id'],file_id=file_id,queryset_id=queryset_id,sheet_name__icontains=search).values().order_by('updated_at')
                charts_data=charts_dt(charts,tok1,server_id,file_id)
                return Response(charts_data,status=status.HTTP_200_OK)
            else:
                return Response({'message':'Serializer value error'},status=status.HTTP_400_BAD_REQUEST)
        else:
            return Response(tok1,status=tok1['status'])
 

    def get(self,request,token):
        tok1 = views.test_token(token)
        if tok1['status']==200:
            charts = models.sheet_data.objects.filter(user_id=tok1['user_id']).values().order_by('-updated_at')
            server_id=''
            file_id=''
            charts_data=charts_dt(charts,tok1,server_id=server_id,file_id=file_id)
            return Response(charts_data,status=status.HTTP_200_OK)
        else:
            return Response(tok1,status=tok1['status'])
        

    @transaction.atomic
    def put(self,request,token):
        tok1 = views.test_token(token)
        if tok1['status']==200:
            serializer = self.get_serializer(data = request.data)
            if serializer.is_valid(raise_exception=True):
                search = serializer.validated_data['search']
                page_no = serializer.validated_data['page_no']
                page_count = serializer.validated_data['page_count']
                if search=='':
                    charts = models.sheet_data.objects.filter(user_id=tok1['user_id']).values().order_by('-updated_at')
                else:
                    charts = models.sheet_data.objects.filter(user_id=tok1['user_id'],sheet_name__icontains=search).values().order_by('-updated_at')
                server_id=''
                file_id=''
                charts_data=charts_dt(charts,tok1,server_id=server_id,file_id=file_id)
                try:
                    resul_data=pagination(request,charts_data['sheets'],page_no,page_count)
                    resul_data["database_name"]=charts_data['database_name']
                    return Response(resul_data,status=status.HTTP_200_OK)
                except:
                    return Response({'message':'Empty page/data not exists/selected count of records are not exists'},status=status.HTTP_400_BAD_REQUEST)
            else:
                return Response({'message':'Serializer value error'},status=status.HTTP_400_BAD_REQUEST)
        else:
            return Response(tok1,status=tok1['status'])


## user all dashboards based on quersetid,serverid, else all
class dashboard_fetch(CreateAPIView):
    serializer_class=serializers.charts_fetch_qr

    def post(self,request,token):
        role_list=roles.get_previlage_id(previlage=[previlages.view_dashboard])
        tok1 = roles.role_status(token,role_list)
        if tok1['status']==200:
            serializer = self.serializer_class(data = request.data)
            if serializer.is_valid(raise_exception=True):
                queryset_id = serializer.validated_data['queryset_id']
                server_id = serializer.validated_data['server_id']
                search = serializer.validated_data['search']
                file_id = serializer.validated_data['file_id']
                if queryset_id=='' and server_id=='' or  queryset_id=='' and file_id=='':
                    return Response({'message':'queryset_id, server_id or  queryset_id, file_id fields are required'},status=status.HTTP_406_NOT_ACCEPTABLE)
                else:
                    pass
                if file_id==None or file_id=='':
                    if search=='':
                        charts = models.dashboard_data.objects.filter(user_id=tok1['user_id'],server_id__contains=server_id,queryset_id__contains=queryset_id).values().order_by('updated_at')
                        charts_users = models.dashboard_data.objects.filter(user_ids__contains=str(tok1['user_id']),server_id__contains=server_id,queryset_id__contains=queryset_id).values().order_by('-updated_at')
                    else:
                        charts = models.dashboard_data.objects.filter(user_id=tok1['user_id'],server_id__contains=server_id,queryset_id__contains=queryset_id,dashboard_name__icontains=search).values().order_by('updated_at')
                        charts_users = models.dashboard_data.objects.filter(user_ids__contains=str(tok1['user_id']),server_id__contains=server_id,queryset_id__contains=queryset_id,dashboard_name__icontains=search).values().order_by('-updated_at')
                else:
                    if search=='':
                        charts = models.dashboard_data.objects.filter(user_id=tok1['user_id'],file_id__contains=file_id,queryset_id__contains=queryset_id).values().order_by('updated_at')
                        charts_users = models.dashboard_data.objects.filter(user_ids__contains=str(tok1['user_id']),file_id__contains=file_id,queryset_id__contains=queryset_id).values().order_by('-updated_at')
                    else:
                        charts = models.dashboard_data.objects.filter(user_id=tok1['user_id'],file_id__contains=file_id,queryset_id__contains=queryset_id,dashboard_name__icontains=search).values().order_by('updated_at')
                        charts_users = models.dashboard_data.objects.filter(user_ids__contains=str(tok1['user_id']),file_id__in=file_id,queryset_id__contains=queryset_id,dashboard_name__icontains=search).values().order_by('-updated_at')
                dashboards_data=dashboard_dt(charts,charts_users,tok1)
                return Response(dashboards_data,status=status.HTTP_200_OK)
            else:
                return Response({'message':'Serializer value error'},status=status.HTTP_400_BAD_REQUEST)
        else:
            return Response(tok1,status=tok1['status'])
        

    def get(self,request,token):
        tok1 = views.test_token(token)
        if tok1['status']==200:
            charts = models.dashboard_data.objects.filter(user_id=tok1['user_id']).values().order_by('-updated_at')
            charts_users = models.dashboard_data.objects.filter(user_ids__contains=str(tok1['user_id'])).values().order_by('-updated_at')
            dashboards_data=dashboard_dt(charts,charts_users,tok1)
            return Response(dashboards_data,status=status.HTTP_200_OK)
        else:
            return Response(tok1,status=tok1['status'])
        

    @transaction.atomic
    def put(self,request,token):
        tok1 = views.test_token(token)
        if tok1['status']==200:
            serializer = self.serializer_class(data = request.data)
            if serializer.is_valid(raise_exception=True):
                search = serializer.validated_data['search']
                page_no = serializer.validated_data['page_no']
                page_count = serializer.validated_data['page_count']
                if search=='':
                    charts = models.dashboard_data.objects.filter(user_id=tok1['user_id']).values().order_by('-updated_at')
                    charts_users = models.dashboard_data.objects.filter(user_ids__contains=str(tok1['user_id'])).values().order_by('-updated_at')
                else:
                    charts = models.dashboard_data.objects.filter(user_id=tok1['user_id'],dashboard_name__icontains=search).values().order_by('-updated_at')
                    charts_users = models.dashboard_data.objects.filter(user_ids__contains=str(tok1['user_id']),dashboard_name__icontains=search).values().order_by('-updated_at')
                dashboards_data=dashboard_dt(charts,charts_users,tok1)
                try:
                    resul_data=pagination(request,dashboards_data,page_no,page_count)
                    return Response(resul_data,status=status.HTTP_200_OK)
                except:
                    return Response({{'message':'Empty page/data not exists/selected count of records are not exists'}},status=status.HTTP_400_BAD_REQUEST)
            else:
                return Response({'message':'data not exists with the page'},status=status.HTTP_400_BAD_REQUEST)
        else:
            return Response(tok1,status=tok1['status'])
            


##### dashboard property update
class dashboard_property_update(CreateAPIView):
    serializer_class=serializers.dash_prop_update

    @transaction.atomic
    def post(self,request,token):
        role_list=roles.get_previlage_id(previlage=[previlages.edit_dashboard_filter,previlages.edit_dashboard_title,previlages.edit_dasboard])
        tok1 = roles.role_status(token,role_list)
        if tok1['status']==200:
            serializer = self.serializer_class(data = request.data)
            if serializer.is_valid(raise_exception=True):
                dashboard_id = serializer.validated_data['dashboard_id']
                role_ids = serializer.validated_data['role_ids']
                user_ids = serializer.validated_data['user_ids']
                if models.dashboard_data.objects.filter(id=dashboard_id).exists():
                    models.dashboard_data.objects.filter(id=dashboard_id).update(role_ids=role_ids,user_ids=user_ids)
                    return Response({'message':'updated successfully'},status=status.HTTP_200_OK)
                else:
                    return Response({'message':'dashboard not exists'},status=status.HTTP_404_NOT_FOUND)
            else:
                return Response({'message':'Serializer value error'},status=status.HTTP_400_BAD_REQUEST)
        else:
            return Response(tok1,status=tok1['status'])




##### List of user sheet names (only sheet names)
class user_list_names(CreateAPIView):
    serializer_class=serializers.charts_fetch_qr

    def post(self,request,token):
        tok1 = views.test_token(token)
        if tok1['status']==200:
            serializer = self.serializer_class(data = request.data)
            if serializer.is_valid(raise_exception=True):
                queryset_id = serializer.validated_data['queryset_id']
                server_id = serializer.validated_data['server_id']
                file_id = serializer.validated_data['file_id']
                sheet_list=[]
                if file_id==None or file_id=='':
                    sheet_dt=models.sheet_data.objects.filter(server_id=server_id,queryset_id=queryset_id,user_id=tok1['user_id']).values('id','sheet_name')
                else:
                    sheet_dt=models.sheet_data.objects.filter(file_id=file_id,queryset_id=queryset_id,user_id=tok1['user_id']).values('id','sheet_name')
                for sh in sheet_dt:
                    sheet_list.append({'id':sh['id'],'sheet_name':sh['sheet_name']})
                return Response({'data':sheet_list},status=status.HTTP_200_OK)
            else:
                return Response({'message':'Serializer value error'},status=status.HTTP_400_BAD_REQUEST)
        else:
            return Response(tok1,status=tok1['status'])   
        

def charts_select(charts,u_id):
    ch_list=[]
    for ch in charts:
        data = {
            "created_by":u_id,
            "sheet_id":ch['id'],
            "sheet_name":ch['sheet_name'],
            "server_id":ch['server_id'],
            "file_id":ch['file_id'],
            "queryset_id":ch['queryset_id'],
            "created":ch['created_at'].date(),
            "Modified":ch['updated_at'].date(),
            "is_selected":False
        }
        ch_list.append(data)
    return ch_list



def query_sheet_search(qrse,tok1,sheet_ids):
    final_list=[]
    for qr in qrse:
        sheet_data={}
        shtdt=models.sheet_data.objects.filter(queryset_id=qr['queryset_id']).values()
        sheet_data['queryset_name']=qr['query_name']
        sheet_data['is_selected']=False
        charts_dt=charts_select(shtdt,tok1['username'])
        sheet_data['sheet_data']=charts_dt
        final_list.append(sheet_data)
    for queryset in final_list:
        for sheet in queryset['sheet_data']:
            if sheet['sheet_id'] in sheet_ids:
                sheet['is_selected'] = True
        if all(sheet['is_selected'] for sheet in queryset['sheet_data']):
            queryset['is_selected'] = True
    try:
        data = {
            "status":200,
            "data":final_list
        }
        return data
    except:
        data = {
            "status":400,
            "message":'Empty page/data not exists/selected count of records are not exists'
        }
        return data
    

class user_sheets_list_data(CreateAPIView):
    serializer_class=serializers.sheets_list_seri

    @transaction.atomic
    def post(self,request,token):
        role_list=roles.get_previlage_id(previlage=[previlages.view_sheet,previlages.view_sheet_filters,previlages.view_dashboard,previlages.view_dashboard_filter])
        tok1 = roles.role_status(token,role_list)
        if tok1['status']==200:
            serializer = self.serializer_class(data = request.data)
            if serializer.is_valid(raise_exception=True):
                sheet_ids=serializer.validated_data['sheet_ids']
                search=serializer.validated_data['search']
                page_no=serializer.validated_data['page_no']
                page_count = serializer.validated_data['page_count']
                if sheet_ids=='' or sheet_ids==None:
                    sheet_ids=[]
                else:
                    sheet_ids=sheet_ids
                if search=='' or search==None:
                    qrse=models.QuerySets.objects.filter(user_id=tok1['user_id']).values()
                elif models.QuerySets.objects.filter(user_id=tok1['user_id'],query_name__icontains=search).exists():
                    qrse=models.QuerySets.objects.filter(user_id=tok1['user_id'],query_name__icontains=search).values()
                elif models.sheet_data.objects.filter(user_id=tok1['user_id'],sheet_name__icontains=search).exists():
                    sheet_dt=models.sheet_data.objects.filter(user_id=tok1['user_id'],sheet_name__icontains=search).values()
                    querysets_li=[qrl['queryset_id'] for qrl in sheet_dt]
                    final_list=[]
                    for qrs in querysets_li:
                        qrse=models.QuerySets.objects.filter(queryset_id=qrs).values()
                        sheets_data = query_sheet_search(qrse,tok1,sheet_ids)
                        if sheets_data['status']==200:
                            final_list.append(sheets_data['data'])
                        else:
                            return Response({'message':sheets_data['message']},status=sheets_data['status'])
                    try:
                        resul_data=pagination(request,[item for sublist in final_list for item in sublist],page_no,page_count)
                        return Response(resul_data,status=status.HTTP_200_OK)
                    except:
                        return Response({"message":'Empty page/data not exists/selected count of records are not exists'},status=status.HTTP_400_BAD_REQUEST)
                

                sheets_data = query_sheet_search(qrse,tok1,sheet_ids)
                if sheets_data['status']==200:
                    try:
                        resul_data=pagination(request,sheets_data['data'],page_no,page_count)
                        return Response(resul_data,status=status.HTTP_200_OK)
                    except:
                        return Response({"message":'Empty page/data not exists/selected count of records are not exists'},status=status.HTTP_400_BAD_REQUEST)
                else:
                    return Response({'message':sheets_data['message']},status=sheets_data['status'])
        else:
            return Response(tok1,status=tok1['status'])
    


class sheet_lists_data(CreateAPIView):
    serializer_class=serializers.sheets_list_seri

    @transaction.atomic
    def post(self,request,token):
        role_list=roles.get_previlage_id(previlage=[previlages.view_sheet,previlages.view_sheet_filters,previlages.view_dashboard,previlages.view_dashboard_filter])
        tok1 = roles.role_status(token,role_list)
        if tok1['status']==200:
            serializer = self.serializer_class(data = request.data)
            if serializer.is_valid(raise_exception=True):
                sheet_ids=serializer.validated_data['sheet_ids']
                sheets_list=[]
                for sh in sheet_ids:
                    if models.sheet_data.objects.filter(id=sh).exists():
                        pass
                    else:
                        return Response({'message':'sheet not exists'},status=status.HTTP_404_NOT_FOUND)
                    charts=models.sheet_data.objects.filter(id=sh).values()
                    shdt=models.sheet_data.objects.get(id=sh)
                    charts_data=charts_dt(charts,tok1,shdt.server_id,shdt.file_id)
                    sheets_list.append(charts_data)
                return Response(sheets_list,status=status.HTTP_200_OK)
            else:
                return Response({'message':'Serializer value error'},status=status.HTTP_400_BAD_REQUEST)
        else:
            return Response(tok1,status=tok1['status'])
            


# ###### fetch multiple sheets based on sheetname
# class multiple_sheets(CreateAPIView):
#     serializer_class=serializers.multiple_charts_data

#     @transaction.atomic
#     def post(self,request,token):
#         tok1 = views.test_token(token)
#         if tok1['status']==200:
#             serializer = self.serializer_class(data = request.data)
#             if serializer.is_valid(raise_exception=True):
#                 queryset_id=[]
#                 server_id=[]
#                 chart_names=[]
#                 ch_list=[]
#                 server_data=serializer.validated_data['server_data']
#                 files_data=serializer.validated_data['files_data']
#                 for dt in server_data:
#                     queryset_id.append(dt[0])
#                     server_id.append(dt[1])
#                     chart_names.append(dt[2])
#                 for q1,s1,c1 in zip(queryset_id,server_id,chart_names):
#                     if models.ServerDetails.objects.filter(id=s1,user_id=tok1['user_id']).exists():
#                         pass
#                     else:
#                         return Response({'message':'server id not exists'},status=status.HTTP_404_NOT_FOUND)
#                     if models.sheet_data.objects.filter(user_id=tok1['user_id'],queryset_id=q1,server_id=s1,sheet_name=c1).exists():
#                         pass
#                     else:
#                         return Response({'message':'sheet not exists for this user'},status=status.HTTP_404_NOT_FOUND)
#                     charts=models.sheet_data.objects.filter(user_id=tok1['user_id'],queryset_id=q1,server_id=s1,sheet_name=c1).values()
#                     server_id=''
#                     charts_data=charts_dt(charts,tok1,server_id=server_id)
#                     ch_list.append(charts_data)
#                 return Response(ch_list,status=status.HTTP_200_OK)
#             else:
#                 return Response({'message':'Serializer value error'},status=status.HTTP_400_BAD_REQUEST)
#         else:
#             return Response(tok1,status=tok1['status'])
        

class saved_queries(CreateAPIView):
    serializer_class=serializers.SearchFilterSerializer

    def get(self,request,token):
        tok1 = views.test_token(token)
        if tok1['status']==200:
            qrsets=models.QuerySets.objects.filter(user_id=tok1['user_id'],is_custom_sql=True).values().order_by('-updated_at')
            queryset=query_sets(qrsets,tok1)
            return Response(queryset,status=status.HTTP_200_OK)
        else:
            return Response(tok1,status=tok1['status'])

    @transaction.atomic
    def put(self,request,token):
        tok1 = views.test_token(token)
        if tok1['status']==200:
            serializer = self.serializer_class(data = request.data)
            if serializer.is_valid(raise_exception=True):
                search=serializer.validated_data['search']
                page_no=serializer.validated_data['page_no']
                page_count = serializer.validated_data['page_count']
                if search=='':
                    qrsets=models.QuerySets.objects.filter(user_id=tok1['user_id'],is_custom_sql=True).values().order_by('-updated_at')
                else:
                    qrsets=models.QuerySets.objects.filter(user_id=tok1['user_id'],is_custom_sql=True,query_name__icontains=search).values().order_by('-updated_at')
                queryset=query_sets(qrsets,tok1)
                try:
                    resul_data=pagination(request,queryset,page_no,page_count)
                    return Response(resul_data,status=status.HTTP_200_OK)
                except:
                    return Response({{'message':'Empty page/data not exists/selected count of records are not exists'}},status=status.HTTP_400_BAD_REQUEST)
            else:
                return Response({'message':'Serializer value error'},status=status.HTTP_400_BAD_REQUEST)
        else:
            return Response(tok1,status=tok1['status'])

        


##########################################################################################################################
class Multicolumndata(CreateAPIView):
    serializer_class = serializers.GetTableInputSerializer
    
    @transaction.atomic()
    def post(self, request,token):
        tok1 = views.test_token(token)
        if tok1['status']==200:
            serializer = self.serializer_class(data = request.data)
            if serializer.is_valid(raise_exception=True):
                db_id = serializer.validated_data['database_id']
                table_data = serializer.validated_data['table_1']
                try:
                    sd = ServerDetails.objects.get(id=db_id)
                except ServerDetails.DoesNotExist:
                    return Response({"message": "ServerDetails with the given ID does not exist."}, status=status.HTTP_404_NOT_FOUND)
                try:
                    st = ServerType.objects.get(id=sd.server_type)
                except ServerType.DoesNotExist:
                    return Response({"message": "ServerType with the given ID does not exist."}, status=status.HTTP_404_NOT_FOUND)

                server_conn=columns_extract.server_connection(sd.username,sd.password,sd.database,sd.hostname,sd.port,sd.service_name,st.server_type.upper(),sd.database_path)
                if server_conn['status']==200:
                    engine=server_conn['engine']
                    cursor=server_conn['cursor']
                else:
                    return Response(server_conn,status=server_conn['status'])

                response_col_data = {}
                for key, col_list in table_data.items():
                    for col in col_list:
                        query = text(f"SELECT {col} FROM {key}")
                        data = cursor.execute(query)
                        column_data = [row[0] for row in data]
                        key_col = f'{key}.{col}'
                        data_type = np.array(column_data).dtype
                        response_col_data[key_col] = column_data
                # cur.close()
                engine.dispose()   
                return Response({"column_data": response_col_data}, status=status.HTTP_200_OK)   
            return Response({'message':"Serializer Error"},status=status.HTTP_406_NOT_ACCEPTABLE)
        else:
            return Response(tok1,status=tok1['status'])
        


class Measure_Function(CreateAPIView):
    serializer_class = serializers.MeasureInputSerializer
    def post(self,request,token):
        tok1 = views.test_token(token)
        if tok1['status']==200:
            serializer = self.serializer_class(data=request.data)
            if serializer.is_valid(raise_exception=True):
                database_id = serializer.validated_data['database_id']
                query_set_id =serializer.validated_data['query_set_id']
                table = serializer.validated_data['tables']
                column = serializer.validated_data['columns']
                aggregate = serializer.validated_data['action']  
            else:
                return Response({'message':'serializer error'},status=status.HTTP_204_NO_CONTENT)
            user_id = tok1['user_id']
            if not QuerySets.objects.filter(queryset_id= query_set_id,server_id=database_id,user_id = tok1['user_id']).exists():
                return Response({"message":"Invalid QuerySet Id on Database ID for User"},status=status.HTTP_404_NOT_FOUND)
            try:
                server_details = ServerDetails.objects.get(user_id=user_id, id=database_id)
            except ServerDetails.DoesNotExist:
                return Response({"message": "Server details with the given user ID and database ID do not exist."}, status=status.HTTP_404_NOT_FOUND)

            try:
                ServerType1 = ServerType.objects.get(id=server_details.server_type)
            except ServerType.DoesNotExist:
                return Response({"message": "Server type with the given ID does not exist."}, status=status.HTTP_404_NOT_FOUND)
            server_conn=columns_extract.server_connection(server_details.username,server_details.password,server_details.database,server_details.hostname,server_details.port,server_details.service_name,ServerType1.server_type.upper(),server_details.database_path)
            if server_conn['status']==200:
                engine=server_conn['engine']
                cursor=server_conn['cursor']
            else:
                return Response(server_conn,status=server_conn['status'])
            try:
                query_data = QuerySets.objects.get(queryset_id = query_set_id)
                data_sourse_string = query_data.custom_query + ' order by 1'
                query = text(data_sourse_string)
                colu = cursor.execute(query)
                col_list = [column for column in colu.keys()]
                alias_data = table_name_from_query(query_data)
                table_aliass = alias_data['table_alias']
                alias_table = alias_data['tables']
                final_alias = associate_tables_with_aliases(table_aliass, alias_table)
                alias = []
                alias.append(final_alias.get(table))
                if column in col_list:
                    if '*' in data_sourse_string:
                        q = data_sourse_string.replace('*', "{}.{}".format(table, column))
                    else:
                        if table_aliass == []:
                            q = re.sub(r'(select\s+)(.*?)(\s+from\s+)', r'\1{}.{}\3'.format(table, column), data_sourse_string, flags=re.IGNORECASE)
                        else:
                            q = re.sub(r'(select\s+)(.*?)(\s+from\s+)', r'\1{}.{}\3'.format(alias[0], column), data_sourse_string, flags=re.IGNORECASE)        
                    query = text(q)                    
                    colu = cursor.execute(query)
                    table_data = colu.fetchall()
                    col_data =[]   
                    for i in table_data:
                        d1 = list(i)
                        col_data.append(d1[0])
                    column_data = np.array(col_data)
                    if np.issubdtype(column_data.dtype, np.number):
                        operation_data = {
                            'sum': np.sum(column_data),
                            'avg': np.mean(column_data),
                            'median': np.median(column_data),
                            'count': len(column_data),
                            'count_distinct': len(set(column_data)),
                            'minimum': np.min(column_data),
                            'maximum': np.max(column_data)
                        }
                    else:
                        operation_data = {
                            'minimum': min(column_data),
                            'maximum': max(column_data),
                            'count': len(column_data),
                            'count_distinct': len(set(column_data))
                        }
                    if aggregate in operation_data:
                        n = f"{aggregate}{column}"
                        response_data = {
                            "queryset_id": query_set_id,
                            "measure":aggregate,
                            "measure_string": f"{aggregate}({column})",
                            f"{aggregate}": operation_data[aggregate]
                            }
                    else:
                        return Response({"message":"no data match"})
                    cursor.close()
                    engine.dispose()   
                    return Response({"col_data":column,"data":response_data})
                
                else:
                    return Response({'message':"Column doesn't exists"},status=status.HTTP_404_NOT_FOUND)
            
            except Exception as e:
                return Response(f'{e}', status=status.HTTP_404_NOT_FOUND)
            except QuerySets.DoesNotExist:
                return Response({'message': 'Query set not found'}, status=status.HTTP_404_NOT_FOUND)
        else:
            return Response({'message':'Invalid Access Token'},status=status.HTTP_401_UNAUTHORIZED)

class Datasource_column_preview(CreateAPIView):
    serializer_class = serializers.Datasource_preview_serializer
    def post(self,request,token):
        tok1 = views.test_token(token)
        if tok1['status']==200:
            serializer = self.serializer_class(data=request.data)
            if serializer.is_valid(raise_exception=True):
                database_id = serializer.validated_data['database_id']
                query_set_id =serializer.validated_data['query_set_id']
                table = serializer.validated_data['tables']
                column = serializer.validated_data['columns']
                # data_type = serializer.validated_data['data_type']
                # format1 = serializer.validated_data['format1']
            else:
                return Response({'message':'serializer error'},status=status.HTTP_204_NO_CONTENT)
            user_id = tok1['user_id']
            if not QuerySets.objects.filter(queryset_id= query_set_id,server_id=database_id,user_id = tok1['user_id']).exists():
                return Response({"message":"Invalid QuerySet Id on Database ID for User"},status=status.HTTP_404_NOT_FOUND)
            try:
                server_details = ServerDetails.objects.get(user_id=user_id, id=database_id)
            except ServerDetails.DoesNotExist:
                return Response({"message": "Server details with the given user ID and database ID do not exist."}, status=status.HTTP_404_NOT_FOUND)

            try:
                ServerType1 = ServerType.objects.get(id=server_details.server_type)
            except ServerType.DoesNotExist:
                return Response({"message": "Server type with the given ID does not exist."}, status=status.HTTP_404_NOT_FOUND)
            server_conn=columns_extract.server_connection(server_details.username,server_details.password,server_details.database,server_details.hostname,server_details.port,server_details.service_name,ServerType1.server_type.upper(),server_details.database_path)
            if server_conn['status']==200:
                engine=server_conn['engine']
                cur=server_conn['cursor']
            else:
                return Response(server_conn,status=server_conn['status'])
            try:
                query_data = QuerySets.objects.get(queryset_id = query_set_id)
                data_sourse_string = query_data.custom_query + ' order by 1'
                query = text(data_sourse_string)
                colu = cur.execute(query)
                col_list = [column for column in colu.keys()]
                alias_data = table_name_from_query(query_data)
                table_aliass = alias_data['table_alias']
                alias_table = alias_data['tables']
                final_alias = associate_tables_with_aliases(table_aliass, alias_table)
                alias = []
                where_condition = re.search(r'where\s+([^;]+)', data_sourse_string, re.IGNORECASE)
                alias.append(final_alias.get(table))
                
                if column in col_list:
                    if '*' in data_sourse_string:
                        q = data_sourse_string.replace('*', "{}.{}".format(table, column))
                    else:
                        if table_aliass == []:
                            q = re.sub(r'(select\s+)(.*?)(\s+from\s+)', r'\1{}.{}\3'.format(table, column), data_sourse_string, flags=re.IGNORECASE)
                        else:
                            q = re.sub(r'(select\s+)(.*?)(\s+from\s+)', r'\1{}.{}\3'.format(alias[0], column), data_sourse_string, flags=re.IGNORECASE) 
                    query = text(q)                    
                    colu = cur.execute(query)
                    table_data = colu.fetchall()
                    col_data =[]   
                    for i in table_data:
                        d1 = list(i)
                        col_data.append(d1[0])
                    cur.close()
                    engine.dispose()    
                    return Response({"col_data":column,'row_data':col_data})
                else:
                    return Response({'message':"Column doesn't exists"},status=status.HTTP_404_NOT_FOUND)   
            except Exception as e:
                return Response(f'{e}', status=status.HTTP_404_NOT_FOUND)
            except QuerySets.DoesNotExist:
                return Response({'message': 'Query set not found'}, status=status.HTTP_404_NOT_FOUND)
        else:
            return Response({'message':'Invalid Access Token'},status=status.HTTP_401_UNAUTHORIZED)
        
            
def get_table_alias(tables, table_alias, table_name):
    if table_name in tables:
        index = tables.index(table_name)
        return table_alias[index]
    else:
        return table_alias[tables.index('')]
            

class Datasource_filter(CreateAPIView):
    serializer_class = serializers.Datasource_filter_Serializer
    @transaction.atomic()
    def post(self,request,token):
        role_list=roles.get_previlage_id(previlage=[previlages.create_datasource_filters])
        tok1 = roles.role_status(token,role_list)
        if tok1['status']==200:
            serializer = self.serializer_class(data=request.data)
            if serializer.is_valid(raise_exception=True):
                database_id = serializer.validated_data['database_id']
                query_set_id =serializer.validated_data['query_set_id']
                tables = serializer.validated_data['tables']
                alias = serializer.validated_data['alias']
                columns = serializer.validated_data['columns']
                data_type  = serializer.validated_data['data_type']
                input_list = serializer.validated_data['input_list']
                format1 = serializer.validated_data['format']
            else:
                return Response({'message':'serializer error'},status=status.HTTP_204_NO_CONTENT)
            
            user_id = tok1['user_id']
            if not QuerySets.objects.filter(queryset_id= query_set_id,server_id=database_id,user_id = tok1['user_id']).exists():
                return Response({"message":"Invalid QuerySet Id on Database ID for User"},status=status.HTTP_404_NOT_FOUND)
                        
            server_details = ServerDetails.objects.get(user_id = user_id,id=database_id)
            ServerType1 = ServerType.objects.get(id = server_details.server_type)
            server_conn=columns_extract.server_connection(server_details.username,server_details.password,server_details.database,server_details.hostname,server_details.port,server_details.service_name,ServerType1.server_type.upper(),server_details.database_path)
            if server_conn['status']==200:
                engine=server_conn['engine']
                cur=server_conn['cursor']
            else:
                return Response(server_conn,status=server_conn['status'])
            column_data = []
            try:
                query_data = QuerySets.objects.get(queryset_id = query_set_id)
                # replace_string  = re.search(r'select(.+?)from',  query_data.custom_query, re.DOTALL|re.IGNORECASE)
                data_sourse_string = query_data.custom_query
                format_data = transform_list(input_list)
                alias_data = table_name_from_query(query_data)
                if query_data.is_custom_sql == True:
                    alias_names = alias_data['table_alias']
                    alias_table = alias_data['tables']
                    final_alias = associate_tables_with_aliases(alias_names, alias_table)
                    if alias == [] and alias_names != []:
                        for table in tables:
                            if table in final_alias:
                                alias.append(final_alias[table])
                            else:
                                pass
                    else:
                        pass
                qr=''
                where_condition = re.search(r'where\s+([^;]+)', data_sourse_string, re.IGNORECASE)
                for i in range(len(tables)):
                    if i ==0:
                        fom = str(format_data[i])
                        data = fom.strip("'")
                        if any(dt in data_type[i].lower() for dt in ['char', 'int', 'text']) and not format1[i] in ['end','start','exact','contains','date','time'] or format1[i] in ['selected']:
                            try:
                                if where_condition:
                                    qr += data_sourse_string + f' and {alias[i]}.{columns[i]} in ({format_data[i]})'
                                else:
                                    qr += data_sourse_string + f' where {alias[i]}.{columns[i]} in ({format_data[i]})'
                            except:
                                if where_condition:
                                    qr += data_sourse_string + f' and {tables[i]}.{columns[i]} in ({format_data[i]})'
                                else:
                                    qr += data_sourse_string + f' where {tables[i]}.{columns[i]} in ({format_data[i]})'                        
                        elif 'start' in format1[i].lower():
                            try:
                                if where_condition:
                                    qr += data_sourse_string + f" and  {alias[i]}.{columns[i]} like '{data}%'"
                                else:
                                    qr += data_sourse_string + f" where  {alias[i]}.{columns[i]} like '{data}%'"
                            except:
                                if where_condition:
                                    qr += data_sourse_string + f" and  {alias[i]}.{columns[i]} like '{data}%'"
                                else:
                                    qr += data_sourse_string + f" where  {alias[i]}.{columns[i]} like '{data}%'"  
                        elif 'end' in format1[i].lower():
                            try:
                                if where_condition:                   
                                    qr += data_sourse_string + f" and  {alias[i]}.{columns[i]} like '%{data}'"
                                else:
                                    qr += data_sourse_string + f" where  {alias[i]}.{columns[i]} like '%{data}'"
                            except:
                                if where_condition:
                                    qr += data_sourse_string + f" and  {tables[i]}.{columns[i]} like '%{data}'"
                                else:
                                    qr += data_sourse_string + f" where  {tables[i]}.{columns[i]} like '%{data}'"
                        elif 'contains' in format1[i].lower():
                            try:
                                if where_condition:
                                    qr += data_sourse_string + f" where  {alias[i]}.{columns[i]} like '%{data}%'"
                                else:
                                    qr += data_sourse_string + f" where  {alias[i]}.{columns[i]} like '%{data}%'"
                            except:
                                if where_condition:
                                    qr += data_sourse_string + f" where  {tables[i]}.{columns[i]} like '%{data}%'"
                                else:
                                    qr += data_sourse_string + f" where  {tables[i]}.{columns[i]} like '%{data}%'"      
                        elif 'exact' in format1[i].lower():         
                            try:
                                if where_condition:
                                    qr += data_sourse_string + f" and  {alias[i]}.{columns[i]} like '{data}'"
                                else:
                                    qr += data_sourse_string + f" where  {alias[i]}.{columns[i]} like '{data}'"
                            except:
                                if where_condition:
                                    qr += data_sourse_string + f" and  {tables[i]}.{columns[i]} like '{data}'"
                                else:
                                    qr += data_sourse_string + f" where  {tables[i]}.{columns[i]} like '{data}'"
                        elif 'date' or 'time' in format1[i].lower():
                            try:
                                if where_condition:
                                    qr += data_sourse_string + f" and TO_CHAR({tables[i]}.{columns[i]}, '{format1[i]}') = {format_data[i]}"
                                else:
                                    qr += data_sourse_string + f" where TO_CHAR({tables[i]}.{columns[i]}, '{format1[i]}') = {format_data[i]} "
                            except:
                                if where_condition:
                                    qr += data_sourse_string + f" and TO_CHAR({alias[i]}.{columns[i]}, '{format1[i]}') = {format_data[i]} "
                                else:
                                    qr += data_sourse_string + f" where TO_CHAR({alias[i]}.{columns[i]}, '{format1[i]}') = {format_data[i]} "
                    else:
                        where_condition2 = re.search(r'where\s+([^;]+)', qr, re.IGNORECASE)
                        fom = str(format_data[i])
                        data = fom.strip("'")                      
                        if any(dt in data_type[i].lower() for dt in ['char', 'int', 'text']) and not format1[i] in ['end','start','exact','contains','date','time'] or format1[i] in ['selected']:
                            try:
                                if where_condition2:
                                    qr += f' and {alias[i]}.{columns[i]} in ({format_data[i]})'
                                else:
                                    qr += f' where {alias[i]}.{columns[i]} in ({format_data[i]})'
                            except:
                                if where_condition2:
                                    qr += f' and {tables[i]}.{columns[i]} in ({format_data[i]})'
                                else:
                                    qr += f' where {tables[i]}.{columns[i]} in ({format_data[i]})'
                        elif 'start' in format1[i].lower():
                            try:
                                if where_condition2:
                                    qr += f" and  {alias[i]}.{columns[i]} like '{data}%'" 
                                else:
                                    qr += f" where  {alias[i]}.{columns[i]} like '{data}%'" 
                            except:  
                                if where_condition2:
                                    qr += f" and  {tables[i]}.{columns[i]} like '{data}%'" 
                                else:
                                    qr += f" where  {tables[i]}.{columns[i]} like '{data}%'" 
                        elif 'end' in format1[i].lower():
                            try:
                                if where_condition2:
                                    qr += f" and {alias[i]}.{columns[i]} like '%{data}'"
                                else:
                                    qr += f" where {alias[i]}.{columns[i]} like '%{data}'"
                            except:
                                if where_condition2:
                                    qr += f" and {alias[i]}.{columns[i]} like '%{data}'"
                                else:
                                    qr += f" where {alias[i]}.{columns[i]} like '%{data}'"
                        elif 'contains' in format1[i].lower():
                            try:
                                if where_condition2:
                                    qr += f" and  {alias[i]}.{columns[i]} like '%{data}%'"
                                else:
                                    qr += f" where  {alias[i]}.{columns[i]} like '%{data}%'" 
                            except:
                                if where_condition2:
                                    qr += f" and  {tables[i]}.{columns[i]} like '%{data}%'"
                                else:
                                    qr += f" where  {tables[i]}.{columns[i]} like '%{data}%'" 
                        elif 'exact' in format1[i].lower():
                            try:
                                if where_condition2:
                                    qr += f" and  {alias[i]}.{columns[i]} like '{data}'" 
                                else:
                                    qr += f" where  {alias[i]}.{columns[i]} like '{data}'" 
                            except:
                                if where_condition2:
                                    qr += f" and  {alias[i]}.{columns[i]} like '{data}'" 
                                else:
                                    qr += f" where  {alias[i]}.{columns[i]} like '{data}'" 
                        elif 'date' or 'time' in format1[i].lower():
                            try:
                                if where_condition2:
                                    qr += f" and TO_CHAR({tables[i]}.{columns[i]}, '{format1[i]}') = {format_data[i]}"
                                else:
                                    qr += f" where TO_CHAR({tables[i]}.{columns[i]}, '{format1[i]}') = {format_data[i]} "
                            except:
                                if where_condition2:
                                    qr += f" and TO_CHAR({alias[i]}.{columns[i]}, '{format1[i]}') = {format_data[i]} "
                                else:
                                    qr += f" where TO_CHAR({alias[i]}.{columns[i]}, '{format1[i]}') = {format_data[i]} "
                        else:
                            return Response({'message':"Please Select Suitable Format"},status=status.HTTP_400_BAD_REQUEST)                
                if qr.count('where') > 1:
                    qr = remove_second_where_condition(qr)
                else:
                    pass
                data = cur.execute(text(qr))
                col_list = [column for column in data.keys()]
                col_data = data.fetchall()  
                for row in col_data:
                    aa = list(row)
                    column_data.append(aa)
            except Exception as e:
                return Response(f'{e}', status=status.HTTP_404_NOT_FOUND)
            except QuerySets.DoesNotExist:
                return Response({'message': 'Query set not found'}, status=status.HTTP_404_NOT_FOUND)        
            cur.close()
            engine.dispose()
            models.QuerySets.objects.filter(user_id=tok1['user_id'],queryset_id=query_set_id).update(custom_query = qr,updated_at=datetime.datetime.now())
            dsf = models.DataSourceFilter.objects.create(
                server_id = database_id,
                user_id = tok1['user_id'],
                queryset_id = query_set_id,
                tables = tables,
                alias = alias,
                datatype = data_type,
                columns = columns,
                custom_selected_data = input_list,
                filter_type = format1
            )
            data ={
                "database_id":database_id,
                "user_id":tok1['user_id'],
                "queryset_id":query_set_id,
                'column_data':col_list,
                'row_data':column_data,
                "is_custom":query_data.is_custom_sql,
                "datasource_filter_id":dsf.filter_id,
                "updated_at":query_data.updated_at
            }
            return Response(data, status=status.HTTP_200_OK)
        
        return Response(tok1,status=tok1['status'])



def associate_tables_with_aliases(table_aliass, alias_table):
    table_associations = {}
    for alias, table in zip(table_aliass, alias_table):
        table_associations[table] = alias
    return table_associations


def remove_second_where_condition(query):

    first_where_index = query.lower().find('where')

    if first_where_index != -1:
        second_where_index = query.lower().find('where', first_where_index + 1)
        if second_where_index != -1:
            second_where_end = query.find('\n', second_where_index)
            if second_where_end == -1:
                second_where_end = len(query)
            query = query[:second_where_index] + query[second_where_end:]

    return query.strip()  


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

import os,requests,pdfplumber,boto3,ast,random,re,secrets,string
from project import settings
import pandas as pd
from dashboard import views,serializers,models,authentication,previlages,Connections
import datetime
from io import BytesIO
from pytz import utc
from django.db import transaction
from django.views.decorators.csrf import csrf_exempt
from rest_framework.generics import CreateAPIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.decorators import api_view
from django.template.loader import render_to_string
from django.core.mail import send_mail
from oauth2_provider.models import AccessToken


created_at=datetime.datetime.now()
updated_at=datetime.datetime.now()


def get_previlage_id(previlage):
    rol_lst=[]
    for previ in previlage:
        prvg=models.previlages.objects.get(previlage=previ)
        roles=models.Role.objects.filter(previlage_id__contains=prvg.id).values('role_id')
        rl_ls = [rl['role_id'] for rl in roles]
    rol_lst.append(rl_ls)
    role_list=[item for sublist in rol_lst for item in sublist]
    return role_list


def role_status(token,rl_id):
    tok12 = views.test_token(token)
    # if tok12['status']==200 and list(filter(lambda x: x['role_id'] in rl_id, [tok12]))!=[]:
    if tok12['status']==200 and list(filter(lambda x: x in tok12['role_id'], rl_id))!=[]:
        data = {
            "status":200,
            # "tok1":{
            "role_id":tok12['role_id'],
            "user_id":tok12['user_id'],
            "usertable":tok12['usertable'],
            "username":tok12['username'],
            "email":tok12['email']
            # }
        }
        return data
    elif tok12['status']==200 and list(filter(lambda x: x['role_id'] in rl_id, [tok12]))==[]:
        data = {
            "status":401,
            # "tok1":{
            "message":"User Not assigned to this ROLE/Not Assigned"
            # }
        }
        return data
    else:
        data = {
            "status":400,
            # "tok1":{
            "message":tok12['message']
            # }
        }
        return data
    

def TM_mail_SMTP_mail(username,password,created_by,liscence,supportemail,email):
    try:
        context = {'username':username,'E_mail':email,'password':password,'created_by':created_by,'liscence':liscence,'supportemail':supportemail,'IP':settings.link_url}
        html_message = render_to_string('role.html', context)
        message = '{} Joined successfully'.format(username)
        subject = "{} Joining".format(username)
        from_email = settings.EMAIL_HOST_USER
        to_email = [email.lower()]
        send_mail(subject, message, from_email, to_email, html_message=html_message)
        data = {
            "status":200,
            "message" : "Account Activation Email Sent",
        }
        return data
    except :
        data = {
            "status":400,
            "message":"SMTP Error"
        }
        return data


class previlages_get(CreateAPIView):
    serializer_class=serializers.search_serializer

    @transaction.atomic
    def put(self,request,token):
        role_list=get_previlage_id(previlage=[previlages.view_previlages])
        tok1 = role_status(token,role_list)
        if tok1['status']==200:
            serializer = self.serializer_class(data=request.data)
            if serializer.is_valid(raise_exception=True):
                search = serializer.validated_data['search']
                pr_list=[]
                if search=='' or search==None:
                    prev=models.previlages.objects.all().values()
                else:
                    prev=models.previlages.objects.filter(previlage__icontains=search).values()
                for i1 in prev:
                    pr_list.append({"id":i1['id'],
                    "previlage":i1['previlage']})
                return Response(pr_list,status=status.HTTP_200_OK)
            else:
                return Response({'message':'serializer value error'},status=status.HTTP_400_BAD_REQUEST)
        else:
            return Response(tok1,status=tok1['status'])
    

class add_role(CreateAPIView):
    serializer_class=serializers.role_seri

    @transaction.atomic
    def post(self,request,token):
        role_list=get_previlage_id(previlage=[previlages.create_roles,previlages.view_roles])
        tok1 = role_status(token,role_list)
        if tok1['status']==200:
            serializer = self.get_serializer(data=request.data)
            if serializer.is_valid(raise_exception=True):
                role_name12 = serializer.validated_data['role_name']
                role_description = serializer.validated_data['role_description']
                previlages_ids = serializer.validated_data['previlages']
                if models.Role.objects.filter(role__exact=role_name12).exists():
                    return Response({'message':'Role already exists'},status=status.HTTP_406_NOT_ACCEPTABLE)
                else:
                    # for prid in ast.literal_eval(previlages):
                    # for prid in previlages:
                    rlct=models.Role.objects.create(role=role_name12,role_desc=role_description,created_by=tok1['user_id'],previlage_id=previlages_ids,
                                                created_at=created_at,updated_at=updated_at)
                    rlct.save()
                    return Response({'message':'Role created successfully'},status=status.HTTP_200_OK)
            else:
                return Response({'message':'serializer value error'},status=status.HTTP_400_BAD_REQUEST)
        else:
            return Response(tok1,status=tok1['status'])
        
class list_of_roles(CreateAPIView):
    serializer_class=serializers.search_serializer

    @transaction.atomic
    def put(self,request,token):
        role_list=get_previlage_id(previlage=[previlages.view_roles])
        tok1 = role_status(token,role_list)
        if tok1['status']==200:
            serializer=self.get_serializer(data=request.data)
            if serializer.is_valid(raise_exception=True):
                search=serializer.validated_data['search']
                page_no=serializer.validated_data['page_no']
                page_count=serializer.validated_data['page_count']
                if search=='' or search==None:
                    rol_pr=models.Role.objects.filter(created_by=tok1['user_id']).values().order_by('-updated_at')
                else:
                    rol_pr=models.Role.objects.filter(created_by=tok1['user_id'],role__icontains=search).values().order_by('-updated_at')
                final_list=[]
                prev_name_li=[]
                for role in rol_pr:
                    for i3 in ast.literal_eval(role['previlage_id']):
                        prev_name=models.previlages.objects.get(id=i3)
                        prev_name_li.append(prev_name.previlage)
                    data = {
                        "role_id":role['role_id'],
                        "created_by":tok1['username'],
                        "role":role['role'],
                        "previlages":prev_name_li,
                        "updated_at":role['updated_at'].date(),
                        "created_at":role['created_at'].date()
                    }  
                    final_list.append(data)   
                try:
                    resul_data=Connections.pagination(request,final_list,page_no,page_count)
                    return Response(resul_data,status=status.HTTP_200_OK)
                except:
                    return Response({'message':'Empty page/data not exists/selected count of records are not exists'},status=status.HTTP_400_BAD_REQUEST)  
            else:
                return Response({'message':'serializer value error'},status=status.HTTP_400_BAD_REQUEST)
        else:
            return Response(tok1,status=tok1['status'])
        
    
    @transaction.atomic
    def get(self,request,token):
        role_list=get_previlage_id(previlage=[previlages.view_roles])
        tok1 = role_status(token,role_list)
        if tok1['status']==200:
            roles_list=models.Role.objects.filter(created_by=tok1['user_id']).values('role').order_by('-updated_at')
            roleslist=[rl['role'] for rl in roles_list]
            return Response(roleslist,status=status.HTTP_200_OK)
        else:
            return Response(tok1,status=tok1['status'])
        

class create_user_role(CreateAPIView):
    serializer_class=serializers.adding_user_serializer

    @transaction.atomic
    def post(self,request,token):
        role_list=get_previlage_id(previlage=[previlages.create_user])
        tok1 = role_status(token,role_list)
        if tok1['status']==200:
            serializer = self.serializer_class(data=request.data)
            if serializer.is_valid(raise_exception=True):
                firstname = serializer.validated_data['firstname']
                lastname = serializer.validated_data['lastname']
                username = serializer.validated_data['username']
                email = serializer.validated_data['email']
                active = serializer.validated_data['is_active']
                password = serializer.validated_data['password']
                conformpassword = serializer.validated_data['conformpassword']
                role = serializer.validated_data['role']
                if models.UserProfile.objects.filter(username=username).exists():
                    return Response({"message": "username already exists"}, status=status.HTTP_400_BAD_REQUEST)
                elif models.UserProfile.objects.filter(email=email).exists():
                    return Response({"message": "email already exists"}, status=status.HTTP_400_BAD_REQUEST)
                elif role=='' or role==None or role==[]:
                    return Response({'message':'Empty Role field is not acceptable'},status=status.HTTP_406_NOT_ACCEPTABLE)
                else:
                    for i21 in role:
                        if models.Role.objects.filter(created_by=tok1['user_id'],role=i21).exists():
                            pass
                        else:
                            return Response({'message':'Role not exists for this User'},status=status.HTTP_404_NOT_FOUND)
                
                pattern = "^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[@#$!%*?&])[A-Za-z\d@#$!%*?&]{8,}$"
                r= re.findall(pattern,password)
                if not r:
                    return Response({"message":"Password is invalid.Min 8 character. Password must contain at least :one small alphabet one capital alphabet one special character \nnumeric digit."},status=status.HTTP_406_NOT_ACCEPTABLE)
                elif password!=conformpassword:
                    return Response({"message":"Password did not matched"},status=status.HTTP_406_NOT_ACCEPTABLE)
                liscence_key = ''.join((secrets.choice(string.ascii_letters + string.digits + string.punctuation) for i in range(300)))
                tm_mail=TM_mail_SMTP_mail(username,password,tok1['username'],liscence_key,tok1['email'],email=email)
                if tm_mail['status']==200:
                    pass
                else:
                    return Response(tm_mail,status=status.HTTP_400_BAD_REQUEST)
                up_tb=models.UserProfile.objects.create_user(username=username,name=username,password=password,email=email,created_at=created_at,updated_at=updated_at,first_name=firstname,last_name=lastname,is_active=active)
                for i1 in role:
                    role_tb=models.Role.objects.get(created_by=tok1['user_id'],role=i1)
                    models.UserRole.objects.create(role_id=role_tb.role_id,user_id=up_tb.id)
                models.license_key.objects.filter(user_id=up_tb.id).delete()
                models.license_key.objects.create(user_id=up_tb.id,max_limit=settings.db_connections,key=liscence_key,created_at=created_at,updated_at=updated_at,is_validated=True)
                return Response({'message':'created successfully'},status=status.HTTP_200_OK)
            else:
                return Response({'message':'serializer value error'},status=status.HTTP_400_BAD_REQUEST)
        else:
            return Response(tok1,status=tok1['status'])
        


class get_user_role(CreateAPIView):
    serializer_class=serializers.search_serializer

    @transaction.atomic
    def put(self,request,token):
        role_list=get_previlage_id(previlage=[previlages.view_user,previlages.view_roles])
        tok1 = role_status(token,role_list)
        if tok1['status']==200:
            serializer=self.get_serializer(data=request.data)
            if serializer.is_valid(raise_exception=True):
                search=serializer.validated_data['search']
                page_no=serializer.validated_data['page_no']
                page_count=serializer.validated_data['page_count']
                rol_tb=models.Role.objects.filter(created_by=tok1['user_id']).values('role_id','role')
                role_ids=[role['role_id'] for role in rol_tb]
                role=[role['role'] for role in rol_tb]
                user=[]
                for rl,role in zip(role_ids,role):
                    up_tb=models.UserRole.objects.filter(role_id=rl).values()
                    user_ids=[usid['user_id'] for usid in up_tb]
                    for us_id in user_ids:
                        if search=='' or search==None:
                            final_user=models.UserProfile.objects.filter(id=us_id).values().order_by('-updated_at')
                        else:
                            final_user=models.UserProfile.objects.filter(id=us_id,username__icontains=search).values().order_by('-updated_at')
                        for i2 in final_user:
                            data = {
                                "user_id":i2['id'],
                                "name":i2['username'],
                                "username":i2['username'],
                                "email":i2['email'],
                                "is_active":i2['is_active'],
                                "created_by":tok1['username'],
                                "created_at":i2['created_at'].date(),
                                "updated_at":i2['updated_at'].date(),
                                "role":role,
                            }
                            user.append(data)
                try:
                    resul_data=Connections.pagination(request,user,page_no,page_count)
                    return Response(resul_data,status=status.HTTP_200_OK)
                except:
                    return Response({'message':'Empty page/data not exists/selected count of records are not exists'},status=status.HTTP_400_BAD_REQUEST) 
            else:
                return Response({'message':'Serializer value error'},status=status.HTTP_400_BAD_REQUEST)
        else:
            return Response(tok1,status=tok1['status'])

    

@api_view(['DELETE'])
def delete_user(request,userid,token):
    if request.method=='DELETE':
        role_list=get_previlage_id(previlage=[previlages.delete_user])
        tok1 = role_status(token,role_list)
        if tok1['status']==200:
            if models.UserRole.objects.filter(user_id=userid).exists():
                us_role=models.UserRole.objects.get(user_id=userid)
                if models.Role.objects.filter(role_id=us_role.role_id,created_by=tok1['user_id']).exists():
                    pass
                else:
                    return Response({'message':'Not allowed to delete user'},status=status.HTTP_406_NOT_ACCEPTABLE)
            else:
                return Response({'message':'User not exists'},status=status.HTTP_404_NOT_FOUND)
            models.UserRole.objects.filter(user_id=userid).delete()
            models.UserProfile.objects.filter(id=userid).delete()
            models.license_key.objects.filter(user_id=userid).delete()
            models.ServerDetails.objects.filter(user_id=userid).delete()
            models.FileDetails.objects.filter(user_id=userid).delete()
            return Response({'message':'Deleted successfully'},status=status.HTTP_200_OK)
        else:
            return Response(tok1,status=tok1['status'])
    else:
        return Response({'message':'Method not allowed'},status=status.HTTP_405_METHOD_NOT_ALLOWED)


class edit_roles(CreateAPIView):
    serializer_class=serializers.previlage_seri

    @transaction.atomic
    def put(self,request,rl_id,token):
        role_list=get_previlage_id(previlage=[previlages.edit_roles])
        tok1 = role_status(token,role_list)
        if tok1['status']==200:
            serializer=self.get_serializer(data=request.data)
            if serializer.is_valid(raise_exception=True):
                role_name=serializer.validated_data['role']
                previlage_list=serializer.validated_data['previlage_list']
                if models.Role.objects.filter(created_by=tok1['user_id'],role_id=rl_id).exists():
                    models.Role.objects.filter(created_by=tok1['user_id'],role_id=rl_id).update(previlage_id=previlage_list,role=role_name,updated_at=updated_at)
                    urtb=models.UserRole.objects.filter(role_id=rl_id).values()
                    for us in urtb:
                        AccessToken.objects.filter(user_id=us['user_id']).delete()
                    return Response({'message':'updated successfully'},status=status.HTTP_200_OK)
                else:
                    return Response({'message':'Role not exists for this user'},status=status.HTTP_404_NOT_FOUND)
            else:
                return Response({'message':'serializer value error'},status=status.HTTP_400_BAD_REQUEST)
        else:
            return Response(tok1,status=tok1['status'])
        

class edit_users(CreateAPIView):
    serializer_class=serializers.update_user_serializer

    @transaction.atomic
    def put(self,request,us_id,token):
        role_list=get_previlage_id(previlage=[previlages.edit_roles,previlages.edit_user])
        tok1 = role_status(token,role_list)
        if tok1['status']==200:
            serializer=self.get_serializer(data=request.data)
            if serializer.is_valid(raise_exception=True):
                firstname = serializer.validated_data['firstname']
                lastname = serializer.validated_data['lastname']
                username = serializer.validated_data['username']
                email = serializer.validated_data['email']
                active = serializer.validated_data['is_active']
                role = serializer.validated_data['role']
                if models.UserProfile.objects.filter(id=us_id).exists():
                    user_tb=models.UserProfile.objects.get(id=us_id)
                    for rl in role:
                        if models.Role.objects.filter(role=rl,created_by=tok1['user_id']).exists():
                            pass
                        else:
                            return Response({'message':'{} role not allowed to assign'.format(rl)},status=status.HTTP_406_NOT_ACCEPTABLE)
                    usrole=models.UserRole.objects.filter(user_id=us_id).values()
                    for rl_id in usrole:
                        if models.Role.objects.filter(role_id=rl_id['role_id'],created_by=tok1['user_id']).exists():
                            pass
                        else:
                            return Response({'message':'{} user not allowed to edit'.format(user_tb.username)},status=status.HTTP_406_NOT_ACCEPTABLE)
                    models.UserProfile.objects.filter(id=us_id).update(first_name=firstname,last_name=lastname,updated_at=updated_at,email=email,username=username,name=username,
                                                                       is_active=active)
                    models.UserRole.objects.filter(user_id=us_id).delete()
                    for rl in role:
                        rltb=models.Role.objects.get(role=rl)
                        models.UserRole.objects.create(role_id=rltb.role_id,user_id=us_id)
                    return Response({'message':'Details updated successfully'},status=status.HTTP_200_OK)
                else:
                    return Response({'message':'user not exists'},status=status.HTTP_404_NOT_FOUND)
        else:
            return Response(tok1,status=tok1['status'])
        

@api_view(['GET'])
def role_details(request,rl_id,token):
    if request.method=='GET':
        role_list=get_previlage_id(previlage=[previlages.view_roles])
        tok1 = role_status(token,role_list)
        if tok1['status']==200:
            roles_list=models.Role.objects.get(role_id=rl_id)
            users_names=models.UserRole.objects.filter(role_id=roles_list.role_id).values()
            names=[]
            for i2 in users_names:
                ustb=models.UserProfile.objects.get(id=i2['user_id'])
                names.append(ustb.username)
            pr_list=[]
            for i3 in ast.literal_eval(roles_list.previlage_id):
                prev_name=models.previlages.objects.get(id=i3)
                data = {
                    "id":prev_name.id,
                    "previlage":prev_name.previlage
                }
                pr_list.append(data)
            return Response({'role_name':roles_list.role,'previlages':pr_list,'users':names},status=status.HTTP_200_OK)
        else:
            return Response(tok1,status=tok1['status'])
    else:
        return Response({'message':'Method not allowed'},status=status.HTTP_406_NOT_ACCEPTABLE)
    


@api_view(['GET'])
def user_details(request,us_id,token):
    if request.method=='GET':
        role_list=get_previlage_id(previlage=[previlages.view_roles,previlages.view_user])
        tok1 = role_status(token,role_list)
        if tok1['status']==200:
            us_data=models.UserProfile.objects.get(id=us_id)
            userrole=models.UserRole.objects.filter(user_id=us_id).values()
            role_nm=[]
            for rl in userrole:
                rltb=models.Role.objects.get(role_id=rl['role_id'])
                role_nm.append(rltb.role)
            data = {
                    "user_id":us_data.id,
                    "name":us_data.name,
                    "username":us_data.username,
                    "firstname":us_data.first_name,
                    "lastname":us_data.last_name,
                    "email":us_data.email,
                    "is_active":us_data.is_active,
                    "created_by":tok1['username'],
                    "created_at":us_data.created_at.date(),
                    "updated_at":us_data.updated_at.date(),
                    "role":role_nm
                }
            return Response(data,status=status.HTTP_200_OK)
        else:
            return Response(tok1,status=tok1['status'])
    else:
        return Response({'message':'Method not allowed'},status=status.HTTP_406_NOT_ACCEPTABLE)




@api_view(['DELETE'])
def delete_role(request,roleid,token):
    if request.method=='DELETE':
        role_list=get_previlage_id(previlage=[previlages.delete_roles])
        tok1 = role_status(token,role_list)
        if tok1['status']==200:
            if models.Role.objects.filter(role_id=roleid).exists():
                models.UserRole.objects.filter(role_id=roleid).update(role_id=None)
                models.Role.objects.filter(role_id=roleid).delete()
                return Response({'message':'Deleted successfully'},status=status.HTTP_200_OK)
            else:
                return Response({'message':'Role not exists'},status=status.HTTP_404_NOT_FOUND)
        else:
            return Response(tok1,status=tok1['status'])
    else:
        return Response({'message':'Method not allowed'},status=status.HTTP_405_METHOD_NOT_ALLOWED)
    

@api_view(['GET'])
def user_roles_list_vi_dsbrd(request,token):
    if request.method=='GET':
        role_list=get_previlage_id(previlage=[previlages.view_roles,previlages.view_previlages,previlages.view_user])
        tok1 = role_status(token,role_list)
        if tok1['status']==200:
            rolelist=[]
            prevg=models.previlages.objects.get(previlage='can view dashboard')
            roles_list=models.Role.objects.filter(created_by=tok1['user_id'],previlage_id__contains=prevg.id).values('role','role_id').order_by('-updated_at')
            for rl in roles_list:
                rolelist.append({'id':rl['role_id'],'role':rl['role']})
            return Response(rolelist,status=status.HTTP_200_OK)
        else:
            return Response(tok1,status=tok1['status'])
    else:
        return Response({'message':'Method not allowed'},status=status.HTTP_405_METHOD_NOT_ALLOWED)


class roles_list_multiple(CreateAPIView):
    serializer_class=serializers.roles_list_seri

    @transaction.atomic
    def post(self,request,token):
        role_list=get_previlage_id(previlage=[previlages.view_roles,previlages.view_previlages,previlages.view_user])
        tok1 = role_status(token,role_list)
        if tok1['status']==200:
            serializer = self.serializer_class(data = request.data)
            if serializer.is_valid(raise_exception=True):
                role_ids=serializer.validated_data['role_ids']
                final_data=[]
                for rl_id in role_ids:
                    if models.Role.objects.filter(role_id=rl_id,created_by=tok1['user_id']).exists():
                        roles_list=models.Role.objects.get(role_id=rl_id,created_by=tok1['user_id'])
                    else:
                        return Response({'message':'Role{} not created by this user'.format(rl_id)},status=status.HTTP_406_NOT_ACCEPTABLE)
                    users_names=models.UserRole.objects.filter(role_id=roles_list.role_id).values()
                    names=[]
                    for i2 in users_names:
                        ustb=models.UserProfile.objects.get(id=i2['user_id'])
                        names.append({'user_id':ustb.id,'username':ustb.username})
                        final_data.append(names)
                role_list1234=[item for sublist in final_data for item in sublist]
                return Response(role_list1234,status=status.HTTP_200_OK)
            else:
                return Response({'message':'Serializer value error'},status=status.HTTP_400_BAD_REQUEST)
        else:
            return Response(tok1,status=tok1['status'])
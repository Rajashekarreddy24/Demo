from django.db import models
import datetime

# Create your models here.
token_expiry_time =datetime.datetime.now()+datetime.timedelta(minutes=60)

class TokenStoring(models.Model):
    user = models.IntegerField(db_column='user_id',null=True,blank=True)
    parameter = models.CharField(max_length=100,null=True,blank=True)
    qbuserid = models.CharField(max_length=1000,db_column='quickbooks_user_id',null=True,blank=True)
    salesuserid = models.CharField(max_length=1000,db_column='salesforce_user_id',null=True,blank=True)
    tokentype = models.CharField(max_length=100,db_column='token_type',null=True,blank=True)
    accesstoken = models.CharField(max_length=1800,db_column='access_token',null=True,blank=True)
    refreshtoken = models.CharField(max_length=1800,db_column='refresh_token',null=True,blank=True)
    idtoken = models.CharField(max_length=1800,db_column='id_token',null=True,blank=True)
    realm_id = models.CharField(max_length=100,db_column='realm_id',null=True,blank=True)
    display_name = models.CharField(max_length=1000,db_column='display_name',null=True,blank=True)
    created_at = models.DateTimeField(default=datetime.datetime.now())
    updated_at = models.DateTimeField(default=datetime.datetime.now())
    expiry_date = models.DateTimeField(default=token_expiry_time)

    class Meta:
        db_table = 'access_tokens'
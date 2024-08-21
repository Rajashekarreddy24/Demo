from django.urls import path
from dashboard import authentication,Connections,columns_extract,views,files,roles,views
from oauth2_provider.views import AuthorizationView, TokenView
from .views import (DBConnectionAPI,GetTableRelationShipAPI,ListofActiveServerConnections,
                    CustomSQLQuery)
from .Connections import Multicolumndata,Measure_Function,Datasource_filter,Datasource_column_preview

from .Filters import (rdbmsjoins,joining_query_data,Chart_filter,Multicolumndata_with_filter,alias_attachment,DataSource_Data_with_Filter,
                      get_list_filters,get_table_namesAPI,filter_delete,retrieve_datasource_data,tables_delete_with_conditions,delete_conition_from_list,Rename_col_name,delete_database_stmt,
                      sheet_delete_stmt,query_delete_stmt,get_datasource)

from .dashboard_filter_apis import *

urlpatterns = [
    path('oauth2/authorize/', AuthorizationView.as_view(), name='authorize'),
    path('oauth2/token/', TokenView.as_view(), name='token'),

    #### AUTHENTICATION
    path('signup/',authentication.signupView.as_view(),name='signup'),
    path('activation/<token>',authentication.AccountActivateView.as_view(),name='account activation'),
    path('login/',authentication.LoginApiView.as_view(),name='login'),

    #### LICENSE
    path('license/',authentication.license_key_verify.as_view(),name='License_key activation'),
    path('licenseregerate/',authentication.license_reactivation.as_view(),name='License_key Re_Generation'),

    ##### SHEETS
    path('sheetsave/<token>',Connections.sheet_saving.as_view(),name='sheet data storing'), #['POST']
    path('sheetupdate/<int:shid>/<token>',Connections.sheet_update.as_view(),name='sheet data updating'), #['PUT]
    path('sheetretrieve/<int:shid>/<token>',Connections.sheet_retrieve.as_view(),name='sheet data retrieve'),  #['POST']
    path('sheetdelete/<int:server_id>/<int:queryset_id>/<int:sheet_id>/<token>',Connections.sheet_delete,name='sheet data delete'),  # ['DELETE']
    # path('sheetname/<token>',Connections.sheet_name_update.as_view(),name='sheet name update'),
    path('sheetslist/<token>',Connections.charts_fetch.as_view(),name='user list of sheets with sheet data'), #['GET','POST','PUT']
    path('sheetnamelist/<token>',Connections.user_list_names.as_view(),name='user list of sheet names'), #['POST']
    path('sheets_data/<token>',Connections.user_sheets_list_data.as_view(),name='sheets data'),   #['POST']
    path('sheetslistdata/<token>',Connections.sheet_lists_data.as_view(),name='sheets lists data'),  #['POST']
    # path('multiplesheetsselection/<token>',Connections.multiple_sheets.as_view(),name='User multiple charts/sheets selection for dashboard'),

    ##### DASHBOARDS
    path('dashboardsave/<token>',Connections.dahshboard_save.as_view(),name='Dashboard data storing'), #['POST']
    path('dashboardupdate/<int:ds_id>/<token>',Connections.dashboard_update.as_view(),name='Dashboard data updating'), #['PUT']
    path('dashboardretrieve/<token>',Connections.dashboard_retrieve.as_view(),name='Dashboard data retrieve'), #['POST']
    path('dashboarddelete/<int:dashboard_id>/<token>',Connections.dashboard_delete,name='dashboard data delete'),  # ['DELETE']
    # path('dashboardname/<token>',Connections.dashboard_name_update.as_view(),name='Dashboard name update'),
    path('dashboardlist/<token>',Connections.dashboard_fetch.as_view(),name='user list of dashboards with dashboards data'), #['GET','POST','PUT']
    path('savedqueries/<token>',Connections.saved_queries.as_view(),name='user list of saved queries'), #['GET','PUT']
    path('dashboardimage/<token>',Connections.dashboard_image.as_view(),name='save,update dashboard image'), #['POST']
    path('dashboard_prop_update/<token>',Connections.dashboard_property_update.as_view(),name='supdate dashboard property'), #['POST']

    path('columnextracting/<token>',columns_extract.new_column_extraction.as_view(),name='Fetching columns, datatypes from tables'),
    path('show_me/<token>',Connections.show_me.as_view(),name='show me'), # Enabling the charts

    path('querysetname/<token>',views.query_Name_save.as_view(),name='saving queryset name for query'), #### to save the name for custom/joining queries.

    #####  Custom query  
    path('custom_query/<token>',CustomSQLQuery.as_view(),name='Custom Query Report'),  #['POST','PUT']
    path('queryfetch/<token>',views.custom_query_get.as_view(),name='Custom Query fetch'),  #['POST]
    path('querysetdelete/<int:query_set_id>/<token>',views.query_delete,name='deleting queryset'),  #['DELETE]

    ##### Files
    path('upload_file/<token>',files.UploadFileAPI.as_view(),name='Upload Files'),   
    path('get_file/<int:file_id>/<token>',files.files_data_fetch,name='fetch Files'),
    path('delete_file/<int:file_id>/<token>',files.files_delete,name='delete Files'),

    #### Roles & Previlages
    path('previlages_list/<token>',roles.previlages_get.as_view(),name='list of previlages'), #['GET]
    path('role/<token>',roles.add_role.as_view(),name='adding role with previlages'),  #['POST']
    path('roleslist/<token>',roles.list_of_roles.as_view(),name='list of added roles'),  #['PUT','GET]
    path('adduser/<token>',roles.create_user_role.as_view(),name='adding user for the role'), #['POST']
    path('getusersroles/<token>',roles.get_user_role.as_view(),name='get list of users added'), #['PUT']
    path('editroles/<int:rl_id>/<token>',roles.edit_roles.as_view(),name='edit previlages,rolename for roles'), #['PUT']
    path('edituser/<int:us_id>/<token>',roles.edit_users.as_view(),name='edit user'), #['PUT']
    path('deleteuser/<int:userid>/<token>',roles.delete_user,name='delete user'), #['DELETE']
    path('deleterole/<int:roleid>/<token>',roles.delete_role,name='delete role'), #['DELETE']
    path('userdetails/<int:us_id>/<token>',roles.user_details,name='user details'), #['GET']
    path('roledetails/<int:rl_id>/<token>',roles.role_details,name='role details'), #['GET']
    path('multipleroles/<token>',roles.roles_list_multiple.as_view(),name='multiple role details'), #['POST']
    path('dashboardroledetails/<token>',roles.user_roles_list_vi_dsbrd,name='role details(with view dashboard)'), #['GET']

    ##### CALCULATION FIELD
    path('calculationfield/<token>',Connections.calculated_field.as_view(),name='create a calculated field'), # create a calculated field
    path('fetchfunctions/<int:db_id>/<token>',Connections.function_get,name='fetch functions'), # fetching the saved functions

    # Account Re_Activation
    path('re_activation/',authentication.Account_reactivate.as_view(),name='account Re_activation'),

    # Forgot password
    path('reset_password/', authentication.ForgotPasswordView.as_view(), name='Reset Password'),
    path('reset_password/confirm/<token>', authentication.ConfirmPasswordView.as_view(), name='Reset Password Confirm'),

    # Update password
    path('updatepassword/<token>', authentication.UpdatePasswordAPI.as_view(), name='Update Password'),

    # Update Email
    path('updateemail/<token>', authentication.UpdateEMAILAPI.as_view(), name='Email Update'),
    path('emailconformation/<ustoken>/<act_token>', authentication.CustomerEmailUpdateView.as_view(), name='Email Update verification'),

    # Username Update
    path('updateusername/<token>', authentication.user_data_update.as_view(), name='Username Update'),
    
    path('database_connection/<token>',DBConnectionAPI.as_view(),name='Db Connection'),
    
    # path('tables_list/<token>/<database_id>',views.GetTablesOfServerDB,name='List Of Tables From Connected DB'),
    
    path('get_table_relationship/<token>',GetTableRelationShipAPI.as_view(),name='Get Specific Table Relation'),
    
    path('connection_list/<token>',ListofActiveServerConnections.as_view(), name='List of All Active Connections'),

    #### Tables joining
    path('tables_joining/<token>',rdbmsjoins.as_view(),name = 'multiple_table_joins'),

    #### Query Joining data
    path('query_data/<token>',joining_query_data.as_view(),name = 'joining tables data'),
    #### multi column data
    path('multi_col/<token>',Multicolumndata.as_view(),name='mutli column'),
        
    path('chart_filter/<token>',Chart_filter.as_view(),name='chart_filter_data'),

    path('multi_col_dk/<token>',Multicolumndata_with_filter.as_view(),name='mutli column_filter'),

    path('measure_fun/<token>',Measure_Function.as_view(),name='measure function'),

    path('add_alias/<token>',alias_attachment.as_view(),name = 'alias added'),

    path('datasource_filter/<token>',Datasource_filter.as_view(),name='ds filter'),
    
    path('database_disconnect/<token>/<database_id>',views.DBDisconnectAPI,name='Database Disconnection'),

    path('datasource_preview/<token>',Datasource_column_preview.as_view(),name='ds column preview'),
    
    path('server_tables/<token>/<database_id>',views.GetServerTablesList.as_view(),name='Test'),

    path('data_source_data/<token>',DataSource_Data_with_Filter.as_view(),name = 'datasource_querydata'),

    path('list_filters/<token>',get_list_filters.as_view(),name='filters_list'),
 
    path('get_datasource/<token>',get_datasource.as_view(),name = 'get data source'),

    path('get_column_names/<token>',get_table_namesAPI.as_view(),name = 'columns_list'),

    path('filter_delete/<int:filter_no>/<str:type_of_filter>/<token>',filter_delete,name = "filter delete"),

    path('retrieve_datasource/<int:db_id>/<int:queryset_id>/<token>',retrieve_datasource_data.as_view(), name = 'retrieve datasource json'),

    path('tables_delete_with_conditions/<token>',tables_delete_with_conditions.as_view(),name = 'delete_conditions'),

    path('delete_condition/<token>',delete_conition_from_list.as_view(),name ='condition_list'),

    path('rename_column/<token>',Rename_col_name.as_view(),name = 'column rename'),

    path('database_delete_stmt/<token>',delete_database_stmt.as_view(),name='delete_statement'),

    path('sheet_delete_stmt/<token>',sheet_delete_stmt.as_view(),name = 'sheet_delete'),

    path('query_delete_stmt/<token>',query_delete_stmt.as_view(),name= 'query_delete_statement'),

    # path('analyze/<token>',UserInputPreview.as_view(),name='analyze'),

    ################################(dashboard filter api's)###################################################
    # Fetch Column to Filter along with Selected Sheets List
    path('dashboard_column_preview/<token>',DashboardQSColumnAndSheetsPreview.as_view(),name='dashboard column preview'),

    # Add Selected Column and Sheets to Table
    path('dashboard_filter_save/<token>',DashboardFilterSave.as_view(),name='dashboard filter save'),

    # Selected Column Data Preview
    path('dashboard_columndata_preview/<token>',DashboardFilterColumnDataPreview.as_view(),name='Dashboard column Data'),

    # Final Chart Data After Filter
    path('dashboard_filtered_data/<token>',FinalDashboardFilterData.as_view(),name='Final Dashboard Filter'),

    path('dashboard_filter_list/<token>',Dashboard_filters_list.as_view(),name='dashboard filter list'),

    path('dashboard_filter_delete/<token>/<int:filter_id>',DashboardFilterDelete.as_view(),name='dashboard filter delete'),

    path('dashboard_filter_detail/<token>',DashboardFilterDetail.as_view(),name='dashboard filter details')

]


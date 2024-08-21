import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { environment } from '../../../environments/environment';
@Injectable({
  providedIn: 'root'
})
export class WorkbenchService {
  fetchSheetsList(sheet_ids: any) {
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.post<any>(`${environment.apiUrl}/sheets_data/`+this.accessToken,sheet_ids);
  }

  accessToken: any;
  constructor(private http: HttpClient) { }

  postGreSqlConnection(obj:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.post<any>(`${environment.apiUrl}/database_connection/`+this.accessToken,obj);
  }
  postGreSqlConnectionput(obj:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.put<any>(`${environment.apiUrl}/database_connection/`+this.accessToken,obj);
  }
  DbConnection(obj:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.post<any>(`${environment.apiUrl}/database_connection/`+this.accessToken,obj);
  }
  DbConnectionFiles(obj:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.post<any>(`${environment.apiUrl}/upload_file/`+this.accessToken,obj);
  }
  getTablesFromFileId(id:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.get<any>(`${environment.apiUrl}/get_file/`+id+'/'+this.accessToken);
  }
  // getTableData(obj:any){
  //   const currentUser = localStorage.getItem( 'currentUser' );
  //   console.log(JSON.parse( currentUser!))
  //   this.accessToken = JSON.parse( currentUser! )['Token'];
  //   return this.http.post<any>(`${environment.apiUrl}/get_details/`,obj);
  // }

  tableRelation(obj:any){
    return this.http.post<any>(`${environment.apiUrl}/get_table_relationship/`+this.accessToken,obj);
  }
  getTableData(obj:any){
    return this.http.put<any>(`${environment.apiUrl}/get_table_relationship/`+this.accessToken,obj)
  }
  getdatabaseConnectionsList(obj:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.put<any>(`${environment.apiUrl}/connection_list/`+this.accessToken,obj)
  }
  getTablesFromConnectedDb(id:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.get<any>(`${environment.apiUrl}/tables_list/`+this.accessToken+'/'+id)
  }
  getTablesfromPrevious(dbId:any,querysetId:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.get<any>(`${environment.apiUrl}/retrieve_datasource/`+dbId+'/'+querysetId+'/'+this.accessToken)
  }
  getSavedQueryData(obj:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.post<any>(`${environment.apiUrl}/queryfetch/`+this.accessToken,obj)
  }
  getSchemaTablesFromConnectedDb(id:any,obj:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.post<any>(`${environment.apiUrl}/server_tables/`+this.accessToken+'/'+id,obj)
  }
  getColumnsData(obj:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.post<any>(`${environment.apiUrl}/columnextracting/`+this.accessToken,obj);
  }
  executeQuery(obj:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.post<any>(`${environment.apiUrl}/custom_query/`+this.accessToken,obj);
  }
  saveQueryName(obj:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.put<any>(`${environment.apiUrl}/querysetname/`+this.accessToken,obj);
  }
  updateCustmQuery(obj:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.put<any>(`${environment.apiUrl}/custom_query/`+this.accessToken,obj);
  }
  joiningTables(obj:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.post<any>(`${environment.apiUrl}/tables_joining/`+this.accessToken,obj);
  }
  getTableJoiningData(obj:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.post<any>(`${environment.apiUrl}/query_data/`+this.accessToken,obj);
  }
  getDataExtraction(obj:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.post<any>(`${environment.apiUrl}/multi_col_dk/`+this.accessToken,obj);
  }
  getChartsEnableDisable(obj:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.post<any>(`${environment.apiUrl}/show_me/`+this.accessToken,obj);
  }
  deleteDbConnection(id:any){
    return this.http.delete<any>(`${environment.apiUrl}/database_disconnect/`+this.accessToken+'/'+id);
  }
  deleteDbMsg(obj:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.post<any>(`${environment.apiUrl}/database_delete_stmt/`+this.accessToken,obj);
  }
  sheetSave(obj:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.post<any>(`${environment.apiUrl}/sheetsave/`+this.accessToken,obj);
  }
  sheetUpdate(obj:any,id:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.post<any>(`${environment.apiUrl}/sheetupdate/`+id+"/"+this.accessToken,obj);
  }
  // sheetDelete(serverId:any,querysetId:any,sheetId:any){
  //   const currentUser = localStorage.getItem( 'currentUser' );
  //   this.accessToken = JSON.parse( currentUser! )['Token'];
  //   return this.http.delete<any>(`${environment.apiUrl}/sheetdelete/`+serverId+"/"+querysetId+"/"+sheetId+"/"+this.accessToken);
  // }
  sheetGet(obj:any,sheet_id:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.post<any>(`${environment.apiUrl}/sheetretrieve/`+sheet_id+'/'+this.accessToken,obj);
  }
  getSheetData(){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.get<any>(`${environment.apiUrl}/sheetslist/`+this.accessToken);
  }
  sheetsDataWithQuerysetId(obj:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.post<any>(`${environment.apiUrl}/sheetslist/`+this.accessToken,obj);
  }

  sheetRetrivelBasedOnIds(obj : any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.post<any>(`${environment.apiUrl}/sheetslistdata/`+this.accessToken,obj);
  }
  
  saveDashboard(obj:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.post<any>(`${environment.apiUrl}/dashboardsave/`+this.accessToken,obj);
  }
  updateDashboard(obj:any,dashboardId:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.post<any>(`${environment.apiUrl}/dashboardupdate/`+dashboardId+'/'+this.accessToken,obj);
  }
  saveDAshboardimage(obj:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.post<any>(`${environment.apiUrl}/dashboardimage/`+this.accessToken,obj);
  }
  getSavedDashboardData(obj:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.post<any>(`${environment.apiUrl}/dashboardretrieve/`+this.accessToken,obj);
  }
  filterPost(obj:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.post<any>(`${environment.apiUrl}/chart_filter/`+this.accessToken,obj);
  }
  filterPut(obj:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.put<any>(`${environment.apiUrl}/chart_filter/`+this.accessToken,obj)
  }
  updateQuerySetTitle(obj:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.put<any>(`${environment.apiUrl}/querysetname/`+this.accessToken,obj)
  }
  filterEditPost(obj:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.post<any>(`${environment.apiUrl}/get_datasource/`+this.accessToken,obj);
  }
  filterDelete(filterId:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.delete<any>(`${environment.apiUrl}/filter_delete/`+filterId+"/sheet/"+this.accessToken);
  }
  selectedColumnGetRows(obj:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.post<any>(`${environment.apiUrl}/chart_filter/`+this.accessToken,obj);
  }
  getSelectedRowsFilter(obj:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.put<any>(`${environment.apiUrl}/chart_filter/`+this.accessToken,obj);
  }
  getDsQuerysetId(obj:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.post<any>(`${environment.apiUrl}/data_source_data/`+this.accessToken,obj);
  }
  getUserSheetList(){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.get<any>(`${environment.apiUrl}/sheetslist/`+this.accessToken);
  }
  getUserSheetListPut(obj:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.put<any>(`${environment.apiUrl}/sheetslist/`+this.accessToken,obj);
  }
  deleteSheet(serverId:any,qurysetId:any,sheetId:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.delete<any>(`${environment.apiUrl}/sheetdelete/`+serverId+'/'+qurysetId+'/'+sheetId+'/'+this.accessToken);
  }
  deleteSheetMessage(obj:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.post<any>(`${environment.apiUrl}/sheet_delete_stmt/`+this.accessToken,obj);
  }
  deleteSavedQuery(qryId:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.delete<any>(`${environment.apiUrl}/querysetdelete/`+qryId+'/'+this.accessToken);
  }
  deleteSavedQueryMessage(obj:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.post<any>(`${environment.apiUrl}/query_delete_stmt/`+this.accessToken,obj);
  }
  getuserDashboardsList(){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.get<any>(`${environment.apiUrl}/dashboardlist/`+this.accessToken);
  }
  getuserDashboardsListput(obj:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.put<any>(`${environment.apiUrl}/dashboardlist/`+this.accessToken,obj);
  }
  deleteDashboard(serverId:any,querysetId:any,dashboardId:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.delete<any>(`${environment.apiUrl}/dashboarddelete/`+serverId+'/'+querysetId+'/'+dashboardId+'/'+this.accessToken);
  }
  getFilteredList(obj:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.post<any>(`${environment.apiUrl}/list_filters/`+this.accessToken,obj);
  }
  editFilter(obj:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.post<any>(`${environment.apiUrl}/get_datasource/`+this.accessToken,obj);
  }
  deleteFilter(filterId:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.delete<any>(`${environment.apiUrl}/filter_delete/`+filterId+'/'+'datasource'+'/'+this.accessToken);
  }
  callColumnWithTable(obj:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.post<any>(`${environment.apiUrl}/get_column_names/`+this.accessToken,obj);
  }
  deleteRelation(obj:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.post<any>(`${environment.apiUrl}/delete_condition/`+this.accessToken,obj);
  }
  getSavedQueryList(obj:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.put<any>(`${environment.apiUrl}/savedqueries/`+this.accessToken,obj);
  }
  //jhansi
  getSheetNames(obj:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.post<any>(`${environment.apiUrl}/sheetnamelist/`+this.accessToken,obj);
  }
  renameColumn(obj:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.post<any>(`${environment.apiUrl}/rename_column/`+this.accessToken,obj);
  }

//Dashboard Filters
getColumnsInDashboardFilter(obj:any){
  const currentUser = localStorage.getItem( 'currentUser' );
  this.accessToken = JSON.parse( currentUser! )['Token'];
  return this.http.post<any>(`${environment.apiUrl}/dashboard_column_preview/`+this.accessToken,obj);  
}
selectedDatafromFilter(obj:any){
  const currentUser = localStorage.getItem( 'currentUser' );
  this.accessToken = JSON.parse( currentUser! )['Token'];
  return this.http.post<any>(`${environment.apiUrl}/dashboard_filter_save/`+this.accessToken,obj);  
}
getColDataFromFilterId(obj:any){
  const currentUser = localStorage.getItem( 'currentUser' );
  this.accessToken = JSON.parse( currentUser! )['Token'];
  return this.http.post<any>(`${environment.apiUrl}/dashboard_columndata_preview/`+this.accessToken,obj); 
}
getDashboardFilterredList(obj:any){
  const currentUser = localStorage.getItem( 'currentUser' );
  this.accessToken = JSON.parse( currentUser! )['Token'];
  return this.http.post<any>(`${environment.apiUrl}/dashboard_filter_list/`+this.accessToken,obj); 
}
getFilteredData(obj:any){
  const currentUser = localStorage.getItem( 'currentUser' );
  this.accessToken = JSON.parse( currentUser! )['Token'];
  return this.http.post<any>(`${environment.apiUrl}/dashboard_filtered_data/`+this.accessToken,obj); 
}

getServerTablesList(obj:any){
  return this.http.post<any>(`${environment.apiUrl}/ai/copilot/`,obj);
}
deleteDashbaordFilter(obj:any){
  const currentUser = localStorage.getItem( 'currentUser' );
  this.accessToken = JSON.parse( currentUser! )['Token'];
  return this.http.delete<any>(`${environment.apiUrl}/dashboard_filtered_save/`+this.accessToken); 
}

//roles
getSavedRolesList(obj:any){
  const currentUser = localStorage.getItem( 'currentUser' );
  this.accessToken = JSON.parse( currentUser! )['Token'];
  return this.http.put<any>(`${environment.apiUrl}/roleslist/`+this.accessToken,obj); 
}
getPrevilagesList(obj:any){
  const currentUser = localStorage.getItem( 'currentUser' );
  this.accessToken = JSON.parse( currentUser! )['Token'];
  return this.http.put<any>(`${environment.apiUrl}/previlages_list/`+this.accessToken,obj); 
}
addPrevilage(obj:any){
  const currentUser = localStorage.getItem( 'currentUser' );
  this.accessToken = JSON.parse( currentUser! )['Token'];
  return this.http.post<any>(`${environment.apiUrl}/role/`+this.accessToken,obj); 
}
deleteRole(id:any){
  const currentUser = localStorage.getItem( 'currentUser' );
  this.accessToken = JSON.parse( currentUser! )['Token'];
  return this.http.delete<any>(`${environment.apiUrl}/deleterole/`+id+'/'+this.accessToken); 
}
getRoleIdDetails(id:any){
  const currentUser = localStorage.getItem( 'currentUser' );
  this.accessToken = JSON.parse( currentUser! )['Token'];
  return this.http.get<any>(`${environment.apiUrl}/roledetails/`+id+'/'+this.accessToken); 
}
editRoleDetails(id:any,obj:any){
  const currentUser = localStorage.getItem( 'currentUser' );
  this.accessToken = JSON.parse( currentUser! )['Token'];
  return this.http.put<any>(`${environment.apiUrl}/editroles/`+id+'/'+this.accessToken,obj); 
}
//users
getUserList(obj:any){
  const currentUser = localStorage.getItem( 'currentUser' );
  this.accessToken = JSON.parse( currentUser! )['Token'];
  return this.http.put<any>(`${environment.apiUrl}/getusersroles/`+this.accessToken,obj); 
}
getAddedRolesList(){
  const currentUser = localStorage.getItem( 'currentUser' );
  this.accessToken = JSON.parse( currentUser! )['Token'];
  return this.http.get<any>(`${environment.apiUrl}/roleslist/`+this.accessToken); 
}
addUserwithRoles(obj:any){
  const currentUser = localStorage.getItem( 'currentUser' );
  this.accessToken = JSON.parse( currentUser! )['Token'];
  return this.http.post<any>(`${environment.apiUrl}/adduser/`+this.accessToken,obj); 
}
deleteUser(id:any){
  const currentUser = localStorage.getItem( 'currentUser' );
  this.accessToken = JSON.parse( currentUser! )['Token'];
  return this.http.delete<any>(`${environment.apiUrl}/deleteuser/`+id+'/'+this.accessToken); 
}

  getUserIdDetails(id: any) {
    const currentUser = localStorage.getItem('currentUser');
    this.accessToken = JSON.parse(currentUser!)['Token'];
    return this.http.get<any>(`${environment.apiUrl}/userdetails/` + id + '/' + this.accessToken);
  }
  editUser(id: any, obj: any) {
    const currentUser = localStorage.getItem('currentUser');
    this.accessToken = JSON.parse(currentUser!)['Token'];
    return this.http.put<any>(`${environment.apiUrl}/edituser/` + id + '/' + this.accessToken, obj);
  }

  //dashboard properties
  getRoleDetailsDshboard(){
    const currentUser = localStorage.getItem('currentUser');
    this.accessToken = JSON.parse(currentUser!)['Token'];
    return this.http.get<any>(`${environment.apiUrl}/dashboardroledetails/`+ this.accessToken);
  }
  getUsersOnRole(obj:any){
    const currentUser = localStorage.getItem( 'currentUser' );
  this.accessToken = JSON.parse( currentUser! )['Token'];
  return this.http.post<any>(`${environment.apiUrl}/multipleroles/`+this.accessToken,obj); 
  }
  saveDashboardProperties(obj:any){
    const currentUser = localStorage.getItem( 'currentUser' );
    this.accessToken = JSON.parse( currentUser! )['Token'];
    return this.http.post<any>(`${environment.apiUrl}/dashboard_prop_update/`+this.accessToken,obj); 
  }
  
  //image convert
      blobToFile(theBlob:any){
      theBlob.lastModifiedDate = new Date();
      theBlob.name = theBlob.lastModifiedDate.getTime()+'.jpeg';
      return theBlob;
      
     }
    // convertBase64ToFileObject(dataURI) {
    //   const byteString = atob(dataURI.split(',')[1]);
    //   const mimeString = dataURI.split(',')[0].split(':')[1].split(';')[0];
    //   const ab = new ArrayBuffer(byteString.length);
    //   const ia = new Uint8Array(ab);
    //   for (let i = 0; i < byteString.length; i++) {  ia[i] = byteString.charCodeAt(i);  }
    //   const blob = new Blob([ab], {type: mimeString});
    //   return (<File> blob);
    // }

    base64ToBlob(dataUrl: string): Blob {
      const byteString = atob(dataUrl.split(',')[1]);
      const mimeString = dataUrl.split(',')[0].split(':')[1].split(';')[0];
      const ab = new ArrayBuffer(byteString.length);
      const ia = new Uint8Array(ab);
    //   for (let i = 0; i < byteString.length; i++) {
    //     ia[i] = byteString.charCodeAt(i);
    //   }
    //   return new Blob([ab], { type: mimeString });
    // }
    for (let i = 0; i < byteString.length; i++) {  ia[i] = byteString.charCodeAt(i);  }
      const blob = new Blob([ab], {type: mimeString});
      return (<File> blob);
    }




}

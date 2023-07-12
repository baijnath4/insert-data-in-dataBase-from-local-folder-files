import pandas as pd
import paramiko
import os
from io import BytesIO
import datetime
import ibm_db_dbi
import ibm_db
import sqlalchemy as db
import ibm_db_sa
from sqlalchemy import create_engine
from sqlalchemy.engine import url

# Databse Connection
driver = "db2+ibm_db"
username="avnet_wco_india_team"
password="Zr]rmayiqvzNyogb4qnkwng4d"
host="db2w-kfleqqf.eu-de.db2w.cloud.ibm.com"
port=50001
database="BLUDB"
SECURITY = "ssl"

# sqlalchemy connection string
conn_string = f'{driver}://{username}:{password}@{host}:{port}/{database}?SECURITY={SECURITY}'
engine = db.create_engine(conn_string)
conn = engine.connect()


# SFTP Connection
host = 'sftp-gw.gpsemea.ihost.com'
username = 'ibmkrkana001'
password = 'aqswde1234'   # win scpt
keyfilepath = 'C:/Users/01934L744/Box/Baijnath Data/Project 2023/Manish/AVNET SFTP to DB/key/ibmkrkana001pemFile.pem'
rmtPth = '/ANALYTICS/WCO/'
locPth = "C:\\Users\\01934L744\\Box\\AVNET MVP Project\\WinSCP SFTP files\\AP Data\\"
rmtPthSFTP = '/ANALYTICS/WCO/AP_Decrypted_latest/'

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(host, username=username, password=password, key_filename=keyfilepath)
connection = ssh.open_sftp()

# AP - Insert data in DB

# Table name available in database
newCol_AP_PARKED_2022 = ['COMPANY_CODE','VENDOR_NUMBER','MFG_NAME','ERROR_CODE','ERROR_DESC','PAYMENT_TERMS_DESC','DOCUMENT_ID','INVOICE_DATE','ENTRY_DATE','INVOICE_NUMBER','INVOICE_AMT','INVOICE_CURR','PO_NUM','HEADER_TEXT']
newCol_AP_OPEN_NEW_2022 = ['SUPPLIER_NUMBER','SUPPLIER_NAME','COMPANY_CODE_OR_LEGAL_ENTITY','FISCAL_YEAR','DOCUMENT_TYPE','DOCUMENT_DATE','POSTING_KEY','POSTING_DATE_OR_ENTRY_DATE','DOCUMENT_NUMBER','SUPPLIER_INVOICE_NUMBER','DOCUMENT_AMOUNT_GROSS_IN_LOCAL_CURRENCY','LOCAL_CURRENCY','DOCUMENT_AMOUNT_GROSS_IN_DOCUMENT_CURRENCY','DOCUMENT_CURRENCY','CLEARING_DOCUMENT','CLEARING_DATE','TEXT_OR_COMMENTS','NETDUE_DATE','PURCHASE_ORDER_NUMBER','ENTRY_DATE','PAYMENTTERMS','BASELINEDT','CDTDUEDT','CDT2DUEDT','AMTIN2NDLOCALCURR','CURR']
newCol_AP_CLOSE_NEW_2022 = ['SUPPLIER_NUMBER','SUPPLIER_NAME','COMPANY_CODE_OR_LEGAL_ENTITY','FISCAL_YEAR','DOCUMENT_TYPE','DOCUMENT_DATE','POSTING_KEY','POSTING_DATE_OR_ENTRY_DATE','DOCUMENT_NUMBER','SUPPLIER_INVOICE_NUMBER','DOCUMENT_AMOUNT_GROSS_IN_LOCAL_CURRENCY','LOCAL_CURRENCY','DOCUMENT_AMOUNT_GROSS_IN_DOCUMENT_CURRENCY','DOCUMENT_CURRENCY','CLEARING_DOCUMENT','CLEARING_DATE','TEXT_OR_COMMENTS','NETDUE_DATE','PURCHASE_ORDER_NUMBER','ENTRY_DATE','PAYMENTTERMS','BASELINEDT','CDTDUEDT','CDT2DUEDT','AMTIN2NDLOCALCURR','CURR']
newCol_AP_VENDOR_MASTER_NEW_2022 = ['SAP_COMPANY_CD','SAP_VENDOR_NO','NAME1','NAME2','NAME3','CENTRAL_POSTING_BLK','CITY','DISTRICT','CITY_POSTAL_CD','COUNTRY_CD','REGION','SEARCH_TERM','TAX_NO','BUSINESS_TYPE','VENDOR_TERMS','PAYMENT_METHOD','DISCOUNT_VENDOR','FILL1','ACCOUNT_GROUP','GROUPING_KEY']

# Rename column function
def rename_column(df,newCOl):
    dic_df = {}
    for key in df.columns:
        for value in newCOl:
            dic_df[key]=value
            newCOl.remove(value)
            break
    df.rename(columns=dic_df,inplace=True)
    return df

local_APfile_path = "C:/Users/01934L744/Box/AVNET MVP Project/WinSCP SFTP files/AP Data/AP_Decrypted/"

ap_fileList = ['ParkedItems','VendorMasterDaily']
#Dictionary to rename column in text files
ap_renamedCol_dic = {
    'ParkedItems':['COMPANY_CODE','VENDOR_NUMBER','MFG_NAME','ERROR_CODE','ERROR_DESC','PAYMENT_TERMS_DESC','DOCUMENT_ID','INVOICE_DATE','ENTRY_DATE','INVOICE_NUMBER','INVOICE_AMT','INVOICE_CURR','PO_NUM','HEADER_TEXT'],
    'OpenItems':['SUPPLIER_NUMBER','SUPPLIER_NAME','COMPANY_CODE_OR_LEGAL_ENTITY','FISCAL_YEAR','DOCUMENT_TYPE','DOCUMENT_DATE','POSTING_KEY','POSTING_DATE_OR_ENTRY_DATE','DOCUMENT_NUMBER','SUPPLIER_INVOICE_NUMBER','DOCUMENT_AMOUNT_GROSS_IN_LOCAL_CURRENCY','LOCAL_CURRENCY','DOCUMENT_AMOUNT_GROSS_IN_DOCUMENT_CURRENCY','DOCUMENT_CURRENCY','CLEARING_DOCUMENT','CLEARING_DATE','TEXT_OR_COMMENTS','NETDUE_DATE','PURCHASE_ORDER_NUMBER','ENTRY_DATE','PAYMENTTERMS','BASELINEDT','CDTDUEDT','CDT2DUEDT','AMTIN2NDLOCALCURR','CURR'],
    'ClosedItems': ['SUPPLIER_NUMBER','SUPPLIER_NAME','COMPANY_CODE_OR_LEGAL_ENTITY','FISCAL_YEAR','DOCUMENT_TYPE','DOCUMENT_DATE','POSTING_KEY','POSTING_DATE_OR_ENTRY_DATE','DOCUMENT_NUMBER','SUPPLIER_INVOICE_NUMBER','DOCUMENT_AMOUNT_GROSS_IN_LOCAL_CURRENCY','LOCAL_CURRENCY','DOCUMENT_AMOUNT_GROSS_IN_DOCUMENT_CURRENCY','DOCUMENT_CURRENCY','CLEARING_DOCUMENT','CLEARING_DATE','TEXT_OR_COMMENTS','NETDUE_DATE','PURCHASE_ORDER_NUMBER','ENTRY_DATE','PAYMENTTERMS','BASELINEDT','CDTDUEDT','CDT2DUEDT','AMTIN2NDLOCALCURR','CURR'],
    'VendorMasterDaily':['SAP_COMPANY_CD','SAP_VENDOR_NO','NAME1','NAME2','NAME3','CENTRAL_POSTING_BLK','CITY','DISTRICT','CITY_POSTAL_CD','COUNTRY_CD','REGION','SEARCH_TERM','TAX_NO','BUSINESS_TYPE','VENDOR_TERMS','PAYMENT_METHOD','DISCOUNT_VENDOR','FILL1','ACCOUNT_GROUP','GROUPING_KEY']
}

# Database table dictionary
dbTable_Dict = {
    'ParkedItems':'AVNETWCO.AP_PARKED_2022',
    'OpenItems':'AVNETWCO.ap_open_new_2022',
    'ClosedItems':'AVNETWCO.AP_CLOSE_NEW_2022',
    'VendorMasterDaily':'AVNETWCO.AP_VENDOR_MASTER_NEW_2022'
}


for file in os.listdir(local_APfile_path):
    fileNameSplit = file.split('_')[1]
  
    if fileNameSplit in ap_fileList:
        print(fileNameSplit)
        newColumArray = ap_renamedCol_dic[fileNameSplit]
        print('newColumArray',len(newColumArray))
    else:
        continue
    
    #Read text file as dataframe
    filePath = local_APfile_path + file
    try:
        df_DataAP = pd.read_csv(local_APfile_path+file,sep='|',encoding='cp1252',engine='python', error_bad_lines=False)
        print('try block - newColumArray',len(newColumArray))
        df_wihtNewColumn = rename_column(df_DataAP,newColumArray)
#         print('df_wihtNewColumn.columns',df_wihtNewColumn.columns)
#         print('df_wihtNewColumn.columns',df_wihtNewColumn.info())
    
        #Inserting data in database

        df_wihtNewColumn.to_sql(dbTable_Dict[fileNameSplit], conn, schema='AVNETWCO', if_exists='append', index=False)
        print(f"Data inserted in table:-{dbTable_Dict[fileNameSplit]} successfully")

        
    except pd.errors.EmptyDataError:
        print(f"Skipping empty file: {file}")
        continue 
        
    print(".........................................................................")
 
# AR Data Insert In Database
ar_new_col_dict= {
    'CustMstrDaily':['SYSTEM','RECORDTYPE','CUSTOMER','NAME','COUNTRY','ORDERBLOCK','BILLBLOCK','ACCNTGROUP','PAYBLOCK','CUSTOMERSINCE','SALESORG','INVLIST','PAYTERM','SALESBLOCK','SEGMENT','RISKCLASS','RISKCLASSCHANGEON','CUSTCREDITGRP','CREDITLMT','CREDITBLOCKED','CREDITLMTCHANGEDT','CREDITBLKREASON','ADDCREDITLMT','ADDSTARTDATE','ADDENDDATE','ADDADDEDON','VERSION','COLLECTIONGROUP','COLLECTIONSPECIALISTID','COLLECTIONSPECIALISTNAME','CREDITREPRESENTATIVEID','CREDITREPRESENTATIVENAME'],
    'AROpenItemsWeeklyCombined':['SYSTEM','DOCUMENTNO','ITEMNO','FISCALYEAR','COMPCODE','CUSTOMER','CLEARINGDA','CLEARINGDOC','ASSIGNMENT','POSTINGDATE','DOC_DATE','CREATEDON','CURR','REFERENCEDOCNO','DOC_TYPE','FISCALPERIOD','POSTINGKEY','D_CIND','AMOUNT','ITEMTXT','GLACCOUNT','BASELINEDATE','PAY_TERM','DIS_DAY1','DIS_DAY2','NETPAY_TERM','DIS_PER1','DIS_PER2','DIS_BASE','DIS_AMOUNT','PAYMETHOD','INV_REF','INV_REFFISCAL','INV_REFITEM','INV_LIST','DUNNBLOCK','LASTDUNNDATE','DUNNLEVEL','DUNNAREA','REASONCODE','XREF1','XREF2','PAYREF','TCODE','DOCHDR_TXT','EXC_RATE','GRP_CURR','GRP_CURREXCRATE','REFTCODE','REFERENCEKEY','XREF1HD','XREF2HD','XREF3','GROUPCURRAMNT','FILEDATE','COMBINED'],
    'ARDisputesDaily':['SYSTEM','CASEID','CASETYPE','CASEGUID','INVOICENO','CREATEDON','CHANGEDON','CLOSEDON','ESCALREASON','CATEGORY','PRIORITY','PROFILE','STATUS','SYSTEMSTATUS','REASON','ROOTCAUSE','DUEDATE','TOTALDISPUTEAMOUNT','CURRDISPUTEAMOUNT','PAID','CREDITED','CLEARED','WRITTENOFF','CURRENCY','COMPANYCOD','DISPUTEDAYS','STATUSCHANGEDAYS','DISPUTETEXT1','DISPUTETEXT2'],
    'ARClosedItemDaily':['SYSTEM','DOCUMENTNO','ITEMNO','FISCALYEAR','COMPCODE','CUSTOMER','CLEARINGDA','CLEARINGDOC','ASSIGNMENT','POSTINGDATE','DOC_DATE','CREATEDON','CURR','REFERENCEDOCNO','DOC_TYPE','FISCALPERIOD','POSTINGKEY','D_CIND','AMOUNT','ITEMTXT','GLACCOUNT','BASELINEDATE','PAY_TERM','DIS_DAY1','DIS_DAY2','NETPAY_TERM','DIS_PER1','DIS_PER2','DIS_BASE','DIS_AMOUNT','PAYMETHOD','INV_REF','INV_REFFISCAL','INV_REFITEM','INV_LIST','DUNNBLOCK','LASTDUNNDATE','DUNNLEVEL','DUNNAREA','REASONCODE','XREF1','XREF2','PAYREF','TCODE','DOCHDR_TXT','EXC_RATE','GRP_CURR','GRP_CURREXCRATE','REFTCODE','REFERENCEKEY','XREF1HD','XREF2HD','XREF3','GROUPCURRAMNT']
}

ar_dbTable_Dict = {
    'CustMstrDaily':'AVNETWCO.AR_CUSTOMER_MASTER_2022',
    'AROpenItemsWeeklyCombined':'AVNETWCO.AR_OPEN_ITEMS_2022',
    'ARDisputesDaily':'AVNETWCO.AR_DISPUTE_2022',
    'ARClosedItemDaily':'AVNETWCO.AR_CLOSED_ITEMS_2022'
}

ar_files_name = ['AROpenItemsWeeklyCombined', 'CustMstrDaily', 'ARDisputesDaily']
# ar_files_name = ['ARClosedItemDaily', 'AROpenItemsWeeklyCombined', 'CustMstrDaily', 'ARDisputesDaily']
ar_localPath = 'C:/Users/01934L744/Box/AVNET MVP Project/WinSCP SFTP files/AR Data/Ar_Decrypted/'

for file in os.listdir(ar_localPath):
    fileNameSplit = file.split('_')[1]    
    if fileNameSplit in ar_files_name:
        print(fileNameSplit)
    else:
        continue
    
    ar_newColumArray = ar_new_col_dict[fileNameSplit]
    print("ar_newColumArray",len(ar_newColumArray))
        
    filePath = ar_localPath + file
    try:
        df_DataAR = pd.read_csv(ar_localPath+file,sep='|',encoding='cp1252',engine='python')
        print("table Info",df_DataAR.info())
    except pd.errors.EmptyDataError:
        print(f"Skipping empty file: {file}")
        continue 
        
#     Renaming column of dataFrame     
    print(f"length of ar_newColumArray:---{len(ar_newColumArray)}")
    df_wihtNewColumn = rename_column(df_DataAR,ar_newColumArray)
#     print(f"df_wihtNewColumn:---{len(df_wihtNewColumn.columns)}",df_wihtNewColumn.columns)
    
#     Inserting data in database

    df_wihtNewColumn.to_sql(ar_dbTable_Dict[fileNameSplit], conn, schema='AVNETWCO', if_exists='append', index=False)
    print(f"Data inserted in table:-{ar_dbTable_Dict[fileNameSplit]} successfully")


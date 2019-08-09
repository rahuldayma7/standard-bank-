# -*- coding: utf-8 -*-
"""
Created on Wed Jul 31 13:42:27 2019

@author: rahul.dayma
"""

import MySQLdb
import pandas as pd
import math
import pypyodbc
import json
import numpy as np
import datetime as dt
import logging

#logging.basicConfig(filename='sba_logg.log',level=logging.DEBUG ,filemode='a', format=' %(asctime)s - %(levelname)s - %(message)s',datefmt='%d-%b-%y %H:%M:%S')

#creating a log file to keep a track on the status, exceptions and errors of program anytime when running 
logger=logging.getLogger(__name__)
file_handler=logging.FileHandler('standard_bank.log',mode='w')
logger.addHandler(file_handler)
formatter=logging.Formatter(' %(asctime)s:%(name)s:%(levelname)s:%(message)s')
file_handler.setFormatter(formatter)
logger.setLevel(logging.INFO)   

#loading all the insert queries and fact table names in a json file 
with open('config_6August_unit.json') as f:  
    config_data=json.load(f)


#creating connection with ECAL database
def mysql_connection():
    mysql_conn = MySQLdb.connect(host='dpa-aws-db.cvi8bjwlvz3a.us-west-2.rds.amazonaws.com',  
           user='sb_readonly', 
           passwd='dpa@1234',
           db='dpau_test'
           )
    return mysql_conn

#creating connection with standardbank database
def mssql_connection():
    mssql_conn = pypyodbc.connect("Driver={SQL Server Native Client 11.0};"
     "Server=192.168.2.6\MSSQLSERVERDEV;"
     "Database=StandardBank;"
     "uid=sa;pwd=admin@123")
    return mssql_conn

file =pd.read_csv('unit_mapping.csv')


#getting data from ecal database 
def enter_indcode(indcode):
    #indcode='ZA15Y007'
    
    #selecting max_seq_id from sb database for entered indcode and comparing with ecal data max_seq_id to get new updated data
    max_id=pd.read_sql("""SELECT  [ECAL_ID]
          ,[ECAL_INDICATOR]
          ,[DB_TABLE_NAME]
      FROM [StandardBank].[dbo].[ECAL_DB_MAPPING] WHERE ECAL_INDICATOR='{}'""".format(indcode), mssql_connection())
    try:
        if len(max_id)==0:
           # indcode="ZA15Y008"
            data=pd.read_sql("""select transactions.indicator_ID as indicator_ID ,indicators.id ,indicators.INDNAME,transactions.PERIOD,transactions.ACTUAL2,indicators.INDUNIT,indicators.INDCURRENCY, countries.CountryCode,countries.CountryDesc,transactions.id as ECAL_ID from indicators join transactions on indicators.id=transactions.indicator_ID
            join countries on indicators.CountryID=countries.countryid  where transactions.DOWNLOADSTATUS=1 and transactions.D_TAG='N' and transactions.id >'{0}' and indicators.INDCODE='{1}'""".format(0,indcode),mysql_connection())
        else:
            data=pd.read_sql("""select transactions.indicator_ID as indicator_ID ,indicators.id ,indicators.INDNAME,transactions.PERIOD,transactions.ACTUAL2,indicators.INDUNIT,indicators.INDCURRENCY, countries.CountryCode,countries.CountryDesc,transactions.id as ECAL_ID from indicators join transactions on indicators.id=transactions.indicator_ID
        join countries on indicators.CountryID=countries.countryid  where transactions.DOWNLOADSTATUS=1 and transactions.D_TAG='N' and transactions.id >'{0}' and indicators.INDCODE='{1}'""".format(max_id['ecal_id'][0],indcode),mysql_connection())
            
    except Exception as error:
        logging.error(error)
        

#    
    #creating a csv file for unit conversion which contain two entities power and unit
#    for i in range(len(data)):
#        value = file['power'][file['unit'] == data['INDUNIT'][i]]
#        data['ACTUAL2'][i]=data['ACTUAL2'][i]*value
        
    return data


def merged_data(indcode,data_mysql):

    #checking for entered indcode in json file when found matched indcode then it select that fact_table for further mapping with standardbank dimtables and ecal data
    for i,j in enumerate(config_data):
        for q in range(len(config_data[i]['ind_code'])):
            if config_data[i]['ind_code'][q]==indcode:
                data1=config_data[i]

    for k in range(len(data1['dim_tables'])):
        
        #getting required dim tables for current fact_table 
        dim_query=data1['dim_tables'][k]['query'] #getting select queries of all dim tables for current fact_tables 
        dim_table=pd.read_sql(dim_query,mssql_connection()) #reading dim_tables from standardbank database 
        
        #checking for data_value column in fact_table for conversion of datatype for mapping process 
        if 'date_value' in dim_table.columns:  
                dim_table['date_value']=dim_table['date_value'].astype('datetime64[ns]')
                        
        #merging ecal and standardbank data based on left and right columns specified in config file
        data_mysql=pd.merge(data_mysql,dim_table,how='left',left_on=str(data1['dim_tables'][k]['left_col']),right_on=str(data1['dim_tables'][k]['right_col']))

              
    return data_mysql

#ac=merged_data("ZA15Y008",data)

def get_data(indcode,data11):
    
    final_main_data = pd.DataFrame()
 
    

    data = merged_data(indcode,data11)
    final_main_data =final_main_data.append(data)

                                                                  

    return final_main_data 

def final_data(indcode,data11):
    
    a=get_data(indcode,data11)
    
    #dropping all the unwanted columns from the dataframe and getting the final dataframe for iinsertion of data in standardbank database
    for unwanted in ['unit_code','indicator_ID','id','INDNAME','PERIOD','INDUNIT','INDCURRENCY','CountryDesc','currency_code','CountryCode','country_code','date_value','A_DT','DOWNLOADSTATUS']:       
        try:
            a= a.drop(columns=[unwanted])
        except:
            continue
    return a


def insert_data(indcode):

    try:
        
        mssql_conn =mssql_connection()
        cur=mssql_conn.cursor() #creating cursor to execute actions 
        
        #selecting insert queries based on entered indcode 
        x=[]
        for i in range(len(config_data)):
            try:
                for m in range(len(config_data[i]['insert_queries'])):
                    if config_data[i]['insert_queries'][m]['ind_code']==indcode: #comparing entered indcode with indcodes in config file
                        new=config_data[i]['insert_queries'][m]['query'] #getting insert query in new and appending it to x
                        x.append(new)
            except:
                continue
        data11=enter_indcode(indcode)
        
        #converting dataframe to dictionary for insertion process
        listToWrite= final_data(indcode,data11).to_dict(orient='records') 
        #converting data into tuple form to insert in database
        if len(listToWrite)!=0:
            records_to_insert = []
            for i , j in enumerate(listToWrite):
                a = tuple(listToWrite[i].values())
                records_to_insert.append(a)
            new_tuples = [tuple(None if isinstance(i, float) and math.isnan(i) else i for i in t) for t in records_to_insert]
            cur.executemany(x[0],new_tuples)


            for conf in config_data:

                
                for key, value in conf.items():
                    #print (key, value)
                    
                    #checking in ecal_db_mappng table in standatdbank database for max_seq_id for entered indcode if indcode is present 
                    #then it updates the max_sq_id if any data is updated in ecal databse if indcode is not present then it insert 
                    #the max_seq_id along with that indcode and fact_table name
                    if key == 'ind_code' and indcode in value:
                        for ins in range(len(data11)):
                            
                            insert_or_update_condtion=pd.read_sql("""select *  from [dbo].[ECAL_DB_MAPPING] where ECAL_INDICATOR ='{}'""".format(indcode),mssql_conn)

                        
                        #if the below condition is true then it updates the ecal_mapping table with max_seq else it insert the data for entered indcode
                        if len(insert_or_update_condtion)==0: #data for that indcode is not present in ecal_db_mapping 
                            insert_val= data11['ECAL_ID'].max() #taking max_seq_id form ecal data
                            insert_query = " insert into [StandardBank].[dbo].[ECAL_DB_MAPPING] (ECAL_ID, ECAL_INDICATOR,DB_TABLE_NAME) values ({0},'{1}','{2}')".format(int(insert_val),str(indcode),conf['fact_table_name'])
#                            mssql_conn.cursor().execute(insert_quert)
                            cur.execute(insert_query)
                            cur.commit()
                            logger.info('{0} - records inserted for indcode - {1}'.format(len(listToWrite),indcode))
                            print('{0} - records inserted for indcode - {1}'.format(len(listToWrite),indcode))
                        else:
                            #if data for entered indcode is present in ecal_db_mapping table in standardbank database 
                            update_val= data11['ECAL_ID'].max()
                            update_query="update  [StandardBank].[dbo].[ECAL_DB_MAPPING] set ECAL_ID ={} ".format( update_val) +"where ECAL_INDICATOR = '{}'".format(indcode)

                            cur.execute(update_query)
                            cur.commit()
                            logger.info('{0} - records inserted for indcode - {1}'.format(len(listToWrite),indcode))
                            print(('{0} - records inserted for indcode - {1}'.format(len(listToWrite),indcode)))
                            
        else:
            logger.warning('{0} - records inserted for indcode - {1}'.format(len(listToWrite),indcode))
            print(('{0} - records inserted for indcode - {1}'.format(len(listToWrite),indcode)))
                

                            

    except Exception as error:
        print(error)
        logger.info(error)
    finally:
        #close all the connections and cursors 
        mysql_connection().close()
        mssql_connection().close()
        mssql_conn.close()
        cur.close()
    



def main():
    #indcodes=input('enter indcodes:')

    #indcodes=["AO01Y026","GH01Y031","GH01Q005","GH01Q001","GH01Q002","GH01Q003","GH01Q004","GH01Q010","GH03Y003","GH03Y005","GH03Y007","GH03Y009","GH03Y011","GH03Y013","GH03Y006","GH03Y008","GH03Y014","GH01Q013","GH01Q006","GH01Q024","GH01Q007","GH01Q014","GH03M012","GH03M026","GH03M027","GH07Y008","GH08Y009","GH03Y049","GH03Y045","GH03Y046","GH03Y048","GH08M004","GH03Y056","GH03Y149","GH03Y148","GH03Y057","GH08Y035","GH03Y001","GH03Y002","GH03Y004","GH03Y010","GH03Y012","GH03Y083","GH03Y084","GH03Y085","GH03Y016","GH03Y019","GH03Y017","GH03Y020","GH03Y022","GH03Y025","GH03Y023","GH03Y026","GH03Y030","GH03Y144","GH03Y145","GH03Y146","GH03Y147","GH08M005","GH08M006","GH08M007","GH08M008","GH03Y106","GH08Y037","GH08Y038","GH08Y039","GH08Y011","GH08Y013","GH03Y108","GH03Y109","GH03Y112","GH03Y110","GH03Y113","GH03Y111","GH08Y031","GH03Y123","GH03Y015","GH15Y013","GH15Y033","GH15Y016","GH15Y032","GH15Y030","GH15Y034","GH15Y035","GH03M014","GH03Y127","GH03Y124","GH03Y125","GH03Y126","GH08Y033","GH08Y032","GH03Y135","GH03Y140","GH03Y136","GH03Y137","GH03Y138","GH03Y139","AU03Y015","AU03Y016","BR03Y015","BR03Y016","CA03Y015","CA03Y016","CL03Y015","CL03Y016","CN03Y016","CN03Y017","GH03Y167","GH03Y166","ID03Y015","ID03Y016","KZ03Y001","KZ03Y002","MX03Y015","MX03Y016","PG03Y001","PG03Y002","PE03Y001","PE03Y002","RU03Y015","RU03Y016","ZA03Y033","ZA03Y034","UZ03Y001","UZ03Y002","ZA03M043","ZA03M044","ZA03M045","ZA03M046","ZA03M047","ZA03M048","ZA03M049","ZA03M050","ZA03Y002","ZA03Y003","ZA03Y004","ZA03Y005","ZA03Y006","ZA03Y007","ZA03Y008","ZA03Y009","ZA03Y010","ZA03Y011","ZA03Y012","ZA03Y013","ZA03Y014","ZA03Y015","ZA03Y016","ZA03Y017","ZA03M034","ZA03M018","ZA03M019","ZA03M020","ZA03M021","ZA03M022","ZA03M023","ZA03M024","ZA03M025","ZA03M026","ZA08M004","ZA08M005","ZA05Y002","ZA03M035","ZA03M036","ZA03M037","ZA03M038","ZA03M039","ZA03M040","ZA03M041","ZA03M042","ZA03M027","ZA03M028","ZA03M029","ZA03M030","ZA15Y007","ZA15Y008","ZA15Y009","ZA05Q003","ZA05Q004","ZA05Q005","ZA05Q006","ZA05Q001","ZA05Q002","ZA03Y001","ZA03Q008","ZA03Q009","ZA03Q010","ZA03Q011","ZA03M070","ZA03M033","ZA03M032","ZA08M003","ZA03Q003","ZA03Q004","ZA03Q005","ZA03Q006","ZA03Q007","MW01Y013","MW01Y014","MW01Y015","MW01Y016","MR01Y013","MR01Y014","MR01Y015","MR01Y016","MU01Y013","MU01Y014","MU01Y015","MU01Y016","MA01Y013","MA01Y014","MA01Y015","MA01Y016","MZ01Y013","MZ01Y014","MZ01Y015","MZ01Y016","NA01Y019","NA01Y020","NA01Y021","NA01Y022","NE01Y013","NE01Y014","NE01Y015","NE01Y016","NG01Y019","NG01Y020","NG01Y021","NG01Y022","RW01Y013","RW01Y014","RW01Y015","RW01Y016","SN01Y013","SN01Y014","SN01Y015","SN01Y016","SL01Y013","SL01Y014","SL01Y015","SL01Y016","SD01Y013","SD01Y014","SD01Y015","SD01Y016","SZ01Y013","SZ01Y014","SZ01Y015","SZ01Y016","TZ01Y013","TZ01Y014","TZ01Y015","TZ01Y016","TG01Y013","TG01Y014","TG01Y015","TG01Y016","TN01Y013","TN01Y014","TN01Y015","TN01Y016","UG01Y013","UG01Y014","UG01Y015","UG01Y016","ZM01Y018","ZM01Y019","ZM01Y020","ZM01Y021"]
    indcodes=["ZA15Y009","ZA15Y008"]
    for indcode in indcodes:
        insert_data(indcode)
       # main_code_for_update(indcode)
        
        
main()
#insert_data("ZA15Y008")

if __name__=="__main__":
    main()



#ecal=final_data("ZA15Y007")
#
#
###    
#
#
#d    data=pd.read_sql("""select transactions.indicator_ID,transactions.A_DT,transactions.DOWNLOADSTATUS,indicators.id ,indicators.INDNAME,transactions.PERIOD,transactions.ACTUAL2,indicators.INDUNIT,indicators.INDCURRENCY, countries.CountryCode,countries.CountryDesc from indicators join transactions on indicators.id=transactions.indicator_ID
# f update_table_value():
#emain()         join countries on indicators.CountryID=countries.countryid  where transactions.DOWNLOADSTATUS=1 and indicators.INDCODE='{}'""".format(x),mysql_connection())

#
#  
#
#
#
#
#                           
 ##indcodes=["MW01Y013","MR01Y013","MU01Y013","MA01Y013","MZ01Y013","NA01Y019","NE01Y013","NG01Y019","RW01Y013","SN01Y013","SL01Y013","SD01Y013","SZ01Y013","TZ01Y013","TG01Y013","TN01Y013","UG01Y013","ZM01Y018"]
                                
#                         
#
#




                      


#def lastupdate_date():
#    x22=[]
#    #date_df=pd.DataFrame()
#    for indcode in indcodes:
#        data2=get_data(indcode)
#        data2['A_DT']=data2['A_DT'].dt.date
#        a=data2['A_DT'].max()
#        z=[indcode,a]
#        x22.append(z)
#    df_new=pd.DataFrame(x22)
#    df_new=df_new.rename(columns={0:'indcode',1:'last__update_date'})
#    
#    df_new.to_csv('final_date.csv')
#    return df_new


#gag=last_date() 

#for i,j in enumerate(indcode):
#    if indcode['last_updated_date'][i]!=data['A_DT'].max():
#        main()
#        print("database updated")
#    else :
##        print("data is up-to-date")
###    return date_df
##
#data_mp=pd.read_excel('E:/code/FACT to DIM mapping.xlsx')
##
##for i in indcode1:
##    sad(i)
#acg=final_data("ZA15Y007")
#
#acg.loc[-1]=[9654212,3,2,20171232]
#acg.index = acg.index + 1  # shifting index
#acg = acg.sort_index()
#
#
#data_f=pd.read_sql("""SELECT  [COUNTRY_ID]
#      ,[DATE_ID]
#      ,[CURRENCY_ID]
#      ,[INSURANCE_WRITTEN_PREMIUM_AMOUNT] 
#      FROM [StandardBank].[dbo].[INSURANCE_WRITTEN_PREMIUM_FACT]""",mssql_connection())


#for i in acg:
#    data_m=acg.loc[acg['ACTUAL2'][i]!=data_f['insurance written_premium_amount'][i]]
#daat=acg[~acg['ACTUAL2'].isin(data_f['insurance written_premium_amount)']]

#df_all =pd.merge(acg,data_f, left_on=acg['ACTUAL2'],right_on=data_f['insurance_written_premium_amount'], 
#                   how='left', indicator=True)

#acg=acg.rename(columns={'ACTUAL2':'insurance_written_premium_amount'})
#final=data_f.append(acg)
#
#a=final.drop_duplicates() 





x='ZA15Y007'
max_id=pd.read_sql("""SELECT TOP (1000) [ECAL_ID]
      ,[ECAL_INDICATOR]
      ,[DB_TABLE_NAME]
  FROM [StandardBank].[dbo].[ECAL_DB_MAPPING] WHERE ECAL_INDICATOR='{}'""".format(x), mssql_connection())


data_new=pd.read_sql("""select transactions.indicator_ID as indicator_ID ,indicators.id ,indicators.INDNAME,transactions.PERIOD,transactions.ACTUAL2,indicators.INDUNIT,indicators.INDCURRENCY, countries.CountryCode,countries.CountryDesc,transactions.id as ECAL_ID from indicators join transactions on indicators.id=transactions.indicator_ID
join countries on indicators.CountryID=countries.countryid  where transactions.id >'{0}' and indicators.INDCODE='{1}'""".format(max_id['ecal_id'][0],x),mysql_connection())












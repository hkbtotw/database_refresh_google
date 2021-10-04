### use with quandl environment on sandbox 3
from datetime import datetime, date,  timedelta
import pyodbc as db
import pandas as pd
from Operations import ReadSheet,catDict
from dateutil.relativedelta import relativedelta
import os
from Credential import *

start_datetime = datetime.now()
print (start_datetime,'execute')
todayStr=date.today().strftime('%Y-%m-%d')
nowStr=datetime.today().strftime('%Y-%m-%d %H:%M:%S')
previousmonthStr=(date.today()-relativedelta(months=1)).strftime('%Y%m')
print("TodayStr's date:", todayStr,' -- ',type(todayStr))
print("nowStr's date:", nowStr,' -- ',type(nowStr))


#####################
def Read_Tracker_Table_Update():
    print('------------- Start ReadDB : DIM_MAPPING_BRAND -------------')    
    conn = connect_tad

    cursor = conn.cursor()

        #- Select data  all records from the table
    sql="""
        SELECT  [rowid]
                    ,[TableName]
                    ,[TotalRows]
                    ,[Max_YYYYMM]
                    ,[TableLocation]
                    ,[UpdateDateTime]
                FROM [TSR_ADHOC].[dbo].[Tracker_Table_Update]
    """
    dfout=pd.read_sql(sql,conn)
    
    #print(len(dfout.columns),' :: ',dfout.columns)
    #print(dfout)    
    del conn, cursor, sql
    print(' --------- Reading End -------------')
    return dfout

def Convert_String_DateTime(x, formatString):
    try:
        return datetime.strptime(x,formatString).date()
    except:
        return datetime.strptime(x,'%Y-%m-%d').date()

####################
file_path=os.getcwd()

## input
input_file='database_test.xlsx'

########################

# Declare class
readSheet=ReadSheet()

# Declare function
sheetList=readSheet.Authorization_DB_Refresh()
print(' --> ',sheetList)


dfIn=Read_Tracker_Table_Update()
dfIn['Date']=dfIn.apply(lambda x: Convert_String_DateTime(x['UpdateDateTime'], '%Y-%m-%d %H:%M:%S'),axis=1)
print(len(dfIn),' --- ',dfIn.head(5), ' ::  ',dfIn.columns)

#readSheet.InsertNewValue_DB(nowStr, 'database',)

################  Read database-refresh 
######################################################################################
#tableName, rowList, yyyymmList, locationList, updateTime=readSheet.GetPreviousValue_DB(sheetList)
tableName, updateTime=readSheet.GetPreviousValue_DB(sheetList)

previous_df=pd.DataFrame(list(zip(tableName,  updateTime)), columns=['TableName', 'UpdateDateTime'] )
print(' ==> ', previous_df)
previous_df.to_excel(file_path+'\\'+input_file)
# ########################################################################################
# previous_df=pd.read_excel(file_path+'\\'+input_file)
# print(' test ==> ', previous_df)
########################################################################################
previous_df['Date']=previous_df.apply(lambda x: Convert_String_DateTime(x['UpdateDateTime'],'%d/%m/%Y %H:%M:%S'),axis=1)


finalDf=dfIn.merge(previous_df, on="TableName", how="left", indicator=True)
finalDf=finalDf[finalDf['_merge']=='both'].copy().reset_index(drop=True)
finalDf.drop(columns=['_merge','rowid'],inplace=True)
print('finalDf ==> ', finalDf)
#finalDf.to_excel(file_path+'\\'+'check_final.xlsx')

## search list of updated table
updateList=[]
rowList=[]
yyyymmList=[]
dateList=[]
for n in range(len(finalDf)):
    #print(' ===> ',n)
    dummyDate_new=finalDf['Date_x'].iloc[n]
    dummyDate_old=finalDf['Date_y'].iloc[n]    
    if(dummyDate_new>dummyDate_old):
        print(dummyDate_new, ' :: ',dummyDate_old, ' ::  yes  ==>', finalDf['TableName'].iloc[n], ' ::: ',finalDf['UpdateDateTime_x'].iloc[n])
        updateList.append(finalDf['TableName'].iloc[n])
        rowList.append(finalDf['TotalRows'].iloc[n])
        yyyymmList.append(finalDf['Max_YYYYMM'].iloc[n])
        dateList.append(finalDf['UpdateDateTime_x'].iloc[n])
    else:    
        print(' same ')


print(' update: ',updateList)

readSheet.Get_Element_DB( sheetList, updateList, rowList, yyyymmList,dateList)


###****************************************************************
end_datetime = datetime.now()
print ('---Start---',start_datetime)
print('---complete---',end_datetime)
DIFFTIME = end_datetime - start_datetime 
DIFFTIMEMIN = DIFFTIME.total_seconds()
print('Time_use : ',round(DIFFTIMEMIN,2), ' Seconds')

##-----------------------------------------------------------------------
## Write log file
activityLog=' database  to DB Successful at '+nowStr+ ' ::  Time_use : '+str(round(DIFFTIMEMIN,2))+ ' Seconds ******** \n'

log_file="Database_inToDB_"+todayStr
f = open(file_path+'\\log\\'+log_file, "a")
f.write(activityLog)
f.close()
import pandas as pd
#from pandas_datareader import data
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, date
import pytz
import time

catDict={ 'currency':['THB=X','EURUSD=X','CNY=X','SGD=X' ,'HKD=X', 'JPY=X' ],
                       'oil':['CL=F'] ,
                       #'stock': ['AOT.BK','INTUCH.BK','^SET.BK']
                       'stock': ['AOT.BK','INTUCH.BK'],
                       'flow':['SET'],
                       'general':['Inflation','Export','Interest','GDP_growth']
                     }

colDict_1={'Date':1,
        'Adj Close':2,
        'UpdateTime':3
}

colDict_2={'Date':1,
        'Volume':6,
        'Adj Close':7,
        'UpdateTime':8
}

colDict_flow={'Date':1,
        'Inst_Domestic':2,
        'Security_Company':3,
        'Investor_Abroad':4,
        'Investor_Domestic':5,
        'UpdateTime':6
}

colDict_general={'Date':1,
        'Percent':2,
        'UpdateTime':3
}


class ReadSheet(object):
    def __init__(self):
        self.secret_path_1=r'/home/pi/Project/DataCollection/CheckInOutReminder-e2ff28c53e80.json'
        self.secret_path_2=r'./CheckInOutReminder-e2ff28c53e80.json'
        self.scope= ['https://spreadsheets.google.com/feeds',
                              'https://www.googleapis.com/auth/drive']

    def Authorization(self):
        try:
            creds = ServiceAccountCredentials.from_json_keyfile_name(self.secret_path_1, self.scope)
        except:
            creds = ServiceAccountCredentials.from_json_keyfile_name(self.secret_path_2, self.scope)
        client = gspread.authorize(creds) 
        sheet = client.open("DataCollection_1").sheet1
        return sheet

    def Authorization_DB_Refresh(self):
        try:
            creds = ServiceAccountCredentials.from_json_keyfile_name(self.secret_path_1, self.scope)
        except:
            creds = ServiceAccountCredentials.from_json_keyfile_name(self.secret_path_2, self.scope)
        client = gspread.authorize(creds) 
        sheet = client.open("database_refresh").sheet1
        return sheet


    def Authorization_Currency(self):
        try:
            creds = ServiceAccountCredentials.from_json_keyfile_name(self.secret_path_1, self.scope)
        except:
            creds = ServiceAccountCredentials.from_json_keyfile_name(self.secret_path_2, self.scope)
        client = gspread.authorize(creds) 
        sheetCList=[]
        cList=catDict['currency']
        for n in cList:
            sheetCList.append(client.open("DataCollection_Currency").worksheet(n))
        return sheetCList

    def Authorization_Oil(self):
        try:
            creds = ServiceAccountCredentials.from_json_keyfile_name(self.secret_path_1, self.scope)
        except:
            creds = ServiceAccountCredentials.from_json_keyfile_name(self.secret_path_2, self.scope)
        client = gspread.authorize(creds) 
        sheetOList=[]
        cList=catDict['oil']
        for n in cList:
            sheetOList.append(client.open("DataCollection_Oil").worksheet(n))
        return sheetOList
    
    def Authorization_Stock(self):
        try:
            creds = ServiceAccountCredentials.from_json_keyfile_name(self.secret_path_1, self.scope)
        except:
            creds = ServiceAccountCredentials.from_json_keyfile_name(self.secret_path_2, self.scope)
        client = gspread.authorize(creds) 
        sheetSList=[]
        cList=catDict['stock']
        for n in cList:
            sheetSList.append(client.open("DataCollection_Stock").worksheet(n))
        return sheetSList

    def Authorization_Flow(self):
        try:
            creds = ServiceAccountCredentials.from_json_keyfile_name(self.secret_path_1, self.scope)
        except:
            creds = ServiceAccountCredentials.from_json_keyfile_name(self.secret_path_2, self.scope)
        client = gspread.authorize(creds) 
        sheetFList=[]
        cList=catDict['flow']
        for n in cList:
            sheetFList.append(client.open("DataScraping_1").worksheet(n))
        return sheetFList

    def Authorization_General(self):
        try:
            creds = ServiceAccountCredentials.from_json_keyfile_name(self.secret_path_1, self.scope)
        except:
            creds = ServiceAccountCredentials.from_json_keyfile_name(self.secret_path_2, self.scope)
        client = gspread.authorize(creds) 
        sheetFList=[]
        cList=catDict['general']
        for n in cList:
            sheetFList.append(client.open("DataScraping_2").worksheet(n))
        return sheetFList


    def StrToDate(self,strIn):
        return datetime.strptime(strIn, '%Y-%m-%d')

    def Date2TString(self, dateIn):
        return dateIn.strftime("%Y-%m-%d")

    def GetDateTime(self):
        todayUTC=datetime.today()
        nowUTC=datetime.now()
        # dd/mm/YY H:M:S
        to_zone = pytz.timezone('Asia/Bangkok')

        today=todayUTC.astimezone(to_zone)
        now=nowUTC.astimezone(to_zone)

        todayStr=today.strftime("%Y-%m-%d")
        nowDate = now.strftime("%Y-%m-%d")
        nowTime = now.strftime("%H:%M:%S")

        #print(' today : ',todayStr)
        #print(nowDate, ' ==> ', nowTime)
        return todayStr, nowDate, nowTime
    
    def InsertNewValue_DB(self,todayStr, sheet):
        lenRecords=len(sheet.get_all_values())
        list_of_hashes=sheet.get_all_records()
        lenHash=len(list_of_hashes)
        print(" len : ",lenRecords)
        lastDate=sheet.cell(lenRecords,1).value
        print(' lastDate : ',lastDate)
        lenDate=len(list_of_hashes[lenHash-1]['Date'])
        if(todayStr == lastDate):
            todayRow=lenRecords
            row_index=todayRow
            col_index=colDict_1['Adj Close']
            message=priceIn
            sheet.update_cell(row_index, col_index,message)
            col_index=colDict_1['UpdateTime']
            message=nowTime
            sheet.update_cell(row_index, col_index,message)
            print('Updated at ', nowTime)
        else:
            todayRow=lenRecords+1
            row_index=todayRow
            col_index=colDict_1['Date']
            message=todayStr
            sheet.update_cell(row_index, col_index,message)
            col_index=colDict_1['Adj Close']
            message=priceIn
            sheet.update_cell(row_index, col_index,message)
            col_index=colDict_1['UpdateTime']
            message=nowTime
            sheet.update_cell(row_index, col_index,message)
            print('Updated on ', todayStr, ' :: ', nowTime)

    def GetPreviousValue_DB(self, sheet):
        lenRecords=len(sheet.get_all_values())
        list_of_hashes=sheet.get_all_records()
        lenHash=len(list_of_hashes)
        print(" len : ",lenRecords)
        NameList=[]
        #RowList=[]
        #YYYYMMList=[]
        #LocationList=[]
        UpdateDateTimeList=[]
        for n in range(2,lenRecords+1):
            #print(' : ',n, ' --',sheet.cell(n,1).value)
            NameList.append(sheet.cell(n,1).value)
            #RowList.append(sheet.cell(n,2).value)
            #YYYYMMList.append(sheet.cell(n,3).value)
            #LocationList.append(sheet.cell(n,4).value)
            UpdateDateTimeList.append(sheet.cell(n,5).value)

        
        #return NameList, RowList, YYYYMMList, LocationList, UpdateDateTimeList
        return NameList, UpdateDateTimeList

    def Get_Element_DB(self, sheet, updateList, rowList, yyyymmList, dateList):
        lenRecords=len(sheet.get_all_values())
        list_of_hashes=sheet.get_all_records()
        lenHash=len(list_of_hashes)
        print(" len : ",lenRecords)

        for n in range(2,lenRecords+1):
            dummyName=sheet.cell(n,1).value
            for name,row,yyyymm,dateT in zip(updateList,rowList,yyyymmList,dateList):
                #print(updateName, ' ===> ', dummyName)
                if(name==dummyName):
                    change_format=datetime.strptime(dateT, '%Y-%m-%d %H:%M:%S')
                    str_format=change_format.strftime('%d/%m/%Y %H:%M:%S')
                    print(n,' :: ',name, ' :: ',row, ' ::  ', yyyymm,' :: ',dateT, ' ==== ',type(dateT),' ::  ',str_format)
                    sheet.update_cell(n, 2,str(row))                    
                    sheet.update_cell(n, 3,str(yyyymm))
                    sheet.update_cell(n, 5,str(str_format))
                    time.sleep(20)

        return None



    def InsertNewValue_1(self,todayStr, nowDate, nowTime, sheet, dateIn, priceIn):
        lenRecords=len(sheet.get_all_values())
        list_of_hashes=sheet.get_all_records()
        lenHash=len(list_of_hashes)
        print(" len : ",lenRecords)
        lastDate=sheet.cell(lenRecords,1).value
        print(' lastDate : ',lastDate)
        lenDate=len(list_of_hashes[lenHash-1]['Date'])
        if(todayStr == lastDate):
            todayRow=lenRecords
            row_index=todayRow
            col_index=colDict_1['Adj Close']
            message=priceIn
            sheet.update_cell(row_index, col_index,message)
            col_index=colDict_1['UpdateTime']
            message=nowTime
            sheet.update_cell(row_index, col_index,message)
            print('Updated at ', nowTime)
        else:
            todayRow=lenRecords+1
            row_index=todayRow
            col_index=colDict_1['Date']
            message=todayStr
            sheet.update_cell(row_index, col_index,message)
            col_index=colDict_1['Adj Close']
            message=priceIn
            sheet.update_cell(row_index, col_index,message)
            col_index=colDict_1['UpdateTime']
            message=nowTime
            sheet.update_cell(row_index, col_index,message)
            print('Updated on ', todayStr, ' :: ', nowTime)

    def GetPreviousValue(self, todayStr, nowDate, nowTime, sheet):
        lenRecords=len(sheet.get_all_values())
        list_of_hashes=sheet.get_all_records()
        lenHash=len(list_of_hashes)
        print(" len : ",lenRecords)
        lastDate=sheet.cell(lenRecords,1).value
        print(" lastDate : ", lastDate)
        lenDate=len(list_of_hashes[lenHash-1]['Date'])
        previousDate=sheet.cell(lenRecords-1,1).value
        previousPrice=sheet.cell(lenRecords-1,2).value
        return lastDate, previousDate, previousPrice


    def InsertNewValue_2(self,todayStr, nowDate, nowTime, sheet, dateIn, priceIn, volumeIn):
        lenRecords=len(sheet.get_all_values())
        list_of_hashes=sheet.get_all_records()
        lenHash=len(list_of_hashes)
        print(" len : ",lenRecords)
        lastDate=sheet.cell(lenRecords,1).value
        print(' lastDate : ',lastDate)
        lenDate=len(list_of_hashes[lenHash-1]['Date'])
        if(todayStr == lastDate):
            todayRow=lenRecords
            row_index=todayRow
            col_index=colDict_2['Adj Close']
            message=priceIn
            sheet.update_cell(row_index, col_index,message)
            col_index=colDict_2['Volume']
            message=volumeIn
            sheet.update_cell(row_index, col_index,message)
            col_index=colDict_2['UpdateTime']
            message=nowTime
            sheet.update_cell(row_index, col_index,message)
            print('Updated at ', nowTime)
        else:
            todayRow=lenRecords+1
            row_index=todayRow
            col_index=colDict_2['Date']
            message=todayStr
            sheet.update_cell(row_index, col_index,message)
            col_index=colDict_2['Adj Close']
            message=priceIn
            sheet.update_cell(row_index, col_index,message)
            col_index=colDict_2['Volume']
            message=volumeIn
            sheet.update_cell(row_index, col_index,message)
            col_index=colDict_2['UpdateTime']
            message=nowTime
            sheet.update_cell(row_index, col_index,message)
            print('Updated on ', todayStr, ' :: ', nowTime)

    def InsertNewValue_Flow(self,todayStr, nowDate, nowTime, sheet, dateIn, Inst_Domestic, Security_Company, Investor_Abroad, Investor_Domestic):
        lenRecords=len(sheet.get_all_values())
        list_of_hashes=sheet.get_all_records()
        lenHash=len(list_of_hashes)
        print(" len : ",lenRecords)
        lastDate=sheet.cell(lenRecords,1).value
        print(' lastDate : ',lastDate)
        lenDate=len(list_of_hashes[lenHash-1]['Date'])
        if(dateIn == lastDate):
            todayRow=lenRecords
            row_index=todayRow
            col_index=colDict_flow['Inst_Domestic']
            message=Inst_Domestic
            sheet.update_cell(row_index, col_index,message)
            col_index=colDict_flow['Security_Company']
            message=Security_Company
            sheet.update_cell(row_index, col_index,message)

            col_index=colDict_flow['Investor_Abroad']
            message=Investor_Abroad
            sheet.update_cell(row_index, col_index,message)

            col_index=colDict_flow['Investor_Domestic']
            message=Investor_Domestic
            sheet.update_cell(row_index, col_index,message)
            
            col_index=colDict_flow['UpdateTime']
            message=nowTime
            sheet.update_cell(row_index, col_index,message)
            print('Updated at ', nowTime)
        else:
            todayRow=lenRecords+1
            row_index=todayRow
            col_index=colDict_flow['Date']
            message=dateIn
            sheet.update_cell(row_index, col_index,message)
            col_index=colDict_flow['Inst_Domestic']
            message=Inst_Domestic
            sheet.update_cell(row_index, col_index,message)
            col_index=colDict_flow['Security_Company']
            message=Security_Company
            sheet.update_cell(row_index, col_index,message)

            col_index=colDict_flow['Investor_Abroad']
            message=Investor_Abroad
            sheet.update_cell(row_index, col_index,message)

            col_index=colDict_flow['Investor_Domestic']
            message=Investor_Domestic
            sheet.update_cell(row_index, col_index,message)
            
            col_index=colDict_flow['UpdateTime']
            message=nowTime
            sheet.update_cell(row_index, col_index,message)            
            print('Updated on ', todayStr, ' :: ', nowTime)

    def InsertNewValue_General(self,todayStr, nowDate, nowTime, sheet, dateIn, percentIn):
        lenRecords=len(sheet.get_all_values())
        list_of_hashes=sheet.get_all_records()
        lenHash=len(list_of_hashes)
        print(" len : ",lenRecords)
        lastDate=sheet.cell(lenRecords,1).value
        print(' lastDate : ',lastDate)
        lenDate=len(list_of_hashes[lenHash-1]['Date'])
        if(dateIn == lastDate):
            todayRow=lenRecords
            row_index=todayRow
            col_index=colDict_general['Percent']
            message=percentIn
            sheet.update_cell(row_index, col_index,message)
            col_index=colDict_general['UpdateTime']
            message=nowDate+' '+nowTime
            sheet.update_cell(row_index, col_index,message)
            print('Updated at ', nowTime)
        else:
            todayRow=lenRecords+1
            row_index=todayRow
            col_index=colDict_general['Date']
            message=dateIn
            sheet.update_cell(row_index, col_index,message)
            col_index=colDict_general['Percent']
            message=percentIn
            sheet.update_cell(row_index, col_index,message)
            col_index=colDict_general['UpdateTime']
            message=nowDate+' '+nowTime
            sheet.update_cell(row_index, col_index,message)
            print('Updated on ', todayStr, ' :: ', nowTime)

    def LoadSheet_0(self,sheet):
        listSheet = sheet.get_all_values()
        #print(' ==> ',type(listSheet)," :: ",listSheet)
        listHash=sheet.get_all_records()
        #print(' ==> ',type(listHash)," :: ",listHash)

        dfSet=pd.DataFrame()
        lenList=len(listHash)
        colList=listSheet[0]
        print(colList)
        dateList=[]
        priceList=[]
        

        for n in range(0,lenList):
            dateList.append(self.StrToDate(listHash[n][colList[0]]))
            priceList.append(listHash[n][colList[1]])
    
        dfSet=pd.concat([pd.DataFrame(dateList),pd.DataFrame(priceList)],axis=1)
        #print(dfSet.columns)
        dfSet.columns=colList

        return dfSet


    def LoadSheet(self,sheet):
        listSheet = sheet.get_all_values()
        #print(' ==> ',type(listSheet)," :: ",listSheet)
        listHash=sheet.get_all_records()
        #print(' ==> ',type(listHash)," :: ",listHash)

        dfSet=pd.DataFrame()
        lenList=len(listHash)
        colList=listSheet[0]
        #print(colList)
        dateList=[]
        priceList=[]
        updateList=[]
        for n in range(0,lenList):
            dateList.append(self.StrToDate(listHash[n][colList[0]]))
            priceList.append(listHash[n][colList[1]])
            updateList.append(listHash[n][colList[2]])
    
        dfSet=pd.concat([pd.DataFrame(dateList),pd.DataFrame(priceList),pd.DataFrame(updateList)],axis=1)
        dfSet.columns=colList

        return dfSet

    def LoadSheet_2(self,sheet):
        listSheet = sheet.get_all_values()
        #print(' ==> ',type(listSheet)," :: ",listSheet)
        listHash=sheet.get_all_records()
        #print(' ==> ',type(listHash)," :: ",listHash)

        dfSet=pd.DataFrame()
        lenList=len(listHash)
        colList=listSheet[0]
        #print(colList)
        dateList=[]
        priceList=[]
        updateList=[]
        volumeList=[]
        for n in range(0,lenList):
            dateList.append(self.StrToDate(listHash[n][colList[0]]))
            volumeList.append(listHash[n][colList[5]])
            priceList.append(listHash[n][colList[6]])
            updateList.append(listHash[n][colList[7]])
    
        dfSet=pd.concat([pd.DataFrame(dateList),pd.DataFrame(volumeList),pd.DataFrame(priceList),pd.DataFrame(updateList)],axis=1)
        dfSet.columns=['Date','Volume','Adj Close','UpdateTime']

        return dfSet

    def ConvertCurrency_2(self,dfIn,category):
        cList=catDict[category]

        dfTHB=dfIn[0]['Date'].to_frame()
      
        dfTHB=pd.concat([dfTHB,dfIn[0]['Adj Close'].to_frame(),dfIn[1]['Adj Close'].to_frame(),dfIn[2]['Adj Close'].to_frame(),dfIn[3]['Adj Close'].to_frame(),dfIn[4]['Adj Close'].to_frame(),dfIn[5]['Adj Close'].to_frame()], axis=1)
        dfTHB.columns=['Date','THB_USD', 'EUR','CNY','SGD' ,'HKD', 'JPY']
        
        
        dfTHB['THB_EUR']=dfTHB['THB_USD']/dfTHB['EUR']
        dfTHB['THB_CNY']=dfTHB['THB_USD']/dfTHB['CNY']
        dfTHB['THB_SGD']=dfTHB['THB_USD']/dfTHB['SGD']
        dfTHB['THB_HKD']=dfTHB['THB_USD']/dfTHB['HKD']
        dfTHB['THB_JPY']=dfTHB['THB_USD']/dfTHB['JPY']

        dfCon=dfTHB[['Date','THB_USD','THB_EUR','THB_CNY','THB_SGD','THB_HKD','THB_JPY']].copy()
        #print(dfCon.tail(), ' :: ',dfCon.columns)
        return dfCon


class LoadData(object):
    def __init__(self):
        self.QUANDL_API_KEY = 'abe1CkdZn-beCcde_GSt'
        self.start_date= '2015-01-01'
        self.filepath1=r'C:/Users/70018928/Quantra_Learning/data/'
        self.filepath2='data/'


    def LoadYahoo_Data(self,end, category):
        cList=catDict[category]
        dfList=[]
        for n in cList:
            dfList.append(data.get_data_yahoo(n, self.start_date, end))
        return dfList

    def LoadYahoo_Data_NoEnd(self,category):
        cList=catDict[category]
        dfList=[]
        for n in cList:
            dfList.append(data.get_data_yahoo(n, self.start_date))
        return dfList
    
    def WriteData(self,ticker,dfIn):
        try:
            fileout=self.filepath1+ticker+'.csv'
            dfIn.to_csv(fileout)
        except:
            fileout=self.filepath2+ticker+'.csv'
            dfIn.to_csv(fileout)

    def WriteInitialData(self,dfIn,category):
        cList=catDict[category]
        for n in range(0,len(dfIn)):
            filename=self.filepath1+cList[n]+'.csv'
            #print(cList[n])       
            dfIn[n].to_csv(filename)

    def ConvertCurrency(self,dfIn,category):
        cList=catDict[category]
        for n in range(0,len(dfIn)):
            filename=self.filepath1+cList[n]+'.csv'
            #print(cList[n])       
            dfIn[n].to_csv(filename)
        dfTHB=dfIn[0]['Adj Close'].to_frame()
      
        dfTHB=pd.concat([dfTHB,dfIn[1]['Adj Close'].to_frame(),dfIn[2]['Adj Close'].to_frame(),dfIn[3]['Adj Close'].to_frame(),dfIn[4]['Adj Close'].to_frame(),dfIn[5]['Adj Close'].to_frame()], axis=1)
        dfTHB.columns=['THB_USD', 'EUR','CNY','SGD' ,'HKD', 'JPY']
        
        dfTHB['THB_EUR']=dfTHB['THB_USD']/dfTHB['EUR']
        dfTHB['THB_CNY']=dfTHB['THB_USD']/dfTHB['CNY']
        dfTHB['THB_SGD']=dfTHB['THB_USD']/dfTHB['SGD']
        dfTHB['THB_HKD']=dfTHB['THB_USD']/dfTHB['HKD']
        dfTHB['THB_JPY']=dfTHB['THB_USD']/dfTHB['JPY']

        dfCon=dfTHB[['THB_USD','THB_EUR','THB_CNY','THB_SGD','THB_HKD','THB_JPY']].copy()
        #print(dfCon.tail(), ' :: ',dfCon.columns)
        return dfCon

    

            
         
        
    
    





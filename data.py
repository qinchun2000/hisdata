#!/usr/bin/python3

# coding=utf-8
from datetime import date
from datetime import datetime

import io
import time
import re
import yaml
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import Column, String,DECIMAL,DateTime,Integer 
from sqlalchemy.ext.declarative import declarative_base
#from sqlalchemy import create_engine, Column, Integer, String,  Boolean, DECIMAL, Enum, Date, DateTime, Time, Text
Base = declarative_base()

from jqdatasdk import *
from sqlalchemy.orm import sessionmaker
from maincontract  import MyContract


path = "/root/hisdata/config.yml"
file = io.open(path, 'r', encoding="utf-8")
file_data = file.read()
file.close()            
config = yaml.load(file_data,Loader=yaml.FullLoader)
engine = create_engine('mysql://root:111111@127.0.0.1/dayline?charset=utf8mb4') 
conn = engine.connect() 


user =config['user']
password = config['authcode']
auth(user,password)

def showAllFuture():

    auth(user,password)
    current_date = time.strftime("%Y-%m-%d", time.localtime()) 
    df =  get_all_securities(['futures'],date=current_date)
    for index, row in df.iterrows():
        print (index,row["display_name"], row["name"])
#     print (df)


def update_contract_to_mysql(contract,name):
    print("start update ------>%s"%(contract))
#     auth('18630881826','Anran881826')
    current_date = time.strftime("%Y-%m-%d", time.localtime()) 
#     df =  get_all_securities(['futures'],date=current_date)
#     for index, row in df.iterrows():
#         print (index,row["display_name"], row["name"])
#     print (df)
#     contract ="RB2105.XSGE" 
#     name = get_security_info(contract).name
#     print("name--->%s"%(name))
#     ret = re.findall('*(8888).',contract)
    if contract.find("8888")>0 or contract.find("9999")>0 :
        print ("main %s"%(contract))
        return False
    elif  contract.find("CCFX")>0:
        return False
    
    s_date = get_security_info(contract).start_date
    print(s_date)
    
    df = get_price(contract,start_date=s_date, end_date=current_date, frequency='daily',fields=['open', 'high','low','close','volume','money','pre_close'])
#     print (df)    

    df1 = get_extras('futures_sett_price', contract,start_date=s_date, end_date=current_date)
#     print (df1)
    df1.dropna(axis=0, how='any')
    last_df1=df1.rename(columns={contract : 'sett_price'})
#     print (last_df1)
 
    df2 = get_extras('futures_positions', contract, start_date=s_date, end_date=current_date)
#     print (df2)
    last_df2=df2.rename(columns={contract : 'opi'})
#     print (last_df2)
 
    mydf=pd.concat([df,last_df1,last_df2],axis=1)
#     mydf=pd.concat([df,df1],axis=1)
#     print (mydf)
     
    last_df = mydf.dropna(axis=0,how='any')
     
#     print (last_df)
    ret = contract,name,last_df 
    return True,"",ret


def check(contract):
    print("start update ------>%s"%(contract))
#     auth('18630881826','Anran881826')
    current_date = time.strftime("%Y-%m-%d", time.localtime()) 
#     df =  get_all_securities(['futures'],date=current_date)
#     for index, row in df.iterrows():
#         print (index,row["display_name"], row["name"])
#     print (df)
#     contract ="RB2105.XSGE" 
    name = get_security_info(contract).name
    print("name--->%s"%(name))
    
    s_date = get_security_info(contract).start_date
    print(s_date)
    
    df = get_price(contract,start_date=s_date, end_date=current_date, frequency='daily',fields=['open', 'high','low','close','volume','money','pre_close'])
#     print (df)    

    df1 = get_extras('futures_sett_price', contract,start_date=s_date, end_date=current_date)
#     print (df1)
    last_df1=df1.rename(columns={contract : 'sett_price'})
#     print (last_df1)
 
    df2 = get_extras('futures_positions', contract, start_date=s_date, end_date=current_date)
#     print (df2)
    last_df2=df2.rename(columns={contract : 'opi'})
#     print (last_df2)
 
    mydf=pd.concat([df,last_df1,last_df2],axis=1)
#     mydf=pd.concat([df,df1],axis=1)
#     print (mydf)
     
    last_df = mydf.dropna(axis=1,how='any')
     
    print (last_df)
    return contract,name,last_df 

def check_mycontract(name,exchangeid):    
    
    exchange = {"DCE":".XDCE" ,"CZCE":".XZCE","SHFE":".XSGE"}

    my_name = "" 
    if exchangeid =="CZCE":
        current_year = time.strftime("%Y", time.localtime())
        ret=re.findall('^([A-Z]+)(\d+)',name)
        if len(ret)>0:
            my_name = ret[0][0].upper()+current_year[2]+ret[0][1]
        else:
            return False,"",""
    else:
        my_name = name
    
        
    contract = my_name.upper()+exchange[exchangeid]
    print("start update %s------>%s"%(my_name,contract))
#     auth('18630881826','Anran881826')
    current_date = time.strftime("%Y-%m-%d", time.localtime()) 
#     df =  get_all_securities(['futures'],date=current_date)
#     for index, row in df.iterrows():
#         print (index,row["display_name"], row["name"])
#     print (df)
#     contract ="RB2105.XSGE" 
#     name = get_security_info(contract).name
#     print("name--->%s"%(name))
    
    s_date = get_security_info(contract).start_date
    print(s_date)
    
    df = get_price(contract,start_date=s_date, end_date=current_date, frequency='daily',fields=['open', 'high','low','close','volume','money','pre_close'])
#     print (df)    

    df1 = get_extras('futures_sett_price', contract,start_date=s_date, end_date=current_date)
#     print (df1)
    df1.dropna(axis=0, how='any')
    last_df1=df1.rename(columns={contract : 'sett_price'})
#     print (last_df1)
 
    df2 = get_extras('futures_positions', contract, start_date=s_date, end_date=current_date)
#     print (df2)
    last_df2=df2.rename(columns={contract : 'opi'})
#     print (last_df2)
 
    mydf=pd.concat([df,last_df1,last_df2],axis=1)
#     mydf=pd.concat([df,df1],axis=1)
#     print (mydf)
     
    last_df = mydf.dropna(axis=0,how='any')
     
#     print (last_df)
    ret = contract,name,last_df 
    return True,"",ret

    
        
def insert_contract(contract,name,df):
    engine = create_engine('mysql://root:111111@127.0.0.1/dayline?charset=utf8mb4') 
    conn = engine.connect()
     
    Session = sessionmaker(engine)
    # 打开会话对象 Session
    db_session = Session()
    # 在db_session会话中添加一条 UserORM模型创建的数据


    class MyDaily(Base):
        """日线行情
        ts_code    str    N    股票代码（二选一）
        trade_date    str    N    交易日期（二选一）
        start_date    str    N    开始日期(YYYYMMDD)
        end_date    str    N    结束日期(YYYYMMDD)
        """
        __tablename__ = name
    
        Time = Column(DateTime, primary_key=True)    # 交易日期
        Open = Column(DECIMAL(10, 4))        # 开盘价
        High = Column(DECIMAL(10, 4))        # 最高价 
        Low = Column(DECIMAL(10, 4))         # 最低价
        Close = Column(DECIMAL(10, 4))       # 收盘价
        LastClose = Column(DECIMAL(10, 4))  
        PreSettlementPrice = Column(DECIMAL(10, 4)) 
        SettlementPrice = Column(DECIMAL(10, 4)) 
        Volume = Column(DECIMAL(20, 4))         # 成交量 （手） 
        OpenInterest = Column(Integer)         # 成交量 （手） 
        Amount = Column(DECIMAL(20, 4))      # 成交额 （千元）

    Base.metadata.create_all(engine)         
#    pandas_index = df.index    # 将dataframe的索引赋给一个变量
#     df.insert(0, 'date', pandas_index) 
#     print (df)


    for index,r in df.iterrows():
#         print(index,r["open"])

        daily = MyDaily(Time=index,
                      Open=r['open'],
                      High=r['high'],
                      Low=r['low'],
                      Close=r['close'],
                      PreSettlementPrice=r['pre_close'],
                      SettlementPrice=r['sett_price'],
                      OpenInterest=r['opi'],
                      Volume=r['volume'],
                      Amount=r['money'])
    
    
        with db_session.no_autoflush:
#             print("index  --> %s"%(index))
            existing =db_session.query(MyDaily).get(index)
            
            if not existing :
                print("insert  --> %s"%(index))
                db_session.add(daily)
            else:
                result = db_session.query(MyDaily).filter(MyDaily.Time==index)
                
                if result.first().SettlementPrice != r['sett_price']:
                    print("delete  --> %s: %s =>  %s "%(index,result.first().SettlementPrice,r['sett_price']))
                    db_session.query(MyDaily).filter(MyDaily.Time==index).delete()
                    db_session.add(daily)

#                 for row in existing.one():
#                     print (row)
# #                 print ( existing.all())
#                 print("%s existing --> %s, not insert date "%(name,index))

        
    # 使用 db_session 会话提交 , 这里的提交是指将db_session中的所有指令一次性提交
    db_session.commit()    
    
def main():
    
    # 创建数据库引擎

    print("start auth jq...")
    auth(user,password)
    current_date = time.strftime("%Y-%m-%d", time.localtime())
    df =  get_all_securities(['futures'],date=current_date)
    for index, row in df.iterrows():
        print (index,row["display_name"], row["name"])
        update_contract_to_mysql(index,row["name"])

#     contract_rb=get_dominant_future('RB','2021-01-29')
#     print ("--->%s"%(contract_rb))
#     contract_au=get_dominant_future('AU','2021-01-29')
#     print ("--->%s"%(contract_au))

    print("update history data is ok...")
    
if __name__ == "__main__":
#     showAllFuture()
#     ret=check("RB2105.XSGE" )
#     insert_contract(ret[0],ret[1],ret[2])
#     ret= check_mycontract("a2109","DCE")
#     insert_contract(ret[2][0],ret[2][1],ret[2][2])

#     ret= check_mycontract("rb2105","SHFE")
#     insert_contract(ret[2][0],ret[2][1],ret[2][2])
        
#     ret=check("A8888.XDCE" )
#     insert_contract(ret[0],ret[1],ret[2])
#     update_contract_to_mysql("A8888.XDCE","A8888")    
#     main()
    maincontract = MyContract()
    list = maincontract.getMyContract()
    print (list)
    for item in list:
        ret= check_mycontract(item['InstrumentID'],item['ExchangeID'])
        insert_contract(ret[2][0],ret[2][1],ret[2][2])
    for item in list:
        ret= check_mycontract(item['InstrumentID_next'],item['ExchangeID'])
        insert_contract(ret[2][0],ret[2][1],ret[2][2])
        insert_contract(ret[2][0],ret[2][1],ret[2][2])

    


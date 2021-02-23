from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date
from sqlalchemy.orm import sessionmaker


# 创建连接
engine = create_engine('mysql+pymysql://root:111111@localhost/baseinfo',
                       encoding='utf-8')

Base = declarative_base()  # 生成orm基类        
        
class MainContract(Base):  # User继承了上述的操作

    __tablename__ = 'maincontract'
    ProductID = Column(String(30), primary_key=True)
#    id = Column(Integer, primary_key=True)
#    ProductId = Column(String(30))
    InstrumentID = Column(String(40))
    InstrumentID_next = Column(String(40))
    ExchangeID = Column(String(40))

    def __repr__(self): # 返回查询到的信息
        	
        return '<ProductId: %s ins: %s exchangeid: %s >' %(self.ProductID, self.InstrumentID,self.ExchangeID)
        #return 'ProductId: %s ins: %s>' %(self.ProductID, self.InstrumentID)

    def dobule_to_dict(self):
        result = {}
        for key in self.__mapper__.c.keys():
            if getattr(self, key) is not None:
                result[key] = str(getattr(self, key))
            else:
                result[key] = getattr(self, key)
        return result


class MyContract():
    def __init__ (self):

        pass
        
    def to_json(self,all_vendors):
        v = [ ven.dobule_to_dict() for ven in all_vendors ]
        return v
    
    def getMyContract(self):
        # 执行上述的操作
        Base.metadata.create_all(engine)
        Session_class = sessionmaker(bind=engine)  # 进行数据库的连接
        Session = Session_class() # 生成session 实例
        
        # 进行数据查询 .all()输出所有的检索结果，.first() 输出第一条的结果
        data = Session.query(MainContract).all()
        #print(data)
        m = self.to_json(data)
#         print(m) 
#         for d in m:
#             print (d['InstrumentID'],d['ExchangeID'])
        return m


    
#data = Session.query(User).filter_by(name='alex').first()
#print(data)
#data = Session.query(User).filter(User.name == 'rain').first()
#print(data)

# 多条件查询
#data = Session.query(User).filter(User.name=='rain').filter(User.id > 1).first()
#print(data)

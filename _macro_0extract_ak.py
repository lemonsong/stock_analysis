import akshare as ak
m2 = ak.macro_china_m2()
print(m2.tail())

# 获取PMI数据
pmi = ak.macro_china_pmi()
print(pmi.tail())

# 获取消费者信心指数
consumer_confidence = ak.macro_china_consumer_confidence()
print(consumer_confidence.tail())

# 获取房屋新开工面积
housing = ak.macro_china_housing_start()
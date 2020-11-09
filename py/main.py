import pandas as pd

from clean.OrderProcess import OrderCleaner, OrderReport
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import datetime
from collect import collect

if __name__ == '__main__':



    SqlName = "total_2020-11-7"
    Input_Data = 'D:\develop\PycharmProjects\salereaport\clean\input\整合数据-11月7日.xlsx'
    df1 = 'D:\develop\PycharmProjects\salereaport\collect\总数据2020-11-7.xlsx'
    df2 = 'D:\develop\PycharmProjects\salereaport\collect\总数据2020-10-31.xlsx'
    # 连接两张表的数据
    collect.concatTwoDf(df1,df2,Input_Data,SqlName)

    BASE_DIR = './clean'
    file_path = Input_Data
    cols = ["订单时间", "店铺", "订单号", "产品", "产品2", "数量", "零售单价", "币种1",
            "佣金比例", "结算总额", "币种2", "零售单价¥", "结算金额￥", "成本价", "币种3",
            "成本总价￥", "蚂蚁操作费", "头/全程运费￥", "尾程运费￥", "资金成本", "毛利￥", "发货仓位", "收件人国家", "物流单号", "回款情况"]



    ## 数据清洗
    oc = OrderCleaner(file_path, cols)
    order_df = oc.process()
    order_df.index = order_df['订单时间']

    ## 生成7天对比数据
    ort = OrderReport(order_df)
    ort.export_store()
    ort.export_goods()
    ort.export()
    print("-----------------------")
    print("---所有程序执行完毕！---")
    print("-----------------------")

import pandas as pd
import uuid
import numpy as np
import datetime

BASE_DIR = './clean'


def log(func):
    def wrapper(*args, **kw):
        print('call %s()' % func.__name__)
        return func(*args, **kw)

    return wrapper


class OrderCleaner:
    def __init__(self, file_path, cols):
        self.df = pd.read_excel(file_path, header=0, sheet_name="Sheet1")[cols]
        self.goods_df = pd.read_excel(BASE_DIR + "/input/Goods-2020.xlsx", header=0)
        self.depart_df = pd.read_excel(file_path, header=0, sheet_name="部门")

    @log
    def remove_blank_cols(self):
        #         self.df = self.df['产品'].replace('', np.nan, inplace=True)
        self.df.dropna(subset=['产品'], inplace=True)

    @log
    def format_time_cols(self, cols):
        for col in cols:
            self.df[col] = pd.to_datetime(self.df[col]).dt.date

    @log
    def get_week_by_orderdate(self):
        self.df['周数'] = pd.to_datetime(self.df['订单时间']).dt.isocalendar().week

    @log
    def format_contry_col(self):
        self.df['收件人国家'] = self.df['收件人国家'].str.upper()

    @log
    def convert_to_float(self):
        self.df['零售单价'] = self.df['零售单价'].apply(str).astype(float)
        self.df['头/全程运费￥'] = self.df['头/全程运费￥'].replace('未更新', np.nan).astype(float)
        self.df['结算总额'] = self.df['结算总额'].replace(',', '', regex=True).astype(float)

    @log
    def format_order(self):
        self.df['订单号'] = self.df['订单号'].apply(str).replace('nan', '')
        self.df['物流单号'] = self.df['物流单号'].apply(str).replace('nan', '')

    @log
    def money_get_round(self):
        dec_column = ['零售单价', '零售单价¥', '结算总额', '结算金额￥', '成本价', '成本总价￥', '蚂蚁操作费', '头/全程运费￥', '尾程运费￥', '资金成本', '毛利￥']
        for col in dec_column:
            self.df[col] = self.df[col].fillna(0)
            self.df[col] = self.df[col].round(2)

    @log
    def get_depart(self):
        self.df['店铺_lower'] = self.df['店铺'].str.lower()
        self.depart_df['Store_lower'] = self.depart_df['Store'].str.lower()
        self.df = self.df.merge(self.depart_df[['Store_lower', '三级部门', 'Platform', 'Operator']], left_on='店铺_lower',
                                right_on='Store_lower', how='left').drop(['店铺_lower', 'Store_lower'], axis=1)
        self.df = self.df.rename(columns={'Platform': '平台', 'Operator': '负责人'})

    @log
    def get_goods(self):
        self.df = self.df.merge(self.goods_df[['id', 'SKU', '品牌', '二级类目', '产品经理']], left_on='产品', right_on='SKU',
                                how='left').drop('SKU', axis=1)
        self.df = self.df.rename(
            columns={'id': '产品1ID', 'SKU': '产品1SKU', '品牌': '产品1品牌', '二级类目': '产品1二级类目', '产品经理': '产品1产品经理'})
        self.df = self.df.merge(self.goods_df[['id', 'SKU', '品牌', '二级类目', '产品经理']], left_on='产品2', right_on='SKU',
                                how='left').drop('SKU', axis=1)
        self.df = self.df.rename(
            columns={'id': '产品2ID', 'SKU': '产品2SKU', '品牌': '产品2品牌', '二级类目': '产品2二级类目', '产品经理': '产品2产品经理'})

    @log
    def get_profit(self):
        self.df.loc[self.df['毛利￥'] <= 0, '是否亏损'] = '是'
        self.df.loc[self.df['毛利￥'] > 0, '是否亏损'] = '否'

    @log
    def get_profit_per(self):
        self.df['毛利率'] = self.df['毛利￥'] / self.df['结算金额￥']

    @log
    def get_mayi_fee(self):
        self.df.loc[self.df['发货仓位'] == "香港", '蚂蚁操作费用'] = 9.5

    @log
    def get_fit(self):
        cols = ['成本总价￥', '蚂蚁操作费', '头/全程运费￥', '尾程运费￥', '资金成本']
        self.df['矫正毛利'] = self.df['结算金额￥'] - self.df[cols].sum(axis=1)
    #         self.df['差额'] = (self.df['矫正毛利']  - self.df['毛利￥']).round(2)

    @log
    def gen_uuid(self):
        self.df['uuid'] = [uuid.uuid4() for _ in range(len(self.df.index))]

    @log
    def process(self):
        self.get_week_by_orderdate()
        self.format_time_cols(['订单时间'])
        self.remove_blank_cols()
        self.convert_to_float()
        self.format_contry_col()
        self.format_order()
        self.money_get_round()
        #   self.get_mayi_fee()
        self.get_profit()
        self.get_fit()
        self.get_depart()
        self.get_goods()
        # self.get_profit_per()
        return self.df

    def get_finnal_df(self):
        self.process()
        return self.df

    @log
    def export(self):
        now = datetime.datetime.now()
        now = now.strftime('%m-%d-%y %H:%M:%S')
        with pd.ExcelWriter(BASE_DIR + '/output' + '-' + now + '.xlsx') as writer:
            self.df.to_excel(writer, sheet_name='原始订单', index=False)
        print('export succsss')


class OrderStat:
    def __init__(self, order_df):
        self.order_df = order_df
        self.platfrom_df = pd.read_excel('D:\develop\PycharmProjects\salereaport\clean\input//10_depart_target.xlsx',
                                         header=0, sheet_name="Sheet1")
        self.depart_df = pd.read_excel('D:\develop\PycharmProjects\salereaport\clean\input//10_platform_target.xlsx',
                                       header=0, sheet_name="Sheet1")
        self.staff_df = pd.read_excel('D:\develop\PycharmProjects\salereaport\clean\input//10_staff_target.xlsx',
                                      header=0, sheet_name="Sheet1")
        self.today = datetime.datetime.now().strftime('%Y-%m-%d')

    def get_statu_by_platfrom(self):
        by = "平台"
        result_df = self.platfrom_df
        res = self.order_df[['平台', '毛利￥', '结算金额￥']].groupby(by=by, as_index=False).sum()
        res['平台_lower'] = res['平台'].str.lower()
        result_df['平台_lower'] = result_df['平台'].str.lower()
        result_df = result_df.merge(res, left_on='平台_lower', right_on='平台_lower', how='left').drop(['平台_lower', '平台_y'],
                                                                                                   axis=1)
        return self.process(result_df, by)

    def get_statu_by_depart(self):
        by = "三级部门"
        result_df = self.depart_df
        res = self.order_df[['三级部门', '毛利￥', '结算金额￥']].groupby(by=by, as_index=False).sum()
        result_df = result_df.merge(res, left_on='部门', right_on='三级部门', how='left').drop('三级部门', axis=1)
        return self.process(result_df, by)

    def get_statu_by_staff(self):
        by = "负责人"
        result_df = self.staff_df
        res = self.order_df[['负责人', '毛利￥', '结算金额￥']].groupby(by=by, as_index=False).sum()
        result_df = result_df.merge(res, on='负责人', how='left')
        return self.process(result_df, by)

    def process(self, result_df, by):
        result_df['达成量'] = result_df['毛利￥'].fillna(0)
        result_df['营业收入'] = result_df['结算金额￥'].fillna(0)
        result_df = result_df.drop(['毛利￥', '结算金额￥'], axis=1).fillna(0)
        result_df['达成率'] = (result_df['达成量'] / result_df['任务量']).fillna(0)
        result_df['毛利率'] = (result_df['达成量'] / result_df['营业收入']).fillna(0).apply(lambda x: format(x, '.2%'))
        result_df['达成率排名'] = result_df['达成率'].rank(method='min', ascending=False).fillna(0).astype(int)
        result_df['达成率'] = result_df['达成率'].apply(lambda x: format(x, '.2%'))
        result_df['达成排名'] = result_df['达成量'].rank(method='min', ascending=False).fillna(0).astype(int)
        #         result_df['统计日期'] = self.today
        result_df.sort_values(by=['达成量'], ignore_index=True)
        result_df.loc['总计'] = result_df.sum(numeric_only=True)
        result_df[by].iloc[[-1]] = '总计'
        return result_df

    def export(self):
        result = {
            "原始数据": self.order_df,
            "平台达成": self.get_statu_by_platfrom(),
            "部门达成": self.get_statu_by_depart(),
            "个人达成": self.get_statu_by_staff()}
        now = datetime.datetime.now()
        now = now.strftime('%y-%m-%d')
        with pd.ExcelWriter(BASE_DIR + '/output' + '-' + now + '.xlsx') as writer:
            for name, df in result.items():
                df.to_excel(writer, sheet_name=name, index=False)
                print(name + ' export succsss')
        print('all done')


def process_order_df(order_df):
    order_df['店铺'] = order_df['店铺'].str.lower()
    return order_df


class OrderReport:
    def __init__(self, order_df):
        self.order_df = process_order_df(order_df)
        self.today = datetime.datetime.now() - datetime.timedelta(days=2)
        self.total = None

    def get_lastst_date(self):
        order_date = self.order_df.drop_duplicates(subset=['订单时间'])['订单时间']

    def get_plat_cet_list(self):
        series = self.order_df.drop_duplicates(subset=['平台'])['平台']
        return series.tolist()

    def get_store_cet_list(self):
        series = self.order_df.drop_duplicates(subset=['店铺'])['店铺']
        return series.tolist()

    def get_staff_cet_list(self):
        series = self.order_df.drop_duplicates(subset=['负责人'])['负责人']
        return series.tolist()

    def get_by_on_list(self):
        return ['产品', '产品1品牌', '产品1二级类目']

    def get_order_df(self, new=True):
        if new:
            end = self.today.date()
        else:
            end = self.today.date() - datetime.timedelta(days=7)
        start = end - datetime.timedelta(days=6)
        return self.order_df.loc[(self.order_df.index >= datetime.date(year=start.year,
                                               month=start.month,
                                               day=start.day))&
                                 (self.order_df.index <= datetime.date(year=end.year,
                                               month=end.month,
                                               day=end.day))]

    def get_profit_df(self, order_df, col, cet, by_on):
        cols = ['订单时间', '毛利￥', '结算金额￥', '数量']
        ext = [by_on, col]
        cols.extend(ext)
        order_df = order_df[cols]
        profit_df = order_df[order_df[col] == cet]
        profit_result_df = profit_df.groupby(by=by_on, as_index=False).sum()
        profit_result_df['毛利率'] = (profit_result_df['毛利￥'] / profit_result_df['结算金额￥']).fillna(0).apply(
            lambda x: format(x, '.2%'))
        profit_result_df_1 = profit_result_df
        profit_df = order_df
        profit_result_df = profit_df.groupby(by=by_on, as_index=False).mean().round(2)
        profit_result_df_2 = profit_result_df.rename(columns={'毛利￥': '平均毛利￥',
                                                              '结算金额￥': '平均结算金额￥',
                                                              '数量': '平均数量'})
        # 汇总集合
        profit_result_df = profit_result_df_1.merge(profit_result_df_2, on=by_on)
        return profit_result_df

    def get_campare_df(self, col, cet, by_on):
        old_df = self.get_order_df(new=False)
        new_df = self.get_order_df(new=True)
        old = self.get_profit_df(order_df=old_df, col=col, cet=cet, by_on=by_on)
        new = self.get_profit_df(order_df=new_df, col=col, cet=cet, by_on=by_on)
        compare = new.merge(old, on=by_on, how='outer', suffixes=('_近7天', '_上7天'))
        compare['毛利￥_7天增长'] = compare['毛利￥_近7天'].fillna(0).round(2) - compare['毛利￥_上7天'].fillna(0).round(2)
        compare['毛利￥_7天增长率'] = (compare['毛利￥_近7天'] - compare['毛利￥_上7天']) / compare['毛利￥_上7天']
        compare['毛利￥_7天增长率'] = compare['毛利￥_7天增长率'].fillna(0).apply(
            lambda x: format(x, '.2%'))
        compare = compare.fillna(0)
        compare = compare.sort_values(by='毛利￥_7天增长', ascending=False, ignore_index=True)
        compare.loc['总计'] = compare.sum(numeric_only=True).round(2)
        compare[by_on].iloc[[-1]] = '总计'
        return compare

    def get_compare_df_dict(self, col, staff_cet):
        by_on_list = self.get_by_on_list()
        compare_df_dict = {}
        for by_on in by_on_list:
            campare_df = self.get_campare_df(col=col, cet=staff_cet, by_on=by_on)
            compare_df_dict.update({by_on: campare_df})
        return compare_df_dict

    def get_store_profit_df(self, order_df):
        cols = ['订单时间', '毛利￥', '结算金额￥', '数量','平台','店铺']
        profit_df = order_df[cols]
        profit_result_df = profit_df.groupby(by=['店铺'], as_index=False).sum()
        profit_result_df['毛利率'] = (profit_result_df['毛利￥'] / profit_result_df['结算金额￥']).fillna(0).apply(
            lambda x: format(x, '.2%'))
        profit_result_df_1 = profit_result_df
        profit_df = order_df[cols]
        profit_result_df = profit_df.groupby(by='店铺', as_index=False).mean().round(2)
        profit_result_df_2 = profit_result_df.rename(columns={'毛利￥': '平均毛利￥',
                                                              '结算金额￥': '平均结算金额￥',
                                                              '数量': '平均数量'})
        # 汇总集合
        profit_result_df = profit_result_df_1.merge(profit_result_df_2, on='店铺')
        return profit_result_df

    def get_store_compare_df(self):
        old_df = self.get_order_df(new=False)
        new_df = self.get_order_df(new=True)
        old = self.get_store_profit_df(order_df=old_df)
        new = self.get_store_profit_df(order_df=new_df)
        compare = new.merge(old, on='店铺', how='outer', suffixes=('_近7天', '_上7天'))
        compare['毛利￥_7天增长'] = compare['毛利￥_近7天'].fillna(0).round(2) - compare['毛利￥_上7天'].fillna(0).round(2)
        compare['毛利￥_7天增长率'] = (compare['毛利￥_近7天'] - compare['毛利￥_上7天']) / compare['毛利￥_上7天']
        compare['毛利￥_7天增长率'] = compare['毛利￥_7天增长率'].fillna(0).apply(
            lambda x: format(x, '.2%'))
        compare = compare.fillna(0)
        compare = compare.sort_values(by='毛利￥_7天增长', ascending=False, ignore_index=True)
        compare.loc['总计'] = compare.sum(numeric_only=True).round(2)
        compare['店铺'].iloc[[-1]] = '总计'
        return compare

    def get_goods_profit_df(self, order_df):
        cols = ['订单时间', '毛利￥', '结算金额￥', '数量','产品']
        profit_df = order_df[cols]
        profit_result_df = profit_df.groupby(by=['产品'], as_index=False).sum()
        profit_result_df['毛利率'] = (profit_result_df['毛利￥'] / profit_result_df['结算金额￥']).fillna(0).apply(
            lambda x: format(x, '.2%'))
        profit_result_df_1 = profit_result_df
        profit_df = order_df[cols]
        profit_result_df = profit_df.groupby(by='产品', as_index=False).mean().round(2)
        profit_result_df_2 = profit_result_df.rename(columns={'毛利￥': '平均毛利￥',
                                                              '结算金额￥': '平均结算金额￥',
                                                              '数量': '平均数量'})
        # 汇总集合
        profit_result_df = profit_result_df_1.merge(profit_result_df_2, on='产品')
        return profit_result_df

    def get_goods_compare_df(self):
        old_df = self.get_order_df(new=False)
        new_df = self.get_order_df(new=True)
        old = self.get_goods_profit_df(order_df=old_df)
        new = self.get_goods_profit_df(order_df=new_df)
        compare = new.merge(old, on='产品', how='outer', suffixes=('_近7天', '_上7天'))
        compare['毛利￥_7天增长'] = compare['毛利￥_近7天'].fillna(0).round(2) - compare['毛利￥_上7天'].fillna(0).round(2)
        compare['毛利￥_7天增长率'] = (compare['毛利￥_近7天'] - compare['毛利￥_上7天']) / compare['毛利￥_上7天']
        compare['毛利￥_7天增长率'] = compare['毛利￥_7天增长率'].fillna(0).apply(
            lambda x: format(x, '.2%'))
        compare = compare.fillna(0)
        compare = compare.sort_values(by='毛利￥_7天增长', ascending=False, ignore_index=True)
        compare.loc['总计'] = compare.sum(numeric_only=True).round(2)
        compare['产品'].iloc[[-1]] = '总计'
        return compare

    @log
    def export_store(self):
        df = self.get_store_compare_df()
        with pd.ExcelWriter(BASE_DIR + '/output/店铺汇总' + str(self.today.day) + '.xlsx') as writer:
            df.to_excel(writer, sheet_name='汇总', index=False)

    @log
    def export_goods(self):
        df = self.get_goods_compare_df()
        with pd.ExcelWriter(BASE_DIR + '/output/产品汇总' + str(self.today.day) + '.xlsx') as writer:
            df.to_excel(writer, sheet_name='汇总', index=False)

    @log
    def get_result_dict(self):
        col = '店铺'
        staff_cet_list = self.get_store_cet_list()
        result_dict = {}
        for staff_cet in staff_cet_list:
            compare_df_dict = self.get_compare_df_dict(col=col, staff_cet=staff_cet)
            result_dict.update({staff_cet: compare_df_dict})
        return result_dict

    @log
    def export(self):
        result_dict = self.get_result_dict()
        for staff, compare_df_dict in result_dict.items():
            with pd.ExcelWriter(BASE_DIR + '/output/数据-' + staff + '-' + str(self.today.day) + '.xlsx') as writer:
                for sheet, df in compare_df_dict.items():
                    df.to_excel(writer, sheet_name=sheet, index=False)
            print(staff + ' export succsss')

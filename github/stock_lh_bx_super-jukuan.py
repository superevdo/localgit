# ���뺯����

# �ۿ��ڲ�API
from kuanke.wizard import *
from jqdata import *
from jqfactor import Factor
from jqlib.optimizer import *

from jqlib.technical_analysis import *
# Pythonͨ�ð�
import operator
import datetime
import talib
import numpy as np
import pandas as pd
from six import BytesIO
import math as mathe

"""
    ȫ�ֱ���
    g.tracklist       # ������ٵĹ�Ʊ�б�
    g.preorderlist    # ����ƻ������Ʊ�б�
    g.selllist        # ����ƻ������Ĺ�Ʊ
    g.daybuy_stock    #���������Ʊ�б�
    g.mon_buy_list    #�¶ȹ�Ʊ��
    g.stock_num       #����λ��
    g.pre_stock_num   #Ԥѡʱ��λ��
    g.super_stock     #���羫ѡ

"""
g_strategy_name = '��ͣ������'
g_log_path = 'log/%s.log' % g_strategy_name
g_signal_path = '%s.csv' % g_strategy_name


# ����ȫ�ֱ���
# �ջص������趨
# �ص�����С��20%
# �ص�ʱ��С��3��

# ��ʼ���������趨��׼�ȵ�
def initialize(context):
    write_file(g_log_path, '', append=False)
    write_file(g_signal_path, '����,��Ʊ����,��������,��������\n', append=False)

    # ���ֲ�������  ������趨ֱ��Ӱ�쵽���漰���س�  ��Խ�󣬻س���ԽС��������Ҳ��С
    # ���̼�С��16Ԫ��Ϊѡ��Ŀ��
    g.Max_close = 16
    g.filterstop = True  # ����ѡ�ɷ�Χ�Ĺ�Ʊ�¶ȿ���ǰ���
    # ��������ʱ���趨
    g.daybuy_hour_low = 13  # ������ʱ���趨��ʼʱ��
    g.daybuy_hour_high = 14  # ������ʱ���趨����ʱ��
    g.daybuy_min_high = 50  # ������ʱ���趨��������
    # ������ʱ���趨
    g.daysell_hour = 9  # ������ʱ���趨Сʱ
    g.daysell_min = 56  # ������ʱ���趨����

    # ���ֲ�������  ������趨ֱ��Ӱ�쵽���漰���س���Խ�󣬻س���ԽС��������Ҳ��С
    g.stock_num = 3
    # Ԥѡʱ��λ��
    g.pre_stock_num = 6

    g.momentum_day = 29  # ���¶����ο����momentum_day��
    # rsrs��ʱ����
    g.ref_stock = '000300.XSHG'  # ��ref_stock����ʱ����Ļ�������
    g.N = 18  # ��������б��slope����϶�r2�ο����N��
    g.M = 600  # �������±�׼��zscore��rsrs_score�ο����M��
    g.score_threshold = 0.7  # rsrs��׼��ָ����ֵ
    # ma��ʱ����
    g.mean_day = 20  # �������ma���̼ۣ��ο����mean_day
    g.mean_diff_day = 3  # �����ʼma���̼ۣ��ο�(mean_day + mean_diff_day)��ǰ������Ϊmean_diff_day��һ��ʱ��
    g.slope_series = initial_slope_series()[:-1]  # ��ȥ�ز��һ���slope����������ʱ�ظ�����
    g.stock_new = []  # ���ļ��ж�ȡ���ص��ע��stock
    g.industry_new = []  # ���ļ��ж�ȡ���ص��ע����ҵ

    # ����δ������
    # set_option("avoid_future_data", True)
    # �趨����300��Ϊ��׼
    set_benchmark('000300.XSHG')
    # ������̬��Ȩģʽ(��ʵ�۸�)
    set_option('use_real_price', True)
    log.info('��ʼ������ʼ������ȫ��ֻ����һ��')
    # ���˵�orderϵ��API�����ı�error����͵�log
    log.set_level('order', 'error')

    # ��Ʊɸѡ�����ʼ������
    check_stocks_sort_initialize()
    ### ��Ʊ����趨 ###
    body = read_file("stock/stock_basic0919.csv")
    basic_data = pd.read_csv(BytesIO(body), header=None)
    g.super_stock = np.array(basic_data).tolist()
    # ��Ʊ��ÿ�ʽ���ʱ���������ǣ�����ʱӶ�����֮��������ʱӶ�����֮����ǧ��֮һӡ��˰, ÿ�ʽ���Ӷ����Ϳ�5��Ǯ
    set_order_cost(OrderCost(close_tax=0.001, open_commission=0.0003, close_commission=0.0003, min_commission=5),
                   type='stock')

    ## ���к�����reference_securityΪ����ʱ��Ĳο���ģ�����ı��ֻ���������֣���˴���'000300.XSHG'��'510300.XSHG'��һ���ģ�
    # ÿ�µ�һ�������տ���ǰ9�����У��ع���Ʊ��
    # run_monthly(month_before_market_open, monthday=1, time='9:00', reference_security='000300.XSHG')
    # ÿ�µ�һ�������տ���ǰ9�������:
    # run_monthly(month_market_open, monthday=1, time='9:30', reference_security='000300.XSHG')
    # �տ���ǰ����
    run_daily(before_trading_start, time='9:26', reference_security='000300.XSHG')
    # ����ʱ9 ��36 ����
    # run_daily(check_lose, time='9:36', reference_security='000300.XSHG')
    # ����ʱÿ��������һ��
    # run_daily(day_handle_data, time='every_bar', reference_security='000300.XSHG')
    # ���̺�����
    run_daily(print_trade_info, time='after_close', reference_security='000300.XSHG')


########## ########## ###############################
### 5���տ���ʱ������         			      ##
###################################################
# ÿ����λʱ��(�������ز�,��ÿ�����һ��,���������,��ÿ���ӵ���һ��)����һ��
def handle_data(context, data):
    # data = get_current_data()
    if g.tracklist != []:
        # print("�����Ʊ��������" + str(len(g.tracklist)))
        cash = context.portfolio.available_cash
        # 10000 �Ͳ�����
        if cash > 1000:
            count = decisionOrder(context, g.tracklist, data)
            # if count > 0:
            #     print("�����������" + str(count))
    selllogic(context)  ##�����߼�����
    buying(context, data)  ##����߼�����


########## ########## #########################################
### 5.1���տ���ʱ������:������Ʊ�߼�����    ÿ������13:46     		##
##############################################################
# ===============================================
# �����Ƿ�������Ʊ

def selllogic(context):
    hour = context.current_dt.hour
    minute = context.current_dt.minute

    #  ʱ����趨ֱ��Ӱ�쵽���漰���س�
    if hour == g.daysell_hour and minute == g.daysell_min:
        #    if hour == 14 and minute == 42:
        for sec in g.selllist.copy():
            print(sec)
            lastprice = get_bars(sec, count=1, include_now=False, fields=['low', 'close', 'date'])
            secprice = get_bars(sec, end_dt=context.current_dt, count=1,
                                fields=['date', 'low', 'close', 'high', 'open'], include_now=True)
            openprice = secprice['open'][0]
            lowprice = secprice['low'][0]
            closeprice = secprice['close'][0]
            precloseprice = lastprice['close'][0]
            # β�̼�飬�����Ƿ�����9.6%,����
            if (closeprice - precloseprice) / precloseprice >= 0.09:
                print("�Ƿ�����9% ���첻���� " + sec)
                continue
            if (closeprice - precloseprice) / precloseprice <= 0.03:
                print("��ƽ3%���µ�����첻���� " + sec)
                continue
            print("�������ˣ�" + sec)
            # ���������Ĺؼ�����
            order_target(sec, 0)
            write_log('������:' + ' [ ' + str(sec) + ' ] ' + str(context.current_dt.time()))
            write_signal(str(context.current_dt.time()) + ',' + str(sec) + ',' + 'SELL,0')
            send_message('������:' + '  ' + str(sec) + '  ' + str(context.current_dt.time()))
            del (g.selllist[sec])

        ########## ########## #########################################


### 5.2���տ���ʱ������:�����Ʊ�߼�����    ÿ������10-11��     		##
##############################################################
# ��ͣ������
# ���̼۸��ڵ�ǰ�۲�����
# ���̵�10-11���Ƿ�>5%,û����ͣ��׷������
# �����Ƿ������������
#  decisionOrder ����ʱ��
#  ������ݣ�g.tracklist�����ٹ�Ʊ��track_stocks(n��m)���ɣ���������ݣ�g.daybuy_stock �ƻ�����Ĺ�Ʊ��������������
def decisionOrder(context, tracklistbottom, data):
    if not tracklistbottom:
        return 0
    hour = context.current_dt.hour
    minu = context.current_dt.minute

    # ʱ��ѡ����10 - 11 ��ǰ
    #  ʱ����趨ֱ��Ӱ�쵽���漰���س�
    if hour > g.daybuy_hour_low and hour < g.daybuy_hour_high and minu < g.daybuy_min_high:
        return 0

    count = 0
    tracklist = tracklistbottom
    for bottom in tracklist:
        # print context.current_price(bottom.stock)
        # todo nick �ļ۸���ȷ��һ��
        currentprice = data[bottom.stock].close
        if currentprice == data[bottom.stock].high_limit:
            print("��ͣ��������" + bottom.stock)
            # g.tracklistbottom.remove(bottom)
            continue

        open_price = get_current_data()[bottom.stock].day_open
        if open_price > currentprice:
            print("���̼۸��ڵ�ǰ�۲�������" + bottom.stock)
            continue

        rate = (currentprice - bottom.last_close_price) / bottom.last_close_price

        if (rate < 0.04):
            print("���̼�{}-{}���Ƿ�����4%��������{}".format(g.daybuy_hour_low, g.daybuy_hour_high, str(bottom.stock)))
            continue

        # ���̵�10-11���Ƿ�>5%,û����ͣ��׷������

        g.preorderlist.append(bottom)
        tracklistbottom.remove(bottom)
        count = count + 1
    return count


########## ########## #########################################
### 5.3���տ���ʱ������:�Ƿ����������    �������� 13:46     		##
##############################################################
# ====================================================
# �����Ƿ���
def buying(context, data):
    #  ʱ����趨ֱ��Ӱ�쵽���漰���س�
    if context.current_dt.hour > g.daybuy_hour_low and context.current_dt.hour < g.daybuy_hour_high and context.current_dt.minute < g.daybuy_min_high:
        return
    # �ȱ���1.2�����ܵ�Ʊ
    preorderlist = g.preorderlist
    for item in preorderlist:
        currentprice = data[item.stock].close
        if currentprice < data[item.stock].high_limit:
            print("ȷ������ " + item.stock + "===========" + str(
                context.current_dt) + " " + str(currentprice))
            buy(context, item.stock)
            g.preorderlist.remove(item)
            return


########## ########## #########################################
### 5.4���տ���ʱ������:������    �������� 13:46                   		##
##############################################################
def buy(context, stock):
    if stock in context.portfolio.positions:
        print("�Ѿ������Ʊ��" + stock)
        return
    if len(context.portfolio.positions) >= g.stock_num:
        print("��λ����" + stock)
        return
    # g.daybuy_stock  = g.daybuy_stock.append(stock)
    buy_cash = context.portfolio.total_value / g.stock_num
    buy_result = order_target_value(stock, buy_cash)
    if buy_result is None:
        log.info('�ʽ��˻���������{}���辡�첹���ʽ����������ӣ�����{}'.format(stock, str(context.current_dt.time())))
        send_message('�ʽ��˻���������{}���辡�첹���ʽ����������ӣ�����{}'.format(stock, str(context.current_dt.time())))
        write_log('�ʽ��˻���������{}���辡�첹���ʽ����������ӣ�����{}'.format(stock, str(context.current_dt.time())))
    else:
        log.info('������:' + ' [ ' + str(stock) + ' ] ' + str(buy_cash))
        write_log('������:' + ' [ ' + str(stock) + ' ] ' + str(buy_cash))
        write_signal(str(context.current_dt.time()) + ',' + str(stock) + ',' + 'BUY,' + str(buy_cash))
        send_message('������:' + str(stock) + '  ' + str(buy_cash) + '  ' + str(context.current_dt.time()))


# 5.6 ��ʱģ��-�����ۺ��ź�
# 1.���rsrs��MA�ź�,rsrs�ź��㷨�ο��Ż�˵����MA�ź�Ϊһ��ʱ�������˵��MA��ֵ�Ƚϴ�С
# 2.�ź�ͬʱΪTrueʱ���������źţ�ͬΪFalseʱ���������źţ�����������سֲֲ����ź�
def get_timing_signal(stock):
    # ����MA�ź�
    close_data = attribute_history(stock, g.mean_day + g.mean_diff_day, '1d', ['close'])
    today_MA = close_data.close[g.mean_diff_day:].mean()
    before_MA = close_data.close[:-g.mean_diff_day].mean()
    # ����rsrs�ź�
    high_low_data = attribute_history(stock, g.N, '1d', ['high', 'low'])
    intercept, slope, r2 = get_ols(high_low_data.low, high_low_data.high)
    g.slope_series.append(slope)
    rsrs_score = get_zscore(g.slope_series[-g.M:]) * r2
    # �ۺ��ж������ź�
    if rsrs_score > g.score_threshold and today_MA > before_MA:
        #     print('BUY')
        return "BUY"
    elif rsrs_score < -g.score_threshold and today_MA < before_MA:
        #     print('SELL')
        return "SELL"
    else:
        #     print('KEEP')
        return "KEEP"


########## ########## #########################################
### 5.5���տ���ʱ������:����ҵ���ٹ�Ʊ����  �������� 13:46                   		##
##############################################################
#  �����С�����̫Զȡ���۲����
#  �ص�С��20%��������ȡ���۲����
# �ص�ʱ��С��3�죬������ȡ���۲�
# û�г�����ƣ�ȡ���۲�
# ���߹��࣬ȡ���۲�
# ����첻����Ҫ��ȡ���۲�
# =========================================================================
#  m����ͣ�������ڵ�n��stock����Ϊz
def track_stocks_indus(context, stock_list, n, m, z):
    # print("�ո�����ҵ��Ʊ������" + str(context.current_dt) + "===========")
    ztlist = []  # ������������ͣ�б�
    tracklist = []  # ���ø��ٹ�Ʊ�б�
    finalbuylist = []
    finalbuylistobject = {}
    for sec in stock_list:
        count = 0
        historys = attribute_history(sec, count=m, unit='1d',
                                     fields=['close', 'pre_close', 'high', 'low', 'open', 'high_limit'],
                                     df=False)

        close = historys['close'][-1]
        last_data_close = historys['pre_close'][-1]
        # �����Ƿ�<3%
        if (close - last_data_close) / last_data_close < 0.03:
            continue

        # RSRS��ʱ�ж�����ȡ��ʱ�ź�
        # timing_signal = get_timing_signal(sec)
        # print('������ʱ�ź�:{}({})'.format(sec, timing_signal))
        # # ��ʱ�����ж�
        # if timing_signal == 'SELL' :
        #     continue

        # �Ƿ���������ͣ
        haslianxu = False
        islastzt = False
        lianxuid = 0
        isok = False
        alllen = m
        for i in range(m - 1, 0, -1):
            # todo ��������Ƿ���Ч��isnan
            limit = historys['high_limit'][i]
            close = historys['close'][i]
            limit1 = historys['high_limit'][i - 1]
            close1 = historys['close'][i - 1]
            if limit == close and limit1 == close1:
                isok = True
                lianxuid = i
        if not isok:
            continue

        max_id, max_price = max(enumerate(historys['high'][lianxuid:]), key=operator.itemgetter(1))
        min_id, min_price = min(enumerate(historys['low'][lianxuid:]), key=operator.itemgetter(1))
        max_id = max_id + lianxuid
        min_id = min_id + lianxuid
        # �����С�����ԭֵΪ2�����޸�Ϊ3
        # if alllen - min_id >= 2:
        #     print(sec + "�����С�����̫Զ " + str(min_id))
        #     continue
        # �ص�ԭֵΪ0.2
        if (max_price - min_price) / min_price < 0.2:
            print(sec + "�ص�����" + str(max_price) + " " + str(min_price))
            continue
        # �ص�ʱ��ԭֵΪ3�����޸�Ϊ2
        # if alllen - max_id < 3:
        #     print(sec + "�ص�ʱ�䲻��")
        #     continue

        haschonggao = False
        for i in range(max_id + 1, alllen):
            last_data_close = historys['pre_close'][i]
            limit = historys['high_limit'][i]
            close = historys['close'][i]
            high = historys['high'][i]
            if (high - last_data_close) / last_data_close > 0.045:
                haschonggao = True
        if not haschonggao:
            print(sec + " û�г��")
            continue

        # yanxiancount = 0
        # for ix in range(max_id + 1, alllen):
        #     last_data_close = historys['pre_close'][i]
        #     close_today = historys['close'][i]
        #     open_today = historys['open'][i]

        #     if close_today < open_today or close_today < last_data_close:
        #         continue
        #     # �����Ƿ�
        #     day_gain = (close_today - last_data_close) / last_data_close
        #     if day_gain >= 0.052:
        #         hasyanxian = True
        #         yanxiancount = yanxiancount + 1
        #         xianyanid = i
        # if yanxiancount > 1:
        #     print(sec + " ���߹���")
        #     continue

        # isok = False
        for i in range(-1, -3, -1):
            lastopenprice = historys['close'][i]
            lastopenprice = historys['open'][i]
            lasthighprice = historys['high'][i]
            lastlowprice = historys['low'][i]
            lastcloseprice = historys['close'][i]
            lastpreclose = historys['pre_close'][i]
            isyingxian = yingxian(lastopenprice, lastcloseprice, lasthighprice, lastlowprice, lastpreclose)
            if isyingxian:
                isok = True
        if isok:
            bottom = CWBotton()
            bottom.inix(historys['close'][-1], sec)
            finalbuylist.append(sec)
            g.tracklist.append(bottom)
            if len(g.tracklist) > z: break
        else:
            print(sec + " ����첻����Ҫ��")

    # ���δ�ҵ�����ģ���ȡ��������֧��Z
    # print(finalbuylist)
    if finalbuylist == []:
        for sec in stock_list[0:z]:
            bottom = CWBotton()
            bottom.inix(historys['close'][-1], sec)
            finalbuylist.append(sec)
            tracklist.append(bottom)
            # print("��Ʊ:" + str(finalbuylist))
    # g.tracklist=list(set(g.tracklist).intersection(set(g.mon_buy_list)))
    # print("����Ҫ�������" + str(len(g.tracklist)))
    # print(���۲���ٵĹ�Ʊ:",finalbuylist)
    # print("���ռƻ�����Ĺ�Ʊ:" + str(finalbuylist))
    return tracklist


########## ########## #########################################
### 5.5���տ���ʱ������:���ٹ�Ʊ����  �������� 13:46                   		##
##############################################################
#  �����С�����̫Զȡ���۲����
#  �ص�С��20%��������ȡ���۲����
# �ص�ʱ��С��3�죬������ȡ���۲�
# û�г�����ƣ�ȡ���۲�
# ���߹��࣬ȡ���۲�
# ����첻����Ҫ��ȡ���۲�
# =========================================================================
#  m����ͣ�������ڵ�n
def track_stocks(context, n, m):
    print("�ո��ٹ�Ʊ������" + str(context.current_dt) + "===========")

    ztlist = []  # ������������ͣ�б�
    g.tracklist = []  # ���ø��ٹ�Ʊ�б�
    finalbuylist = []
    finalbuylistobject = {}

    for sec in g.check_out_lists:
        count = 0
        historys = attribute_history(sec, count=m, unit='1d',
                                     fields=['close', 'pre_close', 'high', 'low', 'open', 'high_limit'],
                                     df=False)

        close = historys['close'][-1]
        last_data_close = historys['pre_close'][-1]
        # �����Ƿ�<3%
        if (close - last_data_close) / last_data_close < 0.03:
            continue

        # RSRS��ʱ�ж�����ȡ��ʱ�ź�
        # timing_signal = get_timing_signal(sec)
        # print('������ʱ�ź�:{}({})'.format(sec, timing_signal))
        # # ��ʱ�����ж�
        # if timing_signal == 'SELL' :
        #     continue

        # �Ƿ���������ͣ
        haslianxu = False
        islastzt = False
        lianxuid = 0
        isok = False
        alllen = m
        for i in range(m - 1, 0, -1):
            # todo ��������Ƿ���Ч��isnan
            limit = historys['high_limit'][i]
            close = historys['close'][i]
            limit1 = historys['high_limit'][i - 1]
            close1 = historys['close'][i - 1]
            if limit == close and limit1 == close1:
                isok = True
                lianxuid = i
        if not isok:
            continue

        max_id, max_price = max(enumerate(historys['high'][lianxuid:]), key=operator.itemgetter(1))
        min_id, min_price = min(enumerate(historys['low'][lianxuid:]), key=operator.itemgetter(1))
        max_id = max_id + lianxuid
        min_id = min_id + lianxuid
        # �����С�����ԭֵΪ2�����޸�Ϊ3
        # if alllen - min_id >= 2:
        #     print(sec + "�����С�����̫Զ " + str(min_id))
        #     continue
        # �ص�ԭֵΪ0.2
        if (max_price - min_price) / min_price < 0.2:
            print(sec + "�ص�����" + str(max_price) + " " + str(min_price))
            continue
        # �ص�ʱ��ԭֵΪ3�����޸�Ϊ2
        # if alllen - max_id < 3:
        #     print(sec + "�ص�ʱ�䲻��")
        #     continue

        haschonggao = False
        for i in range(max_id + 1, alllen):
            last_data_close = historys['pre_close'][i]
            limit = historys['high_limit'][i]
            close = historys['close'][i]
            high = historys['high'][i]
            if (high - last_data_close) / last_data_close > 0.045:
                haschonggao = True
        if not haschonggao:
            print(sec + " û�г��")
            continue

        # yanxiancount = 0
        # for ix in range(max_id + 1, alllen):
        #     last_data_close = historys['pre_close'][i]
        #     close_today = historys['close'][i]
        #     open_today = historys['open'][i]

        #     if close_today < open_today or close_today < last_data_close:
        #         continue
        #     # �����Ƿ�
        #     day_gain = (close_today - last_data_close) / last_data_close
        #     if day_gain >= 0.052:
        #         hasyanxian = True
        #         yanxiancount = yanxiancount + 1
        #         xianyanid = i
        # if yanxiancount > 1:
        #     print(sec + " ���߹���")
        #     continue

        # isok = False
        for i in range(-1, -3, -1):
            lastopenprice = historys['close'][i]
            lastopenprice = historys['open'][i]
            lasthighprice = historys['high'][i]
            lastlowprice = historys['low'][i]
            lastcloseprice = historys['close'][i]
            lastpreclose = historys['pre_close'][i]
            isyingxian = yingxian(lastopenprice, lastcloseprice, lasthighprice, lastlowprice, lastpreclose)

            if isyingxian:
                isok = True
                # break
        if isok:
            bottom = CWBotton()
            bottom.inix(historys['close'][-1], sec)
            finalbuylist.append(sec)
            g.tracklist.append(bottom)
            if len(g.tracklist) > g.pre_stock_num: break
        else:
            print(sec + " ����첻����Ҫ��")

    # ���δ�ҵ�����ģ���ȡ��������֧��g.pre_stock_num
    if finalbuylist == []:
        for sec in g.check_out_lists[0:g.pre_stock_num]:
            bottom = CWBotton()
            bottom.inix(historys['close'][-1], sec)
            finalbuylist.append(sec)
            g.tracklist.append(bottom)
    # ��Ԥ���ٹ۲�Ĺ�Ʊ���³��趨�Ĺ�Ʊ���󽻼�
    # g.tracklist=list(set(g.tracklist).intersection(set(g.mon_buy_list)))
    # print("����Ҫ�������" + str(len(g.tracklist)))
    # print(���۲���ٵĹ�Ʊ:",finalbuylist)
    # print("���ռƻ�����Ĺ�Ʊ:" + str(finalbuylist))
    return g.tracklist


# ====================================================
class CWBotton:
    def inix(self, last_close_price, stock):
        self.last_close_price = last_close_price
        self.stock = stock


########## ########## #########################################
### 5.5.1���տ���ʱ������:ʵ������ߺ���                              		##
##############################################################
# �Ƿ���ʵ������ߣ���������4%��,ʵ�����3%
def yingxian(open, close, high, low, preclose):
    if close > open or close > preclose:
        return False
    # ����С��4%
    if (preclose - close) / preclose < 0.04:
        return False

    return True


########## ########## #########################################
### 5.5.2���տ���ʱ������:��Ӱ�ߺ���                              		       ##
##############################################################
# ��Ӱ�ߴ���2%
def shangyingxian(open, close, high, low):
    if (high - max(open, close)) / max(open, close) > 0.02:
        return True
    return False


########## ########## #########################################
### 5.5.3���տ���ʱ������:��Ӱ�ߺ���                              		       ##
##############################################################
# �ж��Ƿ���T��
# ��Ӱ�ߴ���ʵ��1.2������Ӱ��С�ڵ���ʵ��
def Txian(open, close, high, low):
    # 0.001���쳣����0�����
    shiti = round(max(abs(open - close), 0.001), 3)
    shangyin = round(max(abs(high - max(close, open)), 0.001), 3)
    xiaying = round(max(abs(min(open, close) - low), 0.001), 3)
    # ��Ӱ�߲���̫���ο�600800,�������
    if ((high - low) / open) > 0.9:
        print("�������")
        return False
    if xiaying / shiti >= 1.9 and xiaying / shangyin >= 2:
        return True
    return False


##############################################################################################
#                                �Զ��庯����                  ##################################
########################################################################################

######   1��    ÿ�µ�һ�������տ���ǰ9�����к��������¹����¶ȹ�Ʊ��        ###########
########################################################################################
def month_before_market_open(context):
    print('�¶ȵ������ڣ�%s' % context.current_dt.date())
    # �������ʱ��
    write_log('�¶ȿ���ǰ����(mon_before_market_open)��' + str(context.current_dt.time()))

    # ��΢�ŷ�����Ϣ�����ģ�⽻�ף�����΢����Ч��
    # send_message('���õ�һ��~')

    # ѡ����֤50�ɷֹɵ�һ������ѡ����ETF����������,���ɹ�Ʊ�ء�
    ### �滻ָ����Ʊ��

    # ����֤ȯ������������+���缨�Ź� + �����ʽ�ɣ�
    #     1���ռ۸��Ƿ�ƫ��ֵ��7%
    #     2���ջ����ʴﵽ20%
    #     3���ռ۸�����ﵽ15%
    #     4�����������������ڣ��Ƿ�ƫ��ֵ�ۼƴﵽ20%
    #     ÿ��������ѡǰ3�����ϰ������Ƿ����塢��С�塢��ҵ��ֱ�ȡǰ3
    #
    mon_buy_list = []
    mon_list = list(mon_check_stocks(context) + g.super_stock + bx_check_stocks(context))
    # ȥ���ظ���Ʊ
    for i in mon_list:
        if i not in mon_buy_list:
            mon_buy_list.append(i)
    # �ؼ�ָ���ж�ϵ�����
    g.mon_buy_list = get_rank_new(mon_buy_list)
    # print('����new:{}'.format(g.mon_buy_list))
    # print("�³������أ�" +str(g.mon_buy_list))


########################################################################################

######   2��    ÿ�µ�һ�������տ���ʱ9������к�����1������������ʷ��Ʊ
##                                                     2)  �������Ȩ��
##                                                     3��ִ�е�������                                                                         ###########
########################################################################################
## ����ʱ���к���
def month_market_open(context):
    write_log('�¶ȿ�������(mon_market_open)��' + str(context.current_dt.time()))
    # �����ڹ�Ʊ���еĹ�Ʊ����
    if g.filterstop:
        sell_list = set(context.portfolio.positions.keys()) - set(g.mon_buy_list)
        if sell_list != []:
            for stock in sell_list:
                # �õ���Ʊ֮ǰ5���ƽ����
                h = attribute_history(stock, 5, '1d', ('close'))
                close5 = h['close'].mean()
                # �õ���һʱ����Ʊƽ����
                price = h['close'][-1]
                # �õ���ǰ�ʽ����
                cash = context.portfolio.cash
                # �����һʱ���۸�С������ƽ����*0.995�����ҳ��иù�Ʊ������
                if price < close5 * 0.995 and context.portfolio.positions[stock].closeable_amount > 0:
                    # ����������
                    order_target_value(stock, 0)
                    # log.info('�¶ȵ���ȫ������:'+'  '+ str(stock) + '  '+str(context.current_dt.time()))
                    # ��¼�������
                    write_log('�¶ȵ�������:' + ' [ ' + str(stock) + ' ] ' + str(context.current_dt.time()))
                    write_signal(str(context.current_dt.time()) + ',' + str(stock) + ',' + 'SELL,0')
                    send_message('�¶ȵ���ȫ������:' + '  ' + str(stock) + '  ' + str(context.current_dt.time()))

    mon_buy_list = set(g.mon_buy_list) - set(context.portfolio.positions.keys())
    for stock in mon_buy_list:
        h = attribute_history(stock, 5, '1d', ('close'))
        close5 = h['close'].mean()
        # �õ���һʱ����Ʊƽ����
        price = h['close'][-1]
        # �õ���ǰ�ʽ����
        cash = context.portfolio.cash
        # �����һʱ���۸��������ƽ����*1.005���������ֽ�������
        if price > close5 * 1.005 and cash > 0:
            # �������뵥
            buy(context, stock)
            # ��¼�������
            log.info('�¶ȵ���������Ŀ��Ȩ��:' + '  ' + str(stock) + '  ' + str(context.current_dt.time()))
            write_log('�¶ȵ���������Ŀ��Ȩ��:' + '  ' + str(stock) + '  ' + str(context.current_dt.time()))
            send_message('�¶ȵ���������Ŀ��Ȩ��:' + '  ' + str(stock) + '  ' + str(context.current_dt.time()))

    # total_value = context.portfolio.total_value # ��ȡ���ʲ�


########################################################################################

######   3��   �տ���ǰ������                                             ###########
########################################################################################
def before_trading_start(context):
    write_log('����ǰ����ʱ��(before_trading_start)��' + str(context.current_dt.time()))
    log.info('����ǰ����ʱ��(before_trading_start)��' + str(context.current_dt.time()))
    # �������¶�̬��ѡstock������ê����ҵ
    try:
        # ���¹�עstock�嵥
        body = read_file("stock/stock_new.txt")
        if body is not None:
            basic_data = pd.read_csv(BytesIO(body), header=None)
            g.stock_new = np.array(basic_data).tolist()
    except Exception as e:
        # log.error(traceback.format_exc())
        pass
    if g.stock_new != []:
        print('��ע��Ʊ{}'.format(g.stock_new))

    # ���¹�ע��ҵ�嵥
    try:
        body = read_file("stock/industry_new.txt")
        if body is not None:
            basic_data = pd.read_csv(BytesIO(body), header=None)
            g.industry_new = np.array(basic_data).tolist()
            # print(industry_new)
    except Exception as e:
        # log.error(traceback.format_exc())
        pass

    g.check_out_lists = longhu_check_stocks(context) + bx_check_stocks(context) + g.super_stock + g.stock_new
    day_buy_list = []
    for i in g.check_out_lists:
        if i not in day_buy_list and i is not None:
            day_buy_list.append(i)
    # ȥ���ظ���Ʊ
    g.check_out_lists = day_buy_list
    # �������̼۴���16Ԫ
    g.check_out_lists = filter_by_closehigh(g.check_out_lists)

    # ����ҵɸѡ����
    industry_zero_num = 0

    if g.industry_new != []:
        print('�ص���ҵ{}'.format(g.industry_new))
        g.tracklist = []
        temp_check_out_lists = []
        temp_stocknum = mathe.ceil(g.pre_stock_num / len(g.industry_new))
        for industry in g.industry_new:
            temp_check_out_lists = industry_filter(context, g.check_out_lists, industry)
            # print('��ҵ����{}'.format(temp_check_out_lists))
            if temp_check_out_lists == []:
                industry_zero_num += 1
                continue
            else:
                temp_check_out_lists = get_rank_new(temp_check_out_lists)
                g.tracklist += track_stocks_indus(context, temp_check_out_lists, 2, 16, temp_stocknum)
                # print('{}��ҵ��ѡ{}ɸѡ����{}'.format(industry,temp_check_out_lists,temp_stocknum))
        if industry_zero_num > 0:
            g.check_out_lists = get_rank_new(g.check_out_lists)
            g.tracklist += track_stocks_indus(context, g.check_out_lists, 2, 16,
                                              temp_stocknum * industry_zero_num)
    else:
        # �����ּ�
        g.check_out_lists = get_rank_new(g.check_out_lists)
        # print('��ѡ{}'.format(g.check_out_lists))
        g.tracklist = track_stocks(context, 2, 16)

    print('�ո��ٹ�Ʊ{}֧��{}'.format(str(len(g.tracklist)), [stock_dict.stock for stock_dict in g.tracklist]))

    g.preorderlist = []
    # print('�ƻ�������Ĺ�Ʊ��' + str(len(g.preorderlist)))
    # ����ƻ�������Ʊ
    g.selllist = {}
    for sec in context.portfolio.positions:
        ##attribute_history ��ȡ��ʷ���ݣ��ɲ�ѯ������Ķ�������ֶΣ��������ݸ�ʽΪ DataFrame �� Dict(�ֵ�)
        ##pre_close: ǰһ����λʱ�����ʱ�ļ۸�close: ʱ��ν���ʱ�۸�
        historys = attribute_history(sec, count=1, unit='1d', fields=['close', 'pre_close'])
        sellitem = {}
        sellitem['pre_close'] = historys['pre_close'][-1]
        sellitem['sec'] = sec
        # print("���󣡣�" + str(sellitem))
        g.selllist[sec] = sellitem
    # �ƻ������Ĺ�Ʊ
    print('�ƻ��������Ĺ�Ʊ������' + str(len(g.selllist)))
    # ����ƻ���������Ĺ�Ʊ��ԭ����Ϊ12����2����ͣ���޸�Ϊ15������2����ͣ��


########################################################################################

######   4.1��   �տ���ǰ������:�Ӻ���     ��ֵС��800��                  ###########
########################################################################################
# �����ص����м���һ�γ�߻���
# ��ֵ С�� 800
def market_cap():
    wholeA = get_fundamentals(query(
        valuation.code
    ).filter(
        valuation.market_cap < 800
    ))
    wholeAList = list(wholeA['code'])
    return wholeAList


########################################################################################

######   4.2��   �տ���ǰ������:�Ӻ���     ���̼���ͣ                      ###########
########################################################################################
# �����ص����м���һ�γ�߻���
####### ���̼���ͣ###############
###���ǹ��˿��̼۵���high_limit��
# high_limit: ʱ����е���ͣ��
def filter_stock_limit(stock_list):
    curr_data = get_current_data()
    for stock in stock_list:
        price = curr_data[stock].day_open
        if (price >= curr_data[stock].high_limit):
            stock_list.remove(stock)
    return stock_list


########################################################################################

######   4.3��   �տ���ǰ������:�Ӻ���    �����¹ɺ�St��                  ###########
########################################################################################
#### ����ST�����¹�   ############�Ż�
def filter_new_and_ST(stock_list, context):
    df = get_all_securities(types=['stock'], date=context.current_dt)
    df = df[(df['start_date'] > (context.current_dt - timedelta(days=100)).date()) | (
        df['display_name'].str.contains("ST")) |
            (df['display_name'].str.contains("��")) | (df['display_name'].str.contains("\*"))]
    return list(set(stock_list).difference(set(df.index)))


# 3-1 ����ģ��-����ͣ�ƹ�Ʊ
# ����ѡ���б������޳�ͣ�ƹ�Ʊ����б�
def filter_paused_stock(stock_list):
    current_data = get_current_data()
    return [stock for stock in stock_list if not current_data[stock].paused]


# 3-2 ����ģ��-����ST�������������б�ǩ�Ĺ�Ʊ
# ����ѡ���б������޳�ST�������������б�ǩ��Ʊ����б�
# def filter_st_stock(stock_list):
# 	current_data = get_current_data()
# 	return [stock for stock in stock_list
# 			if not current_data[stock].is_st
# 			and 'ST' not in current_data[stock].name
# 			and '*' not in current_data[stock].name
# 			and '��' not in current_data[stock].name]

# 3-3 ����ģ��-������ͣ�Ĺ�Ʊ
# ����ѡ���б������޳�δ����������ͣ��Ʊ����б�
def filter_limitup_stock(context, stock_list):
    last_prices = history(1, unit='1m', field='close', security_list=stock_list)
    current_data = get_current_data()
    # �Ѵ����ڳֲֵĹ�Ʊ��ʹ��ͣҲ�����ˣ�����˹�Ʊ�ٴο��򣬵��򱻹��˶�����ѡ���Ĺ�Ʊ
    return [stock for stock in stock_list if stock in context.portfolio.positions.keys()
            or last_prices[stock][-1] < current_data[stock].high_limit]


# 3-4 ����ģ��-���˵�ͣ�Ĺ�Ʊ
# �����Ʊ�б������޳��ѵ�ͣ��Ʊ����б�
def filter_limitdown_stock(context, stock_list):
    last_prices = history(1, unit='1m', field='close', security_list=stock_list)
    current_data = get_current_data()
    return [stock for stock in stock_list if stock in context.portfolio.positions.keys()
            or last_prices[stock][-1] > current_data[stock].low_limit]


########################################################################################

######   4.4��   �տ���ǰ������:�Ӻ���    �������񽻼��Ĺ�Ʊ             ###########
########################################################################################
##########ѡ�ɺ��ĺ���###############################
### ��ֵС��800�ڣ����30�ճ������������У���ͣ�ƣ���ST##
###################################################
def longhu_check_stocks(context):
    check_out_lists = market_cap()  # �޳�����800����ֵ��Ʊ
    check_out_lists = filter_new_and_ST(check_out_lists, context)  # �޳��¹ɺ�ST
    # check_out_list = filter_limitup_stock(context, check_out_list)   #�޳���ͣ��
    check_out_lists = filter_limitdown_stock(context, check_out_lists)  # �޳���ͣ��
    check_out_lists = filter_paused_stock(check_out_lists)  # �޳�ͣ�ƹ�
    ##��ȡָ�����������ڵ�����������   count: ������������ ������ end_date ͬʱʹ�ã� ��ʾ��ȡ end_date ǰ count �������յ�����(�� end_date ����)
    longhu = get_billboard_list(stock_list=check_out_lists, end_date=context.previous_date, count=30)
    check_out_lists = list(set(check_out_lists).intersection(set(longhu["code"])))
    # bx = bx_check_stocks(context)
    # check_out_lists = list(set(check_out_lists).intersection(set(bx["code"])))[-10:]
    return check_out_lists


########################################################################################

######   4.5��   �¿���ǰ������:�Ӻ���    �¶��������񽻼��Ĺ�Ʊ             ###########
########################################################################################
##########ѡ�ɺ��ĺ���###############################
### ��ֵС��500�ڣ����30�ճ������������У���ͣ�ƣ���ST##
###################################################
def mon_check_stocks(context):
    check_out_lists = market_cap()  # �޳�����800����ֵ��Ʊ
    check_out_lists = filter_new_and_ST(check_out_lists, context)  # �޳��¹ɺ�ST
    # check_out_list = filter_limitup_stock(context, check_out_list)   #�޳���ͣ��
    # check_out_list = filter_limitdown_stock(context, check_out_list)   #�޳���ͣ��
    check_out_lists = filter_paused_stock(check_out_lists)  # �޳�ͣ�ƹ�
    ##��ȡָ�����������ڵ�����������   count: ������������ ������ end_date ͬʱʹ�ã� ��ʾ��ȡ end_date ǰ count �������յ�����(�� end_date ����)
    longhu = get_billboard_list(stock_list=check_out_lists, end_date=context.previous_date, count=30)
    check_out_lists = list(set(check_out_lists).intersection(set(longhu["code"])))[-20:]
    # bx=mon_bx_check_stocks(context)
    # print('check_out_lists:{}'.format(check_out_lists))
    # check_out_lists = list(set(bx).intersection(set(check_out_lists["code"])))[-15:]
    return check_out_lists


########################################################################################

######   4.6��   �ձ����ʽ�Ĺ�Ʊ             ###########
########################################################################################
def bx_check_stocks(context):
    df = finance.run_query(query(finance.STK_HK_HOLD_INFO
                                 ).filter(
        finance.STK_HK_HOLD_INFO.link_id == 310001 or finance.STK_HK_HOLD_INFO.link_id == 310002
    ).filter(finance.STK_HK_HOLD_INFO.day == str(context.current_dt.date())
             ).order_by(finance.STK_HK_HOLD_INFO.day.desc()))
    symbol_list = df['code'].tolist()
    price_list = [get_last_price(symbol, None) for symbol in symbol_list]
    df['price'] = price_list
    df['money'] = df['share_number'] * df['price']

    df = df[df.money > 1e8]
    df = df.sort_values('money', ascending=False)

    # ----------+----------+----------+----------+----------+----------+----------+----------+----------+
    df2 = get_fundamentals(query(valuation.day, valuation.code, valuation.market_cap
                                 ).filter(valuation.market_cap < 800))
    df_list = filter_by_finance(context, df2['code'])
    # df = df[df.code.isin(df2['code'].tolist())]
    bx_pool = df_list  # [:int(g.stock_num * 10)]
    bx_pool = list([symbol for symbol in bx_pool if n_day_chg_dayu(symbol, 100, -0.05)])
    return bx_pool


def get_last_price(symbol, fq='post'):
    hst = attribute_history(symbol, 2, '1m', ['close'], fq=fq)
    close_list = [float(x) for x in hst['close']]
    if (math.isnan(close_list[0]) or math.isnan(close_list[-1])):
        return -1.0

    return close_list[-1]


########################################################################################

######   4.7��   �������� �껯������ж�ϵ�������������            ###########
########################################################################################
###################################################
# 1-1 ѡ��ģ��-���������ֶ�
# ���ڹ�Ʊ�껯������ж�ϵ�����,�����շ����Ӵ�С����
def get_rank(stock_pool):
    score_list = []
    for stock in stock_pool:
        data = attribute_history(stock, g.momentum_day, '1d', ['close'])
        y = data['log'] = np.log(data.close)
        x = data['num'] = np.arange(data.log.size)
        slope, intercept = np.polyfit(x, y, 1)
        annualized_returns = math.pow(math.exp(slope), 250) - 1
        r_squared = 1 - (sum((y - (slope * x + intercept)) ** 2) / ((len(y) - 1) * np.var(y, ddof=1)))
        score = annualized_returns * r_squared
        score_list.append(score)
    stock_dict = dict(zip(stock_pool, score_list))
    sort_list = sorted(stock_dict.items(), key=lambda item: item[1], reverse=True)  # TrueΪ����
    code_list = []
    for i in range((len(stock_pool))):
        code_list.append(sort_list[i][0])
    rank_stock = code_list[0:g.pre_stock_num]
    # print(code_list[0:5])
    return rank_stock


########################################################################################

######   4.7��   ѡ����������������ֵ��ë���ʡ��������ۺ���������            ###########
########################################################################################
###################################################
## ��Ʊɸѡ�����ʼ������
def check_stocks_sort_initialize():
    # ������׼�� desc-����asc-����
    g.check_out_lists_ascending = 'desc'


# ��ȡѡ������� input_dict
def get_check_stocks_sort_input_dict():
    input_dict = {
        # valuation.pe_ratio:('desc',0.5),
        valuation.market_cap: ('desc', 1),
        indicator.gross_profit_margin: ('desc', 1),
        income.net_profit: ('desc', 1),
        # finance.STK_EMPLOYEE_INFO.employee:('asc',1),
        # finance.STK_EMPLOYEE_INFO.retirement:('asc',1),
    }
    # ���ؽ��
    return input_dict


# ͨ������ѡ��������Ĺ�˾
def filter_by_finance(context, stock_list):
    q = query(
        indicator
    ).filter(indicator.code.in_(stock_list),
             indicator.adjusted_profit >= 50000000, indicator.roe >= 2, indicator.inc_return >= 2,
             indicator.gross_profit_margin >= 10, indicator.inc_total_revenue_year_on_year >= 1,
             indicator.adjusted_profit_to_profit > 70)
    df = get_fundamentals(q)
    stock_list = df["code"].tolist()
    return stock_list


# ���˸߸�ծ
def filter_by_liabilities(context, stock_list):
    q = query(
        balance
    ).filter(balance.code.in_(stock_list),
             or_(balance.good_will.is_(None), balance.good_will / balance.total_assets <= 0.65),
             balance.total_liability / balance.total_sheet_owner_equities <= 0.50,
             balance.total_owner_equities > balance.total_liability)
    df = get_fundamentals(q)
    stock_list = df["code"].tolist()
    return stock_list


# �����������̼۸���16Ԫ
def filter_by_closehigh(stock_list_new):
    new_list = []
    # print(stock_list_new)
    for sec in stock_list_new:
        try:
            close_price = history(1, '1d', 'close', sec)
            # print(close_price)
            last_close = close_price[sec][0]
            # print("{}{}".format(last_close,sec))
            if last_close <= g.Max_close:
                # print("3{}:{}".format(new_list,stock))
                new_list.append(sec)
                # print("4{}:{}".format(new_list,stock))
        except Exception as e:
            # log.error(traceback.format_exc())
            pass
    return new_list


# �������̼۴���16Ԫ��������ֵ������ë���ʡ�������������
def get_rank_new(stock_pool):
    input_dict = get_check_stocks_sort_input_dict()
    ascending = 'desc'
    # ���� key �� list
    idk = list(input_dict.keys())
    # ���ɾ���
    a = pd.DataFrame()
    for i in idk:
        b = get_sort_dataframe(stock_pool, i, input_dict[i])
        a = pd.concat([a, b], axis=1)
    # ���� score ��
    a['score'] = a.sum(1)
    # ���� score ����
    if ascending == 'asc':  # ����
        if hasattr(a, 'sort'):
            a = a.sort(['score'], ascending=True)
        else:
            a = a.sort_values(['score'], ascending=True)
    elif ascending == 'desc':  # ����
        if hasattr(a, 'sort'):
            a = a.sort(['score'], ascending=False)
        else:
            a = a.sort_values(['score'], ascending=False)
    # ���ؽ��
    rank_stock = list(a.index)[0:g.pre_stock_num]
    # print(code_list[0:5])
    return rank_stock


# 2-1 ��ʱģ��-�������Իع�ͳ��ֵ
# ��������Ա���ÿ����ͼ�x(series)�������ÿ����߼�y(series)����OLS�ع�ģ��,����Ԫ��(�ؾ�,б��,��϶�)
def get_ols(x, y):
    slope, intercept = np.polyfit(x, y, 1)
    r2 = 1 - (sum((y - (slope * x + intercept)) ** 2) / ((len(y) - 1) * np.var(y, ddof=1)))
    return (intercept, slope, r2)


# 2-2 ��ʱģ��-�趨��ʼб������
# ͨ��ǰM�������ͼ۵����Իع�����ʼ��б��,����б�ʵ��б�
def initial_slope_series():
    data = attribute_history(g.ref_stock, g.N + g.M, '1d', ['high', 'low'])
    return [get_ols(data.low[i:i + g.N], data.high[i:i + g.N])[1] for i in range(g.M)]


# 2-3 ��ʱģ��-�����׼��
# ͨ��б���б���㲢���ؽ����ز�����յ����±�׼��
def get_zscore(slope_series):
    mean = np.mean(slope_series)
    std = np.std(slope_series)
    return (slope_series[-1] - mean) / std


########## ########## #########################################
### 6�������̺����к���                                  ##
##############################################################
# ��ӡÿ�ճֲ���Ϣ
def print_trade_info(context):
    log.info(str('��������ʱ��(print_trade_info):' + str(context.current_dt.time())))
    # ��ӡ����ɽ���¼
    trades = get_trades()
    for _trade in trades.values():
        print('�ɽ���¼��' + str(_trade))
    # ��ӡ�˻���Ϣ
    for position in list(context.portfolio.positions.values()):
        securities = position.security
        cost = position.avg_cost
        price = position.price
        ret = 100 * (price / cost - 1)
        value = position.value
        amount = position.total_amount
        print('����:{}'.format(securities))
        print('�ɱ���:{}'.format(format(cost, '.2f')))
        print('�ּ�:{}'.format(price))
        print('������:{}%'.format(format(ret, '.2f')))
        print('�ֲ�(��):{}'.format(amount))
        print('��ֵ:{}'.format(format(value, '.2f')))
        write_log('����:{}'.format(securities))
        write_log('�ɱ���:{}'.format(format(cost, '.2f')))
        write_log('�ּ�:{}'.format(price))
        write_log('������:{}%'.format(format(ret, '.2f')))
        write_log('�ֲ�(��):{}'.format(amount))
        write_log('��ֵ:{}'.format(format(value, '.2f')))
    print('һ�����')
    print('�������������������������������������������������������������������������������շָ��ߡ�������������������������������������������������������������������������������')
    write_log('һ�����')
    write_log('�������������������������������������������������������������������������������շָ��ߡ�������������������������������������������������������������������������������')


# 4-6 ����ģ��-ֹ��
# ���ֲֲ����б�Ҫ��ֹ�����
def check_lose(context):
    for position in list(context.portfolio.positions.values()):
        securities = position.security
        cost = position.avg_cost
        price = position.price
        ret = 100 * (price / cost - 1)
        value = position.value
        amount = position.total_amount
        # �����趨80%ֹ�𼸺���ͬ��ֹ����Ϊֹ����ָ��etf������Ӱ�첻��
        if ret <= -5:
            order_target_value(position.security, 0)
            write_log('ֹ������:' + ' [ ' + str(position.security) + ' ] ' + str(context.current_dt.time()))
            write_signal(str(context.current_dt.time()) + ',' + str(position.security) + ',' + 'STOP,0')
            send_message('ֹ������:' + '  ' + str(position.security) + '  ' + str(context.current_dt.time()))
            print("����������������ֹ���ź�: ���={},��ļ�ֵ={},����ӯ��={}% ������������"
                  .format(securities, format(value, '.2f'), format(ret, '.2f')))


# ----------+----------+----------+----------+----------+
# ��������.д��־
def write_log(text, is_append=True):
    write_file(g_log_path, text + '\n', append=is_append)


def write_signal(text, is_append=True):
    write_file(g_signal_path, text + '\n', append=is_append)


def write_original_signal(text, is_append=True):
    write_file(g_original_signal_path, text + '\n', append=is_append)


def update_last_signal(context, buy_symbol_list):
    text = 'time,symbol,type\n'
    write_file(g_last_signal_path, text, append=False)

    for symbol in buy_symbol_list:
        text = '%s,%s,buy\n' % (datetime_to_string(context.current_dt), symbol)
        write_file(g_last_signal_path, text, append=True)

    # ----------+----------+----------+----------+----------+


# ��������.CSV�ļ���д
def get_remote_file_content(url):
    rsp = urllib.request.urlopen(url)
    text = rsp.read()
    return text


def read_csv(filename):
    try:
        vdata = read_file(filename)
    except:
        return []

    buffer = StringIO()
    if version_info.major < 3:
        buffer.write(vdata)
    else:
        buffer.write(vdata.decode())

    buffer.seek(0)
    return list(DictReader(buffer))


def read_remote_csv(url):
    try:
        vdata = get_remote_file_content(url)
    except:
        return []

    buffer = StringIO()
    if version_info.major < 3:
        buffer.write(vdata)
    else:
        buffer.write(vdata.decode())

    buffer.seek(0)
    return list(DictReader(buffer))


##################################  ������Ⱥ ##################################
## ����Ԫ���ݵ� DataFrame
def get_sort_dataframe(security_list, search, sort_weight):
    if search in ['open', 'close']:
        df = get_price(security_list, fields=search, count=1).iloc[:, 0]
        if sort_weight[0] == 'asc':  # ����
            df = df.rank(ascending=False, pct=True) * sort_weight[1]
        elif sort_weight[0] == 'desc':  # ����
            df = df.rank(ascending=True, pct=True) * sort_weight[1]
    else:
        # ���ɲ�ѯ����
        q = query(valuation.code, search).filter(valuation.code.in_(security_list))
        # ���ɹ�Ʊ�б�
        df = get_fundamentals(q)
        df.set_index(['code'], inplace=True)
        if sort_weight[0] == 'asc':  # ����
            df = df.rank(ascending=False, pct=True) * sort_weight[1]
        elif sort_weight[0] == 'desc':  # ����
            df = df.rank(ascending=True, pct=True) * sort_weight[1]
    return df


# ��ȡN���Ƿ�
def get_n_day_chg(security, n, include_now=False):
    try:
        security_data = get_bars(security, n + 1, '1d', 'close', include_now)
        chg = (security_data['close'][-1] / security_data['close'][0]) - 1
        return chg
    except Exception as e:
        log.error(traceback.format_exc())


# ��ҵ����
def industry_filter(context, security_list, industry_list):
    if len(industry_list) == 0:
        # ���ع�Ʊ�б�
        return security_list
    else:
        securities = []
        for s in industry_list:
            temp_securities = get_industry_stocks(str(s))
            securities += temp_securities
        security_list = [stock for stock in security_list if stock in securities]
        # ���ع�Ʊ�б�
        return security_list


# �������
def concept_filter(context, security_list, concept_list):
    if len(concept_list) == 0:
        return security_list
    else:
        securities = []
        for s in concept_list:
            temp_securities = get_concept_stocks(s)
            securities += temp_securities
        security_list = [stock for stock in security_list if stock in securities]
        # ���ع�Ʊ�б�
        return security_list


# �ۺϹ�����
def filter_special(context, stock_list):  # ������������ͣ�ƣ�ST���ƴ����¹�
    curr_data = get_current_data()
    stock_list = [stock for stock in stock_list if stock[0:3] != '688']  # ���˿ƴ���'688'
    stock_list = [stock for stock in stock_list if not curr_data[stock].is_st]
    stock_list = [stock for stock in stock_list if not curr_data[stock].paused]
    stock_list = [stock for stock in stock_list if 'ST' not in curr_data[stock].name]
    stock_list = [stock for stock in stock_list if '*' not in curr_data[stock].name]
    stock_list = [stock for stock in stock_list if '��' not in curr_data[stock].name]
    stock_list = [stock for stock in stock_list if curr_data[stock].day_open > 1]
    stock_list = [stock for stock in stock_list if
                  (context.current_dt.date() - get_security_info(stock).start_date).days > 150]
    # �������̼۴���16Ԫ
    # stock_list = get_rank_new(stock_list)
    return stock_list
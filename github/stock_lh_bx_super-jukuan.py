# 导入函数库

# 聚宽内部API
from kuanke.wizard import *
from jqdata import *
from jqfactor import Factor
from jqlib.optimizer import *

from jqlib.technical_analysis import *
# Python通用包
import operator
import datetime
import talib
import numpy as np
import pandas as pd
from six import BytesIO
import math as mathe

"""
    全局变量
    g.tracklist       # 当天跟踪的股票列表
    g.preorderlist    # 当天计划买入股票列表
    g.selllist        # 当天计划卖出的股票
    g.daybuy_stock    #当天买入股票列表
    g.mon_buy_list    #月度股票池
    g.stock_num       #最大仓位数
    g.pre_stock_num   #预选时仓位数
    g.super_stock     #王哥精选

"""
g_strategy_name = '涨停超短线'
g_log_path = 'log/%s.log' % g_strategy_name
g_signal_path = '%s.csv' % g_strategy_name


# 策略全局变量
# 日回调比例设定
# 回调比例小于20%
# 回调时间小于3天

# 初始化函数，设定基准等等
def initialize(context):
    write_file(g_log_path, '', append=False)
    write_file(g_signal_path, '日期,股票代码,交易类型,交易数量\n', append=False)

    # 最多持仓数量，  具体的设定直接影响到收益及最大回撤  数越大，回撤会越小，但收益也会小
    # 收盘价小于16元作为选股目标
    g.Max_close = 16
    g.filterstop = True  # 不在选股范围的股票月度开盘前清仓
    # 日盘买入时间设定
    g.daybuy_hour_low = 13  # 日买入时间设定开始时间
    g.daybuy_hour_high = 14  # 日买入时间设定结束时间
    g.daybuy_min_high = 50  # 日买入时间设定结束分钟
    # 日卖出时间设定
    g.daysell_hour = 9  # 日卖出时间设定小时
    g.daysell_min = 56  # 日买入时间设定分钟

    # 最多持仓数量，  具体的设定直接影响到收益及最大回撤数越大，回撤会越小，但收益也会小
    g.stock_num = 3
    # 预选时仓位数
    g.pre_stock_num = 6

    g.momentum_day = 29  # 最新动量参考最近momentum_day的
    # rsrs择时参数
    g.ref_stock = '000300.XSHG'  # 用ref_stock做择时计算的基础数据
    g.N = 18  # 计算最新斜率slope，拟合度r2参考最近N天
    g.M = 600  # 计算最新标准分zscore，rsrs_score参考最近M天
    g.score_threshold = 0.7  # rsrs标准分指标阈值
    # ma择时参数
    g.mean_day = 20  # 计算结束ma收盘价，参考最近mean_day
    g.mean_diff_day = 3  # 计算初始ma收盘价，参考(mean_day + mean_diff_day)天前，窗口为mean_diff_day的一段时间
    g.slope_series = initial_slope_series()[:-1]  # 除去回测第一天的slope，避免运行时重复加入
    g.stock_new = []  # 从文件中读取的重点关注的stock
    g.industry_new = []  # 从文件中读取的重点关注的行业

    # 避免未来函数
    # set_option("avoid_future_data", True)
    # 设定沪深300作为基准
    set_benchmark('000300.XSHG')
    # 开启动态复权模式(真实价格)
    set_option('use_real_price', True)
    log.info('初始函数开始运行且全局只运行一次')
    # 过滤掉order系列API产生的比error级别低的log
    log.set_level('order', 'error')

    # 股票筛选排序初始化函数
    check_stocks_sort_initialize()
    ### 股票相关设定 ###
    body = read_file("stock/stock_basic0919.csv")
    basic_data = pd.read_csv(BytesIO(body), header=None)
    g.super_stock = np.array(basic_data).tolist()
    # 股票类每笔交易时的手续费是：买入时佣金万分之三，卖出时佣金万分之三加千分之一印花税, 每笔交易佣金最低扣5块钱
    set_order_cost(OrderCost(close_tax=0.001, open_commission=0.0003, close_commission=0.0003, min_commission=5),
                   type='stock')

    ## 运行函数（reference_security为运行时间的参考标的；传入的标的只做种类区分，因此传入'000300.XSHG'或'510300.XSHG'是一样的）
    # 每月第一个交易日开盘前9点运行：重构股票池
    # run_monthly(month_before_market_open, monthday=1, time='9:00', reference_security='000300.XSHG')
    # 每月第一个交易日开盘前9点半运行:
    # run_monthly(month_market_open, monthday=1, time='9:30', reference_security='000300.XSHG')
    # 日开盘前运行
    run_daily(before_trading_start, time='9:26', reference_security='000300.XSHG')
    # 开盘时9 ：36 运行
    # run_daily(check_lose, time='9:36', reference_security='000300.XSHG')
    # 开盘时每分钟运行一次
    # run_daily(day_handle_data, time='every_bar', reference_security='000300.XSHG')
    # 收盘后运行
    run_daily(print_trade_info, time='after_close', reference_security='000300.XSHG')


########## ########## ###############################
### 5、日开盘时处理函数         			      ##
###################################################
# 每个单位时间(如果按天回测,则每天调用一次,如果按分钟,则每分钟调用一次)调用一次
def handle_data(context, data):
    # data = get_current_data()
    if g.tracklist != []:
        # print("当天股票跟踪数量" + str(len(g.tracklist)))
        cash = context.portfolio.available_cash
        # 10000 就不买了
        if cash > 1000:
            count = decisionOrder(context, g.tracklist, data)
            # if count > 0:
            #     print("可以买的数量" + str(count))
    selllogic(context)  ##卖出逻辑策略
    buying(context, data)  ##买进逻辑策略


########## ########## #########################################
### 5.1、日开盘时处理函数:卖出股票逻辑函数    每天下午13:46     		##
##############################################################
# ===============================================
# 决定是否卖出股票

def selllogic(context):
    hour = context.current_dt.hour
    minute = context.current_dt.minute

    #  时间的设定直接影响到收益及最大回撤
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
            # 尾盘检查，当日涨幅不到9.6%,卖出
            if (closeprice - precloseprice) / precloseprice >= 0.09:
                print("涨幅超过9% 今天不卖了 " + sec)
                continue
            if (closeprice - precloseprice) / precloseprice <= 0.03:
                print("持平3%或下跌则今天不卖了 " + sec)
                continue
            print("卖出好运：" + sec)
            # 卖出动作的关键分析
            order_target(sec, 0)
            write_log('日卖出:' + ' [ ' + str(sec) + ' ] ' + str(context.current_dt.time()))
            write_signal(str(context.current_dt.time()) + ',' + str(sec) + ',' + 'SELL,0')
            send_message('日卖出:' + '  ' + str(sec) + '  ' + str(context.current_dt.time()))
            del (g.selllist[sec])

        ########## ########## #########################################


### 5.2、日开盘时处理函数:买入股票逻辑函数    每天上午10-11点     		##
##############################################################
# 涨停不买入
# 开盘价高于当前价不买入
# 开盘到10-11点涨幅>5%,没有涨停，追高买入
# 决定是否购买和评分排行
#  decisionOrder 决策时序
#  入参数据：g.tracklist（跟踪股票由track_stocks(n，m)生成），结果数据：g.daybuy_stock 计划买进的股票（已满足条件）
def decisionOrder(context, tracklistbottom, data):
    if not tracklistbottom:
        return 0
    hour = context.current_dt.hour
    minu = context.current_dt.minute

    # 时间选定，10 - 11 点前
    #  时间的设定直接影响到收益及最大回撤
    if hour > g.daybuy_hour_low and hour < g.daybuy_hour_high and minu < g.daybuy_min_high:
        return 0

    count = 0
    tracklist = tracklistbottom
    for bottom in tracklist:
        # print context.current_price(bottom.stock)
        # todo nick 的价格在确定一下
        currentprice = data[bottom.stock].close
        if currentprice == data[bottom.stock].high_limit:
            print("涨停不予买入" + bottom.stock)
            # g.tracklistbottom.remove(bottom)
            continue

        open_price = get_current_data()[bottom.stock].day_open
        if open_price > currentprice:
            print("开盘价高于当前价不予买入" + bottom.stock)
            continue

        rate = (currentprice - bottom.last_close_price) / bottom.last_close_price

        if (rate < 0.04):
            print("开盘价{}-{}点涨幅低于4%不予买入{}".format(g.daybuy_hour_low, g.daybuy_hour_high, str(bottom.stock)))
            continue

        # 开盘到10-11点涨幅>5%,没有涨停，追高买入

        g.preorderlist.append(bottom)
        tracklistbottom.remove(bottom)
        count = count + 1
    return count


########## ########## #########################################
### 5.3、日开盘时处理函数:是否决定购买函数    截至下午 13:46     		##
##############################################################
# ====================================================
# 决定是否购买
def buying(context, data):
    #  时间的设定直接影响到收益及最大回撤
    if context.current_dt.hour > g.daybuy_hour_low and context.current_dt.hour < g.daybuy_hour_high and context.current_dt.minute < g.daybuy_min_high:
        return
    # 先遍历1.2倍动能的票
    preorderlist = g.preorderlist
    for item in preorderlist:
        currentprice = data[item.stock].close
        if currentprice < data[item.stock].high_limit:
            print("确定买入 " + item.stock + "===========" + str(
                context.current_dt) + " " + str(currentprice))
            buy(context, item.stock)
            g.preorderlist.remove(item)
            return


########## ########## #########################################
### 5.4、日开盘时处理函数:购买函数    截至下午 13:46                   		##
##############################################################
def buy(context, stock):
    if stock in context.portfolio.positions:
        print("已经有这个票了" + stock)
        return
    if len(context.portfolio.positions) >= g.stock_num:
        print("仓位满了" + stock)
        return
    # g.daybuy_stock  = g.daybuy_stock.append(stock)
    buy_cash = context.portfolio.total_value / g.stock_num
    buy_result = order_target_value(stock, buy_cash)
    if buy_result is None:
        log.info('资金账户余额不够买入{}，需尽快补入资金，请引起重视！！！{}'.format(stock, str(context.current_dt.time())))
        send_message('资金账户余额不够买入{}，需尽快补入资金，请引起重视！！！{}'.format(stock, str(context.current_dt.time())))
        write_log('资金账户余额不够买入{}，需尽快补入资金，请引起重视！！！{}'.format(stock, str(context.current_dt.time())))
    else:
        log.info('日买入:' + ' [ ' + str(stock) + ' ] ' + str(buy_cash))
        write_log('日买入:' + ' [ ' + str(stock) + ' ] ' + str(buy_cash))
        write_signal(str(context.current_dt.time()) + ',' + str(stock) + ',' + 'BUY,' + str(buy_cash))
        send_message('日买入:' + str(stock) + '  ' + str(buy_cash) + '  ' + str(context.current_dt.time()))


# 5.6 择时模块-计算综合信号
# 1.获得rsrs与MA信号,rsrs信号算法参考优化说明，MA信号为一段时间两个端点的MA数值比较大小
# 2.信号同时为True时返回买入信号，同为False时返回卖出信号，其余情况返回持仓不变信号
def get_timing_signal(stock):
    # 计算MA信号
    close_data = attribute_history(stock, g.mean_day + g.mean_diff_day, '1d', ['close'])
    today_MA = close_data.close[g.mean_diff_day:].mean()
    before_MA = close_data.close[:-g.mean_diff_day].mean()
    # 计算rsrs信号
    high_low_data = attribute_history(stock, g.N, '1d', ['high', 'low'])
    intercept, slope, r2 = get_ols(high_low_data.low, high_low_data.high)
    g.slope_series.append(slope)
    rsrs_score = get_zscore(g.slope_series[-g.M:]) * r2
    # 综合判断所有信号
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
### 5.5、日开盘时处理函数:多行业跟踪股票函数  截至下午 13:46                   		##
##############################################################
#  最后最小离今天太远取消观察跟踪
#  回调小于20%，不够，取消观察跟踪
# 回调时间小于3天，不够，取消观察
# 没有冲高趋势，取消观察
# 阳线过多，取消观察
# 最后几天不符合要求，取消观察
# =========================================================================
#  m天涨停次数大于等n的stock数量为z
def track_stocks_indus(context, stock_list, n, m, z):
    # print("日跟踪行业股票函数：" + str(context.current_dt) + "===========")
    ztlist = []  # 满足条件的涨停列表
    tracklist = []  # 重置跟踪股票列表
    finalbuylist = []
    finalbuylistobject = {}
    for sec in stock_list:
        count = 0
        historys = attribute_history(sec, count=m, unit='1d',
                                     fields=['close', 'pre_close', 'high', 'low', 'open', 'high_limit'],
                                     df=False)

        close = historys['close'][-1]
        last_data_close = historys['pre_close'][-1]
        # 昨日涨幅<3%
        if (close - last_data_close) / last_data_close < 0.03:
            continue

        # RSRS择时判定，获取择时信号
        # timing_signal = get_timing_signal(sec)
        # print('今日择时信号:{}({})'.format(sec, timing_signal))
        # # 择时交易判定
        # if timing_signal == 'SELL' :
        #     continue

        # 是否有连续涨停
        haslianxu = False
        islastzt = False
        lianxuid = 0
        isok = False
        alllen = m
        for i in range(m - 1, 0, -1):
            # todo 检查数据是否有效，isnan
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
        # 最后最小离今天原值为2，现修改为3
        # if alllen - min_id >= 2:
        #     print(sec + "最后最小离今天太远 " + str(min_id))
        #     continue
        # 回调原值为0.2
        if (max_price - min_price) / min_price < 0.2:
            print(sec + "回调不够" + str(max_price) + " " + str(min_price))
            continue
        # 回调时间原值为3，现修改为2
        # if alllen - max_id < 3:
        #     print(sec + "回调时间不够")
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
            print(sec + " 没有冲高")
            continue

        # yanxiancount = 0
        # for ix in range(max_id + 1, alllen):
        #     last_data_close = historys['pre_close'][i]
        #     close_today = historys['close'][i]
        #     open_today = historys['open'][i]

        #     if close_today < open_today or close_today < last_data_close:
        #         continue
        #     # 日内涨幅
        #     day_gain = (close_today - last_data_close) / last_data_close
        #     if day_gain >= 0.052:
        #         hasyanxian = True
        #         yanxiancount = yanxiancount + 1
        #         xianyanid = i
        # if yanxiancount > 1:
        #     print(sec + " 阳线过多")
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
            print(sec + " 最后几天不符合要求")

    # 如果未找到满足的，则取排序中首支至Z
    # print(finalbuylist)
    if finalbuylist == []:
        for sec in stock_list[0:z]:
            bottom = CWBotton()
            bottom.inix(historys['close'][-1], sec)
            finalbuylist.append(sec)
            tracklist.append(bottom)
            # print("股票:" + str(finalbuylist))
    # g.tracklist=list(set(g.tracklist).intersection(set(g.mon_buy_list)))
    # print("符合要求的数量" + str(len(g.tracklist)))
    # print(“观察跟踪的股票:",finalbuylist)
    # print("当日计划买入的股票:" + str(finalbuylist))
    return tracklist


########## ########## #########################################
### 5.5、日开盘时处理函数:跟踪股票函数  截至下午 13:46                   		##
##############################################################
#  最后最小离今天太远取消观察跟踪
#  回调小于20%，不够，取消观察跟踪
# 回调时间小于3天，不够，取消观察
# 没有冲高趋势，取消观察
# 阳线过多，取消观察
# 最后几天不符合要求，取消观察
# =========================================================================
#  m天涨停次数大于等n
def track_stocks(context, n, m):
    print("日跟踪股票函数：" + str(context.current_dt) + "===========")

    ztlist = []  # 满足条件的涨停列表
    g.tracklist = []  # 重置跟踪股票列表
    finalbuylist = []
    finalbuylistobject = {}

    for sec in g.check_out_lists:
        count = 0
        historys = attribute_history(sec, count=m, unit='1d',
                                     fields=['close', 'pre_close', 'high', 'low', 'open', 'high_limit'],
                                     df=False)

        close = historys['close'][-1]
        last_data_close = historys['pre_close'][-1]
        # 昨日涨幅<3%
        if (close - last_data_close) / last_data_close < 0.03:
            continue

        # RSRS择时判定，获取择时信号
        # timing_signal = get_timing_signal(sec)
        # print('今日择时信号:{}({})'.format(sec, timing_signal))
        # # 择时交易判定
        # if timing_signal == 'SELL' :
        #     continue

        # 是否有连续涨停
        haslianxu = False
        islastzt = False
        lianxuid = 0
        isok = False
        alllen = m
        for i in range(m - 1, 0, -1):
            # todo 检查数据是否有效，isnan
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
        # 最后最小离今天原值为2，现修改为3
        # if alllen - min_id >= 2:
        #     print(sec + "最后最小离今天太远 " + str(min_id))
        #     continue
        # 回调原值为0.2
        if (max_price - min_price) / min_price < 0.2:
            print(sec + "回调不够" + str(max_price) + " " + str(min_price))
            continue
        # 回调时间原值为3，现修改为2
        # if alllen - max_id < 3:
        #     print(sec + "回调时间不够")
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
            print(sec + " 没有冲高")
            continue

        # yanxiancount = 0
        # for ix in range(max_id + 1, alllen):
        #     last_data_close = historys['pre_close'][i]
        #     close_today = historys['close'][i]
        #     open_today = historys['open'][i]

        #     if close_today < open_today or close_today < last_data_close:
        #         continue
        #     # 日内涨幅
        #     day_gain = (close_today - last_data_close) / last_data_close
        #     if day_gain >= 0.052:
        #         hasyanxian = True
        #         yanxiancount = yanxiancount + 1
        #         xianyanid = i
        # if yanxiancount > 1:
        #     print(sec + " 阳线过多")
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
            print(sec + " 最后几天不符合要求")

    # 如果未找到满足的，则取排序中首支至g.pre_stock_num
    if finalbuylist == []:
        for sec in g.check_out_lists[0:g.pre_stock_num]:
            bottom = CWBotton()
            bottom.inix(historys['close'][-1], sec)
            finalbuylist.append(sec)
            g.tracklist.append(bottom)
    # 将预跟踪观察的股票与月初设定的股票池求交集
    # g.tracklist=list(set(g.tracklist).intersection(set(g.mon_buy_list)))
    # print("符合要求的数量" + str(len(g.tracklist)))
    # print(“观察跟踪的股票:",finalbuylist)
    # print("当日计划买入的股票:" + str(finalbuylist))
    return g.tracklist


# ====================================================
class CWBotton:
    def inix(self, last_close_price, stock):
        self.last_close_price = last_close_price
        self.stock = stock


########## ########## #########################################
### 5.5.1、日开盘时处理函数:实体大阴线函数                              		##
##############################################################
# 是否是实体大阴线（跌幅大于4%）,实体大于3%
def yingxian(open, close, high, low, preclose):
    if close > open or close > preclose:
        return False
    # 跌幅小于4%
    if (preclose - close) / preclose < 0.04:
        return False

    return True


########## ########## #########################################
### 5.5.2、日开盘时处理函数:上影线函数                              		       ##
##############################################################
# 上影线大于2%
def shangyingxian(open, close, high, low):
    if (high - max(open, close)) / max(open, close) > 0.02:
        return True
    return False


########## ########## #########################################
### 5.5.3、日开盘时处理函数:下影线函数                              		       ##
##############################################################
# 判断是否是T线
# 下影线大于实体1.2倍，上影线小于等于实体
def Txian(open, close, high, low):
    # 0.001是异常处理0的情况
    shiti = round(max(abs(open - close), 0.001), 3)
    shangyin = round(max(abs(high - max(close, open)), 0.001), 3)
    xiaying = round(max(abs(min(open, close) - low), 0.001), 3)
    # 下影线不能太长参考600800,震幅过大
    if ((high - low) / open) > 0.9:
        print("震幅过大")
        return False
    if xiaying / shiti >= 1.9 and xiaying / shangyin >= 2:
        return True
    return False


##############################################################################################
#                                自定义函数区                  ##################################
########################################################################################

######   1、    每月第一个交易日开盘前9点运行函数：重新构建月度股票池        ###########
########################################################################################
def month_before_market_open(context):
    print('月度调仓日期：%s' % context.current_dt.date())
    # 输出运行时间
    write_log('月度开市前运行(mon_before_market_open)：' + str(context.current_dt.time()))

    # 给微信发送消息（添加模拟交易，并绑定微信生效）
    # send_message('美好的一天~')

    # 选出上证50成分股的一部分与选定的ETF基金进行组合,构成股票池。
    ### 替换指定股票池

    # 沪深证券交易所龙虎榜+王哥绩优股 + 北向资金股：
    #     1、日价格涨幅偏离值±7%
    #     2、日换手率达到20%
    #     3、日价格振幅达到15%
    #     4、连续三个交易日内，涨幅偏离值累计达到20%
    #     每个条件都选前3名的上榜，深市是分主板、中小板、创业板分别取前3
    #
    mon_buy_list = []
    mon_list = list(mon_check_stocks(context) + g.super_stock + bx_check_stocks(context))
    # 去除重复股票
    for i in mon_list:
        if i not in mon_buy_list:
            mon_buy_list.append(i)
    # 关键指标判定系数打分
    g.mon_buy_list = get_rank_new(mon_buy_list)
    # print('代码new:{}'.format(g.mon_buy_list))
    # print("月初调整池：" +str(g.mon_buy_list))


########################################################################################

######   2、    每月第一个交易日开盘时9点半运行函数：1）清理卖出历史股票
##                                                     2)  计算调仓权重
##                                                     3）执行调仓买入                                                                         ###########
########################################################################################
## 开盘时运行函数
def month_market_open(context):
    write_log('月度开盘运行(mon_market_open)：' + str(context.current_dt.time()))
    # 将不在股票池中的股票卖出
    if g.filterstop:
        sell_list = set(context.portfolio.positions.keys()) - set(g.mon_buy_list)
        if sell_list != []:
            for stock in sell_list:
                # 得到股票之前5天的平均价
                h = attribute_history(stock, 5, '1d', ('close'))
                close5 = h['close'].mean()
                # 得到上一时间点股票平均价
                price = h['close'][-1]
                # 得到当前资金余额
                cash = context.portfolio.cash
                # 如果上一时间点价格小于三天平均价*0.995，并且持有该股票，卖出
                if price < close5 * 0.995 and context.portfolio.positions[stock].closeable_amount > 0:
                    # 下入卖出单
                    order_target_value(stock, 0)
                    # log.info('月度调仓全部卖出:'+'  '+ str(stock) + '  '+str(context.current_dt.time()))
                    # 记录这次卖出
                    write_log('月度调仓卖出:' + ' [ ' + str(stock) + ' ] ' + str(context.current_dt.time()))
                    write_signal(str(context.current_dt.time()) + ',' + str(stock) + ',' + 'SELL,0')
                    send_message('月度调仓全部卖出:' + '  ' + str(stock) + '  ' + str(context.current_dt.time()))

    mon_buy_list = set(g.mon_buy_list) - set(context.portfolio.positions.keys())
    for stock in mon_buy_list:
        h = attribute_history(stock, 5, '1d', ('close'))
        close5 = h['close'].mean()
        # 得到上一时间点股票平均价
        price = h['close'][-1]
        # 得到当前资金余额
        cash = context.portfolio.cash
        # 如果上一时间点价格大于三天平均价*1.005，并且有现金余额，买入
        if price > close5 * 1.005 and cash > 0:
            # 下入买入单
            buy(context, stock)
            # 记录这次买入
            log.info('月度调仓买入至目标权重:' + '  ' + str(stock) + '  ' + str(context.current_dt.time()))
            write_log('月度调仓买入至目标权重:' + '  ' + str(stock) + '  ' + str(context.current_dt.time()))
            send_message('月度调仓买入至目标权重:' + '  ' + str(stock) + '  ' + str(context.current_dt.time()))

    # total_value = context.portfolio.total_value # 获取总资产


########################################################################################

######   3、   日开盘前处理函数                                             ###########
########################################################################################
def before_trading_start(context):
    write_log('日盘前运行时间(before_trading_start)：' + str(context.current_dt.time()))
    log.info('日盘前运行时间(before_trading_start)：' + str(context.current_dt.time()))
    # 更新最新动态精选stock和最新锚定行业
    try:
        # 最新关注stock清单
        body = read_file("stock/stock_new.txt")
        if body is not None:
            basic_data = pd.read_csv(BytesIO(body), header=None)
            g.stock_new = np.array(basic_data).tolist()
    except Exception as e:
        # log.error(traceback.format_exc())
        pass
    if g.stock_new != []:
        print('关注股票{}'.format(g.stock_new))

    # 最新关注行业清单
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
    # 去除重复股票
    g.check_out_lists = day_buy_list
    # 过滤收盘价大于16元
    g.check_out_lists = filter_by_closehigh(g.check_out_lists)

    # 分行业筛选排名
    industry_zero_num = 0

    if g.industry_new != []:
        print('重点行业{}'.format(g.industry_new))
        g.tracklist = []
        temp_check_out_lists = []
        temp_stocknum = mathe.ceil(g.pre_stock_num / len(g.industry_new))
        for industry in g.industry_new:
            temp_check_out_lists = industry_filter(context, g.check_out_lists, industry)
            # print('行业过滤{}'.format(temp_check_out_lists))
            if temp_check_out_lists == []:
                industry_zero_num += 1
                continue
            else:
                temp_check_out_lists = get_rank_new(temp_check_out_lists)
                g.tracklist += track_stocks_indus(context, temp_check_out_lists, 2, 16, temp_stocknum)
                # print('{}行业初选{}筛选数量{}'.format(industry,temp_check_out_lists,temp_stocknum))
        if industry_zero_num > 0:
            g.check_out_lists = get_rank_new(g.check_out_lists)
            g.tracklist += track_stocks_indus(context, g.check_out_lists, 2, 16,
                                              temp_stocknum * industry_zero_num)
    else:
        # 排名分级
        g.check_out_lists = get_rank_new(g.check_out_lists)
        # print('初选{}'.format(g.check_out_lists))
        g.tracklist = track_stocks(context, 2, 16)

    print('日跟踪股票{}支：{}'.format(str(len(g.tracklist)), [stock_dict.stock for stock_dict in g.tracklist]))

    g.preorderlist = []
    # print('计划日买入的股票：' + str(len(g.preorderlist)))
    # 今天计划卖出的票
    g.selllist = {}
    for sec in context.portfolio.positions:
        ##attribute_history 获取历史数据，可查询单个标的多个数据字段，返回数据格式为 DataFrame 或 Dict(字典)
        ##pre_close: 前一个单位时间结束时的价格，close: 时间段结束时价格
        historys = attribute_history(sec, count=1, unit='1d', fields=['close', 'pre_close'])
        sellitem = {}
        sellitem['pre_close'] = historys['pre_close'][-1]
        sellitem['sec'] = sec
        # print("错误！！" + str(sellitem))
        g.selllist[sec] = sellitem
    # 计划卖出的股票
    print('计划日卖出的股票数量：' + str(len(g.selllist)))
    # 今天计划跟踪买入的股票（原参数为12天内2次涨停，修改为15天内有2次涨停）


########################################################################################

######   4.1、   日开盘前处理函数:子函数     市值小于800亿                  ###########
########################################################################################
# 连板后回调，中间有一次冲高机会
# 市值 小于 800
def market_cap():
    wholeA = get_fundamentals(query(
        valuation.code
    ).filter(
        valuation.market_cap < 800
    ))
    wholeAList = list(wholeA['code'])
    return wholeAList


########################################################################################

######   4.2、   日开盘前处理函数:子函数     开盘即涨停                      ###########
########################################################################################
# 连板后回调，中间有一次冲高机会
####### 开盘即涨停###############
###这是过滤开盘价等于high_limit的
# high_limit: 时间段中的涨停价
def filter_stock_limit(stock_list):
    curr_data = get_current_data()
    for stock in stock_list:
        price = curr_data[stock].day_open
        if (price >= curr_data[stock].high_limit):
            stock_list.remove(stock)
    return stock_list


########################################################################################

######   4.3、   日开盘前处理函数:子函数    过滤新股和St股                  ###########
########################################################################################
#### 过滤ST股与新股   ############优化
def filter_new_and_ST(stock_list, context):
    df = get_all_securities(types=['stock'], date=context.current_dt)
    df = df[(df['start_date'] > (context.current_dt - timedelta(days=100)).date()) | (
        df['display_name'].str.contains("ST")) |
            (df['display_name'].str.contains("退")) | (df['display_name'].str.contains("\*"))]
    return list(set(stock_list).difference(set(df.index)))


# 3-1 过滤模块-过滤停牌股票
# 输入选股列表，返回剔除停牌股票后的列表
def filter_paused_stock(stock_list):
    current_data = get_current_data()
    return [stock for stock in stock_list if not current_data[stock].paused]


# 3-2 过滤模块-过滤ST及其他具有退市标签的股票
# 输入选股列表，返回剔除ST及其他具有退市标签股票后的列表
# def filter_st_stock(stock_list):
# 	current_data = get_current_data()
# 	return [stock for stock in stock_list
# 			if not current_data[stock].is_st
# 			and 'ST' not in current_data[stock].name
# 			and '*' not in current_data[stock].name
# 			and '退' not in current_data[stock].name]

# 3-3 过滤模块-过滤涨停的股票
# 输入选股列表，返回剔除未持有且已涨停股票后的列表
def filter_limitup_stock(context, stock_list):
    last_prices = history(1, unit='1m', field='close', security_list=stock_list)
    current_data = get_current_data()
    # 已存在于持仓的股票即使涨停也不过滤，避免此股票再次可买，但因被过滤而导致选择别的股票
    return [stock for stock in stock_list if stock in context.portfolio.positions.keys()
            or last_prices[stock][-1] < current_data[stock].high_limit]


# 3-4 过滤模块-过滤跌停的股票
# 输入股票列表，返回剔除已跌停股票后的列表
def filter_limitdown_stock(context, stock_list):
    last_prices = history(1, unit='1m', field='close', security_list=stock_list)
    current_data = get_current_data()
    return [stock for stock in stock_list if stock in context.portfolio.positions.keys()
            or last_prices[stock][-1] > current_data[stock].low_limit]


########################################################################################

######   4.4、   日开盘前处理函数:子函数    与龙虎榜交集的股票             ###########
########################################################################################
##########选股核心函数###############################
### 市值小于800亿，最近30日出现在龙虎榜中，非停牌，非ST##
###################################################
def longhu_check_stocks(context):
    check_out_lists = market_cap()  # 剔除大于800亿市值股票
    check_out_lists = filter_new_and_ST(check_out_lists, context)  # 剔除新股和ST
    # check_out_list = filter_limitup_stock(context, check_out_list)   #剔除涨停股
    check_out_lists = filter_limitdown_stock(context, check_out_lists)  # 剔除跌停股
    check_out_lists = filter_paused_stock(check_out_lists)  # 剔除停牌股
    ##获取指定日期区间内的龙虎榜数据   count: 交易日数量， 可以与 end_date 同时使用， 表示获取 end_date 前 count 个交易日的数据(含 end_date 当日)
    longhu = get_billboard_list(stock_list=check_out_lists, end_date=context.previous_date, count=30)
    check_out_lists = list(set(check_out_lists).intersection(set(longhu["code"])))
    # bx = bx_check_stocks(context)
    # check_out_lists = list(set(check_out_lists).intersection(set(bx["code"])))[-10:]
    return check_out_lists


########################################################################################

######   4.5、   月开盘前处理函数:子函数    月度与龙虎榜交集的股票             ###########
########################################################################################
##########选股核心函数###############################
### 市值小于500亿，最近30日出现在龙虎榜中，非停牌，非ST##
###################################################
def mon_check_stocks(context):
    check_out_lists = market_cap()  # 剔除大于800亿市值股票
    check_out_lists = filter_new_and_ST(check_out_lists, context)  # 剔除新股和ST
    # check_out_list = filter_limitup_stock(context, check_out_list)   #剔除涨停股
    # check_out_list = filter_limitdown_stock(context, check_out_list)   #剔除跌停股
    check_out_lists = filter_paused_stock(check_out_lists)  # 剔除停牌股
    ##获取指定日期区间内的龙虎榜数据   count: 交易日数量， 可以与 end_date 同时使用， 表示获取 end_date 前 count 个交易日的数据(含 end_date 当日)
    longhu = get_billboard_list(stock_list=check_out_lists, end_date=context.previous_date, count=30)
    check_out_lists = list(set(check_out_lists).intersection(set(longhu["code"])))[-20:]
    # bx=mon_bx_check_stocks(context)
    # print('check_out_lists:{}'.format(check_out_lists))
    # check_out_lists = list(set(bx).intersection(set(check_out_lists["code"])))[-15:]
    return check_out_lists


########################################################################################

######   4.6、   日北向资金的股票             ###########
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

######   4.7、   动量因子 年化收益和判定系数打分排名函数            ###########
########################################################################################
###################################################
# 1-1 选股模块-动量因子轮动
# 基于股票年化收益和判定系数打分,并按照分数从大到小排名
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
    sort_list = sorted(stock_dict.items(), key=lambda item: item[1], reverse=True)  # True为降序
    code_list = []
    for i in range((len(stock_pool))):
        code_list.append(sort_list[i][0])
    rank_stock = code_list[0:g.pre_stock_num]
    # print(code_list[0:5])
    return rank_stock


########################################################################################

######   4.7、   选股排序函数（按总市值、毛利率、净利润）综合评分排名            ###########
########################################################################################
###################################################
## 股票筛选排序初始化函数
def check_stocks_sort_initialize():
    # 总排序准则： desc-降序、asc-升序
    g.check_out_lists_ascending = 'desc'


# 获取选股排序的 input_dict
def get_check_stocks_sort_input_dict():
    input_dict = {
        # valuation.pe_ratio:('desc',0.5),
        valuation.market_cap: ('desc', 1),
        indicator.gross_profit_margin: ('desc', 1),
        income.net_profit: ('desc', 1),
        # finance.STK_EMPLOYEE_INFO.employee:('asc',1),
        # finance.STK_EMPLOYEE_INFO.retirement:('asc',1),
    }
    # 返回结果
    return input_dict


# 通过财务选择高增长的公司
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


# 过滤高负债
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


# 过滤昨日收盘价高于16元
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


# 过滤收盘价大于16元并根据市值、销售毛利率、净利润排序打分
def get_rank_new(stock_pool):
    input_dict = get_check_stocks_sort_input_dict()
    ascending = 'desc'
    # 生成 key 的 list
    idk = list(input_dict.keys())
    # 生成矩阵
    a = pd.DataFrame()
    for i in idk:
        b = get_sort_dataframe(stock_pool, i, input_dict[i])
        a = pd.concat([a, b], axis=1)
    # 生成 score 列
    a['score'] = a.sum(1)
    # 根据 score 排序
    if ascending == 'asc':  # 升序
        if hasattr(a, 'sort'):
            a = a.sort(['score'], ascending=True)
        else:
            a = a.sort_values(['score'], ascending=True)
    elif ascending == 'desc':  # 降序
        if hasattr(a, 'sort'):
            a = a.sort(['score'], ascending=False)
        else:
            a = a.sort_values(['score'], ascending=False)
    # 返回结果
    rank_stock = list(a.index)[0:g.pre_stock_num]
    # print(code_list[0:5])
    return rank_stock


# 2-1 择时模块-计算线性回归统计值
# 对输入的自变量每日最低价x(series)和因变量每日最高价y(series)建立OLS回归模型,返回元组(截距,斜率,拟合度)
def get_ols(x, y):
    slope, intercept = np.polyfit(x, y, 1)
    r2 = 1 - (sum((y - (slope * x + intercept)) ** 2) / ((len(y) - 1) * np.var(y, ddof=1)))
    return (intercept, slope, r2)


# 2-2 择时模块-设定初始斜率序列
# 通过前M日最高最低价的线性回归计算初始的斜率,返回斜率的列表
def initial_slope_series():
    data = attribute_history(g.ref_stock, g.N + g.M, '1d', ['high', 'low'])
    return [get_ols(data.low[i:i + g.N], data.high[i:i + g.N])[1] for i in range(g.M)]


# 2-3 择时模块-计算标准分
# 通过斜率列表计算并返回截至回测结束日的最新标准分
def get_zscore(slope_series):
    mean = np.mean(slope_series)
    std = np.std(slope_series)
    return (slope_series[-1] - mean) / std


########## ########## #########################################
### 6、日收盘后运行函数                                  ##
##############################################################
# 打印每日持仓信息
def print_trade_info(context):
    log.info(str('函数运行时间(print_trade_info):' + str(context.current_dt.time())))
    # 打印当天成交记录
    trades = get_trades()
    for _trade in trades.values():
        print('成交记录：' + str(_trade))
    # 打印账户信息
    for position in list(context.portfolio.positions.values()):
        securities = position.security
        cost = position.avg_cost
        price = position.price
        ret = 100 * (price / cost - 1)
        value = position.value
        amount = position.total_amount
        print('代码:{}'.format(securities))
        print('成本价:{}'.format(format(cost, '.2f')))
        print('现价:{}'.format(price))
        print('收益率:{}%'.format(format(ret, '.2f')))
        print('持仓(股):{}'.format(amount))
        print('市值:{}'.format(format(value, '.2f')))
        write_log('代码:{}'.format(securities))
        write_log('成本价:{}'.format(format(cost, '.2f')))
        write_log('现价:{}'.format(price))
        write_log('收益率:{}%'.format(format(ret, '.2f')))
        write_log('持仓(股):{}'.format(amount))
        write_log('市值:{}'.format(format(value, '.2f')))
    print('一天结束')
    print('———————————————————————————————————————日分割线————————————————————————————————————————')
    write_log('一天结束')
    write_log('———————————————————————————————————————日分割线————————————————————————————————————————')


# 4-6 交易模块-止损
# 检查持仓并进行必要的止损操作
def check_lose(context):
    for position in list(context.portfolio.positions.values()):
        securities = position.security
        cost = position.avg_cost
        price = position.price
        ret = 100 * (price / cost - 1)
        value = position.value
        amount = position.total_amount
        # 这里设定80%止损几乎等同不止损，因为止损在指数etf策略中影响不大
        if ret <= -5:
            order_target_value(position.security, 0)
            write_log('止损卖出:' + ' [ ' + str(position.security) + ' ] ' + str(context.current_dt.time()))
            write_signal(str(context.current_dt.time()) + ',' + str(position.security) + ',' + 'STOP,0')
            send_message('止损卖出:' + '  ' + str(position.security) + '  ' + str(context.current_dt.time()))
            print("！！！！！！触发止损信号: 标的={},标的价值={},浮动盈亏={}% ！！！！！！"
                  .format(securities, format(value, '.2f'), format(ret, '.2f')))


# ----------+----------+----------+----------+----------+
# 公共函数.写日志
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


# 公共函数.CSV文件读写
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


##################################  排序函数群 ##################################
## 返回元数据的 DataFrame
def get_sort_dataframe(security_list, search, sort_weight):
    if search in ['open', 'close']:
        df = get_price(security_list, fields=search, count=1).iloc[:, 0]
        if sort_weight[0] == 'asc':  # 升序
            df = df.rank(ascending=False, pct=True) * sort_weight[1]
        elif sort_weight[0] == 'desc':  # 降序
            df = df.rank(ascending=True, pct=True) * sort_weight[1]
    else:
        # 生成查询条件
        q = query(valuation.code, search).filter(valuation.code.in_(security_list))
        # 生成股票列表
        df = get_fundamentals(q)
        df.set_index(['code'], inplace=True)
        if sort_weight[0] == 'asc':  # 升序
            df = df.rank(ascending=False, pct=True) * sort_weight[1]
        elif sort_weight[0] == 'desc':  # 降序
            df = df.rank(ascending=True, pct=True) * sort_weight[1]
    return df


# 获取N日涨幅
def get_n_day_chg(security, n, include_now=False):
    try:
        security_data = get_bars(security, n + 1, '1d', 'close', include_now)
        chg = (security_data['close'][-1] / security_data['close'][0]) - 1
        return chg
    except Exception as e:
        log.error(traceback.format_exc())


# 行业过滤
def industry_filter(context, security_list, industry_list):
    if len(industry_list) == 0:
        # 返回股票列表
        return security_list
    else:
        securities = []
        for s in industry_list:
            temp_securities = get_industry_stocks(str(s))
            securities += temp_securities
        security_list = [stock for stock in security_list if stock in securities]
        # 返回股票列表
        return security_list


# 概念过滤
def concept_filter(context, security_list, concept_list):
    if len(concept_list) == 0:
        return security_list
    else:
        securities = []
        for s in concept_list:
            temp_securities = get_concept_stocks(s)
            securities += temp_securities
        security_list = [stock for stock in security_list if stock in securities]
        # 返回股票列表
        return security_list


# 综合过滤器
def filter_special(context, stock_list):  # 过滤器，过滤停牌，ST，科创，新股
    curr_data = get_current_data()
    stock_list = [stock for stock in stock_list if stock[0:3] != '688']  # 过滤科创板'688'
    stock_list = [stock for stock in stock_list if not curr_data[stock].is_st]
    stock_list = [stock for stock in stock_list if not curr_data[stock].paused]
    stock_list = [stock for stock in stock_list if 'ST' not in curr_data[stock].name]
    stock_list = [stock for stock in stock_list if '*' not in curr_data[stock].name]
    stock_list = [stock for stock in stock_list if '退' not in curr_data[stock].name]
    stock_list = [stock for stock in stock_list if curr_data[stock].day_open > 1]
    stock_list = [stock for stock in stock_list if
                  (context.current_dt.date() - get_security_info(stock).start_date).days > 150]
    # 过滤收盘价大于16元
    # stock_list = get_rank_new(stock_list)
    return stock_list
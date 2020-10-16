import requests
from concurrent import futures
import json
import math
import datetime
import re

import configparser

def readini(path):
    cf=configparser.ConfigParser()

    cf.read(path,encoding='utf-8')

    return cf

cf=readini('./config.ini')

url_host=cf.get('api','api_host')

overviewapiurl_path=cf.get('api','overview_api_url')
overview_api_url=url_host+overviewapiurl_path

userapiurl_path=cf.get('api','user_api_url')
user_api_url=url_host+userapiurl_path

liucunapiurl_path=cf.get('api','liucun_api_url')
liucun_api_url=liucunapiurl_path

channel_analysis_path=cf.get('api','channel_api_url')
channel_analysis_api_url=url_host+channel_analysis_path



def request(url,data=None):

    textdata=requests.get(url,params=data)

    if textdata is not None and textdata.status_code==200:
        return json.loads(textdata.text)

    #请求失败
    return 0

def request_f(data=None):

    url=channel_analysis_api_url + '/channel_trend'
    textdata=requests.get(url,params=data)

    if textdata is not None and textdata.status_code==200:
        return json.loads(textdata.text)

    #请求失败
    return 0

def datechange(type,enddate):
    templist = enddate.split('-')

    date_type=type

    # date转化
    key=''
    if date_type == 'day':
        key = enddate
    elif date_type == 'wtd':
        a = datetime.datetime.strptime(enddate, '%Y-%m-%d')
        key = templist[0] + '-w' + str(a.isocalendar()[1])
    elif date_type == 'mtd':
        key = templist[0] + '-m' + str(int(templist[1]))
    else:
        key = templist[0] + '-q' + str(math.ceil(int(templist[1]) / 3))

    return key

def overviewrequest(module=None,data=None):

    datacopy=dict(data)

    enddate=datacopy['date_str']


    if module=='core_index':    #-指标块
         key = datechange(datacopy['date_type'], enddate)
         datacopy['date_type'] = datacopy['date_type'][0]
         datacopy['date'] = key
         del (datacopy['date_str'])

         finalurl = overview_api_url.format(module,datacopy['tichu_type'])
         datadict = request(finalurl, datacopy)

         if datadict and datadict['code']==200:

            #取list
             datalist=datadict['data']['list']

             overviewinfo={}
             for ele in datalist:
                apiinfo={}
                # 组装为字典，并返回
                subinfolist=ele['sub']

                for subinfo in subinfolist:
                    ename=subinfo['ename']

                    if subinfo['value_ori'] !='--':
                        apiinfo.update({ename: round(float(subinfo['value_ori']),2)})
                    else:
                        apiinfo.update({ename: subinfo['value_ori']})

                    if subinfo['value_tb'] != '--':
                        apiinfo.update({ename+"_lastyear_ratio": round(float(subinfo['value_tb'].strip('%')),2)})
                    else:
                        apiinfo.update({ename + "_lastyear_ratio": subinfo['value_tb']})

                    if subinfo['value_hb'] != '--':
                        apiinfo.update({ename+"_pre_ratio": round(float(subinfo['value_hb'].strip('%')),2)})
                    else:
                        apiinfo.update({ename + "_pre_ratio": subinfo['value_hb']})

                overviewinfo.update(apiinfo)

             return overviewinfo

         return 0

    if module=='xiazuan':

        datacopy['date'] = enddate
        del (datacopy['date_str'])
        datacopy['date_type'] = datacopy['date_type'][0]

        templist = enddate.split('-')

        if datacopy['date_type'] == 'd':
            key = templist[1] + '/' + templist[2]

        elif datacopy['date_type'] == 'w':
            a = datetime.datetime.strptime(enddate, '%Y-%m-%d')
            key = 'W' + str(a.isocalendar()[1])
        elif datacopy['date_type'] == 'm':
            key = str(int(templist[1])) + '月'
        else:
            key = 'Q' + str(math.ceil(int(templist[1]) / 3))


        xiazuan={}

        #依次请求趋势、部门分布、平台分布、用户分布、城市排名

        if datacopy['field_str']=='transRate':

            itemslist=['trend','bd','platform','customer','app']
        else:
            itemslist=['trend', 'bd', 'platform', 'customer', 'city']


        for item in itemslist:
            apiinfo = {}
            finalurl = overview_api_url.format(item,datacopy['tichu_type'])

            datadict = request(finalurl, datacopy)

            if datadict and datadict['code'] == 200:

                if item=='trend':
                    # 取value
                    try:
                        datalist = datadict['data'][0]['values'][-1]

                        if datalist[0]!=key:
                            apiinfo = {}
                        else:
                            apiinfo[key]=round(float(datalist[1]),2)
                    except Exception as e:
                        apiinfo={}

                else:
                    # 取data
                    if item=='app':
                        datalist = datadict['data'][0:10]
                    else:
                        datalist = datadict['data']
                    for ele in datalist:
                        key=ele['name']
                        if ele['value'] is not None and ele['value']!='--':         #如果值为空，跳过该key
                            apiinfo[key]= round(float(ele['value']),2)

            xiazuan[item]=apiinfo


        return xiazuan


def userrequest(module=None,data=None):
    datacopy = dict(data)


    if module=='core_index':

        enddate = datacopy['date_str']
        key = datechange(datacopy['date_type'], enddate)

        datacopy['date'] = key
        del (datacopy['date_str'])

        datacopy['date_type'] = datacopy['date_type'][0]  # 取首字母 d\w\m\q



        finalurl = user_api_url.format(module)
        datadict = request(finalurl, datacopy)

        userinfo = {}

        if datadict and datadict['code'] == 200:

            # 取list
            datalist = datadict['data']['list']

            for ele in datalist:
                apiinfo = {}

                # 组装为字典，并返回
                subinfolist = ele['sub']

                for subinfo in subinfolist:
                    apiinfo[subinfo['ename']] = {'value': re.split('万|亿|元',str(subinfo['value']))[0],
                                                     'value_tb': subinfo['value_tb'], 'value_hb': subinfo['value_hb']}
                userinfo.update(apiinfo)

        if len(userinfo)==0:
            userinfo={'new_uv': {'value': '--', 'value_tb': '0%', 'value_hb': '0%'},
             'new_uv_ratio': {'value': '--', 'value_tb': '0%', 'value_hb': '0%'},
             'register_number': {'value': '--', 'value_tb': '0%', 'value_hb': '0%'},
             'new_create_parent_uv_sd': {'value': '--', 'value_tb': '--', 'value_hb': '--'},
             'new_create_parent_uv_zf': {'value': '--', 'value_tb': '--', 'value_hb': '--'},
             'new_create_parent_uv_ck': {'value': '--', 'value_tb': '--', 'value_hb': '--'},
                      'uv':{'value': '--', 'value_tb': "0%", 'value_hb': "0%"},
             'create_parent_uv_sd': {'value': '--', 'value_tb': '--', 'value_hb': '--'},
             'create_parent_uv_zf': {'value': '--', 'value_tb': '--', 'value_hb': '--'},
             'create_parent_uv_ck': {'value': '--', 'value_tb': '--', 'value_hb': '--'},
            'daycount_ratio_sd': {'value': '--', 'value_tb': '--', 'value_hb': '--'},
            'daycount_ratio_zf': {'value': '--', 'value_tb': '--', 'value_hb': '--'}}

        return userinfo


    if module == 'xiazuan':

        enddate = datacopy['date_str']
        datacopy['date'] = enddate

        del (datacopy['date_str'])

        datacopy['date_type'] = datacopy['date_type'][0]  # 取首字母 d\w\m\q

        templist = enddate.split('-')

        if datacopy['date_type'] == 'd':
            key = templist[1] + '/' + templist[2]

        elif datacopy['date_type'] == 'w':
            a = datetime.datetime.strptime(enddate, '%Y-%m-%d')
            key = 'W' + str(a.isocalendar()[1])
        elif datacopy['date_type'] == 'm':
            key = str(int(templist[1])) + '月'
        else:
            key = 'Q' + str(math.ceil(int(templist[1]) / 3))

        xiazuan = {}

        # 依次请求趋势、平台分布
        for item in ['trend', 'platform']:
            apiinfo = {}

            finalurl = user_api_url.format(item)
            datadict = request(finalurl, datacopy)

            if datadict and datadict['code'] == 200:

                if item == 'trend':
                    # 取value
                    try:
                        datalist = datadict['data'][0]['values'][-1]

                        if datalist[0] != key or datalist[1]=='0' or datalist[1]==0:
                            apiinfo = {}
                        else:
                            apiinfo[key] = round(float(datalist[1]), 2)
                    except Exception as e:
                        apiinfo = {}

                else:
                    # 取data
                    datalist = datadict['data']
                    for ele in datalist:
                        key = ele['name']
                        if ele['value'] is not None:  # 如果值为空，跳过该key
                            apiinfo[key] = round(float(ele['value']), 2)

            xiazuan[item] = apiinfo

        return xiazuan


def liucunrequest(data):
    datacopy=dict(data)

    liucun_api_url.format(data['action'])

    datadict = request(liucun_api_url, datacopy)

    liucun={}
    if datadict and datadict['code'] == 200:

        apiinfo={}
        try:
            datalist = datadict['data']['retention_datas']

        except Exception as e:
            datalist=[]
        if len(datalist)>0:
            for item in datalist:
                if item !={}:
                    key=item['日期']
                    del(item['日期'])
                    apiinfo[key]=item
                    #将字符串转换为数字

                    if apiinfo[key]['人数'] !=0:
                        apiinfo[key]['人数']=re.split('万|亿',str(apiinfo[key]['人数']))[0]

                        # itemkeylist=item.keys()
                        # for key in itemkeylist:
                        #     if item[key]!='0%':         #值为0的跳过
                        #         item[key] = round(float(item[key].strip('%')), 2)
                        #     else:
                        #         del item[key]
                        nitem={key:round(float(value.strip('%')), 2) for key,value in item.items() if value!='0%'}
                        apiinfo[key]=nitem

                    else:
                        apiinfo[key]={}

        liucun.update(apiinfo)


    return liucun



# channeltrendkeydict={ 'all':['all'],
#                      '1应用市场':['1-华为智汇云','2-小米','3-OPPO市场','4-VIVO商店','5-魅族','6-百度','7-360助手',
#                                '8-应用宝（自然）','9-应用宝（付费）','10-UC浏览器（付费）','11-其他','12-App Store'],
#                      '2-快应用渠道':['1-华为快应用','2-OPPO快应用','3-VIVO快应用','4-小米快应用','5-魅族快应用',
#                                         '6-努比亚快应用','7-其他快应用'],
#                      '3-精准投放':['all-全部'],
#                       '4-联盟':['1-手机联盟','2-评论联盟','3-微信联盟'],
#                       '5-分享活动':['1-打卡','2-0元领','3-一分抽奖','4-步数赚钱','5-天天领现金','6-1元砍价','7-读书计划',
#                                 '8-抓娃娃','9-答题领红包','10-当当抽大奖','11-助力免单'],
#                       '6-搜索':['1-SEM','2-免费搜索'],
#                       '7-品专':['1-百度品专','2-搜狗品专','3-神马品专','4-360品专'],
#                       '8-广告':['1-app广告','2-微信小程序广告']
#
#                      }

channeltrendkeydict={
                    'all':{'1':'应用市场','2':'快应用渠道','3':'精准投放','4':'联盟','5':'分享活动','6':'搜索','7':'品专','8':'广告'},
                     '1':{'1':'华为智汇云','2':'小米','3':'OPPO市场','4':'VIVO商店','5':'魅族','6':'百度','7':'360助手',
                               '8':'应用宝（自然）','9':'应用宝（付费）','10':'UC浏览器（付费）','11':'其他','12':'App Store'},
                     '2':{'1':'华为快应用','2':'OPPO快应用','3':'VIVO快应用','4':'小米快应用','5':'魅族快应用',
                                        '6':'努比亚快应用','7':'其他快应用'},
                     '3': {'all':'全部'},
                     '4': {'1':'手机联盟', '2':'评论联盟', '3':'微信联盟'},
                    '5': {'1':'打卡', '2':'0元领', '3':'一分抽奖', '4':'步数赚钱', '5':'天天领现金', '6':'1元砍价', '7':'读书计划',
                     '8':'抓娃娃', '9':'答题领红包', '10':'当当抽大奖', '11':'助力免单'},
                    '6': {'1':'SEM', '2':'免费搜索'},
                    #   '6':{'1':{'1':'百度SEM','2':'神马SEM','3':'360SEM','4':'搜狗SEM'},
                    #                '2':{'1':'搜狗免费搜索','2':'神马免费搜索','3':'360免费搜索','4':'百度免费搜索'}},
                     '7': {'1':'百度品专', '2':'搜狗品专', '3':'神马品专', '4':'360品专'},
                     '8': {'1':'app广告', '2':'微信小程序广告'}
                     }


def trend_analysis_api(data,dwmkey):

    overtrendkey=dwmkey[0]

    trend_analysis={}

    datacopy = dict(data)
    datacopy['date_type']=datacopy['date_type'][0]
    datacopy['date']=dwmkey[1]

    # trend
    trend={}
    channel_analysis_trend_url=channel_analysis_api_url+'/trend'

    datadict = request(channel_analysis_trend_url, datacopy)

    if datadict['data'] is not None :

        targets=datadict['data']['targets']
        for item in targets:
            data=round(float(re.split('万|%',str(item['data']))[0]),2)
            lastyear_ratio=round(float(re.split('%',item['lastyear_ratio'])[0]),2)
            pre_ratio=round(float(re.split('%',item['pre_ratio'])[0]),2)
            trend[item['title']]=(data,lastyear_ratio,pre_ratio)

    trend_analysis['trend']=trend

    #overall_trend
    overall_trend={}
    channel_analysis_overalltrend_url = channel_analysis_api_url + '/overall_trend'

    datacopy['target']='uv_dau'
    datadict = request(channel_analysis_overalltrend_url, datacopy)

    if datadict['data'] is not None :

        targets=datadict['data']['values']
        #取最后一个
        overtrendvalue=targets[-1]
        if overtrendvalue[0]==overtrendkey:
            overall_trend[overtrendkey]=round(float(overtrendvalue[1].strip('%')),2)

    trend_analysis['overall_trend'] = overall_trend


    #渠道趋势
    channel_trend={}
    channel_trend_url= channel_analysis_api_url + '/channel_trend'

    url_data_list=[]
    for firstchannelkey in channeltrendkeydict.keys():

        parent_channel=firstchannelkey

        for secondchannelkey in channeltrendkeydict[firstchannelkey]:
            parent_second_channel=secondchannelkey

            datacopy['parent_channel']=parent_channel
            datacopy['parent_second_channel']=parent_second_channel


            datadict = request(channel_trend_url, datacopy)

            if datadict['data'] is not None :
                targets = datadict['data']['channel_trend']

                if len(targets)!=0:
                    for item in targets:

                        key=list(item.keys())[0]
                        value=item[key][-1]
                        if value[0]==overtrendkey and value[1]!='0%':
                            if not channel_trend.__contains__(firstchannelkey):
                                channel_trend[firstchannelkey]={(key,round(float(value[1].strip('%')),2))}
                            else:
                                channel_trend[firstchannelkey].add((key,round(float(value[1].strip('%')),2)))


    #         url_data_list.append(dict(datacopy))
    #
    #
    # with futures.ThreadPoolExecutor(4) as executor:
    #     res = executor.map(request_f, url_data_list)
    #
    # for datadict in res:
    #     if datadict['data'] is not None :
    #         targets = datadict['data']['channel_trend']
    #
    #         if len(targets)!=0:
    #             for item in targets:
    #
    #                 key=list(item.keys())[0]
    #                 value=item[key][-1]
    #                 if value[0]==overtrendkey:
    #                     if not channel_trend.__contains__(firstchannelkey):
    #                         channel_trend[firstchannelkey]=[(key,round(float(value[1].strip('%')),2))]
    #                     else:
    #                         channel_trend[firstchannelkey]+=[(key,round(float(value[1].strip('%')),2))]


    trend_analysis['channel_trend']=channel_trend

    return trend_analysis


def client_analysis_api(data,dwmkey,keyword):
    form_title=["UV", "UV占比", "同比", "环比", "收订金额", "收订转化率", "收订人数",
                "支付金额", "支付转化率", "支付人数", "出库金额", "出库转化率", "出库人数"]

    overtrendkey = dwmkey[0]

    datacopy = dict(data)
    datacopy['date_type'] = datacopy['date_type'][0]
    datacopy['date'] = dwmkey[1]

    client={}
    client_url=channel_analysis_api_url+'/'+keyword

    datadict = request(client_url, datacopy)

    if datadict['data'] is not None :


        try:
            apititle=datadict['data']['form_title'][0]
            targets=datadict['data']['form_datas']
        except KeyError as e:
            targets=[]
        if len(targets)>0:
            for item in targets:
                tempalist=[]
                for key in form_title:
                    try:
                        tempalist.append((key,round(float(re.split('亿|万|%',str(item[key]))[0]),2)))
                    except Exception as e :
                        pass

                client[item[apititle]] =tempalist


    return client


def channel_site_api(data,dwmkey):






    pass













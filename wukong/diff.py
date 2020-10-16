import api
import sql
from log import log
import threading
import datetime

def channel_trend_diff(apidata,sqldata):

    #依次比较trend,overall_trend,channel_trend

    for key in ['trend','overall_trend','channel_trend']:

            if apidata[key].keys()!=sqldata[key].keys():
                log(key+"子键不同")
                return True

            for subkey in apidata[key].keys():
                if apidata[key][subkey] != sqldata[key][subkey]:
                    log(subkey+"值不同")
                    return True

    return False



def diff(data1,data2):

        #字典比较
        if data1==data2:
            return False
        else:
            if len(data1)!=len(data2):
                log("数据长度不一样")
            diffkey=[]
            diffsubkey=[]
            for key in data1.keys():
                try:
                    if data1[key] !=data2[key]:

                        i=0
                        while i<len(data1[key]):
                            if data1[key][i] !=data2[key][i]:
                                diffsubkey.append((data1[key][i],data2[key][i]))
                            i+=1
                        diffkey+=[str(key) + '=>' + str(diffsubkey)]
                except Exception as e:
                    log('sqldata缺少键')

            log("不同的键值对:"+str(diffkey))
        return True


def valuediff(apidata,sqldata):

    apilen=len(apidata)
    sqllen=len(sqldata)

    if apilen!=sqllen:
        return True

    sqlkeys=sqldata.keys()

    for key in apidata.keys(): # api第一层
        if key not in sqlkeys:
            return True
        if isinstance(apidata[key],str) and isinstance(sqldata[key],str):
            continue
        if  abs(apidata[key] - sqldata[key]) > 0.5:
            return True

    return False

def strvaluediff(title,data,apidata,sqldata):

    for item in apidata.items(): # api第一层

        subitemkey=item[0]              #如 trend,bd,platform,customer,city
        subitemdict=item[1]

        apikeys=subitemdict.keys()
        sqlkeys = sqldata[subitemkey].keys()

        if len(subitemdict) !=len(sqldata[subitemkey]):
            return True

        for key in apikeys:
            if key not in sqlkeys or abs(subitemdict[key] - sqldata[subitemkey][key]) > 0.5:
                print(title)
                print("api:", end='')
                print(apidata[subitemkey])
                print("sql:", end='')
                print(sqldata[subitemkey])
                print(data)
                print('XXXXXXXXXXXXXXXXX---XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX--XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')
                return True

    return False
'''
    if apidata !={} and sqldata!={}:
        for item in apidata.items():
            if item[1]!=sqldata[item[0]]:

                if item[1]=={}:
                    return True

                for keyvalue in item[1].keys():

                    if keyvalue not in sqldata[item[0]].keys() or abs(item[1][keyvalue]-sqldata[item[0]][keyvalue])>0.5:
                            print(title)
                            print("api:",end='')
                            print(apidata[item[0]])
                            print("sql:" ,end='')
                            print(sqldata[item[0]])
                            print(data)
                            print('XXXXXXXXXXXXXXXXX---XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX--XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')
                            return True

    elif apidata =={} and sqldata=={}:
        return False
    else:
        return True
'''

# zuanqu_common_dict = {'create_price': '金额', 'create_parent_amt': '订单量', 'create_order_amt': '包裹数',
#               "priceByParent": "单均价", "priceByPerson": "客单价", "transRate": "用户转化率"}

zuanqu_common_dict = {'create_price': '金额', 'create_parent_amt': '订单量', 'create_order_amt': '包裹数',
               "priceByParent": "单均价", "priceByPerson": "客单价"}

cancel_dict={"cancel_rate":"取消率"}
zuanqu_zf_ck_dict={'out_pay_amount': '实付金额', 'out_profit': '净销售额'}
zuanqudict_ck = { 'gross_profit': '毛利额', 'gross_profit_rate': '毛利率'}

def overview_diff(datetype,date,flag):

    for source in ['1','2','3','4','all']:                 #平台来源      all-all 1主站 2天猫 3抖音 4拼多多
        if source!='1':
            parent_platformlist = ['all']
        else :                                               #点击主站可以下钻APP、轻应用、H5、PC、其他
            parent_platformlist=[ '1', '2', '3','4','5']

        for parent_platform in parent_platformlist:

            if parent_platform == '1':
                platformlist = ['1', '2']                      # android 、 ios

            elif parent_platform == '2':
                platformlist = ['all','3', '4','5','6','7','8','9'] # all、快应用、微信小程序、百度小程序、头条、qq、360

            elif parent_platform == '3' or parent_platform == '4' or parent_platform == '5':
                platformlist=['all']
            else:
                platformlist = ['all']


            for platform in platformlist:

                for bd_id in ['all', '1', '2', '3', '4']:         # 事业部id：all-all 1-出版物事业部 2-日百服 3-数字业务事业部 4-文创

                 for shop_type in ['all', '1', '2']:              # 经营方式 all-ALL 1-自营 2-招商

                     for tichu_type in ['all', '1']:              # 剔除选项 all-all 1-剔除建工

                            for sale_type in ['sd','zf','ck']:               # 收订sd、支付zf、出库ck

                                data = {'source': source, 'parent_platform': parent_platform, 'platform': platform,
                                    'bd_id': bd_id, 'shop_type': shop_type, 'tichu_type': tichu_type,'sale_type':sale_type ,\
                                    'date_type':datetype,'date_str': date}

                                     #overview

                                if flag==1:
                                    jingying_diff(data)
                                else:
                                    jingyingzuanqu_diff(sale_type, data)


userzuanqudict = {'new_uv': '新访UV', 'new_uv_ratio': '新访UV占比', 'register_number': '新增注册用户',
                  "new_create_parent_uv_sd": "新增收订用户", "new_create_parent_uv_zf": "新增支付用户",
                  "new_create_parent_uv_ck": "新增出库用户","uv":'活跃UV','create_parent_uv_sd':"收订用户",
                  "create_parent_uv_zf":"支付用户","create_parent_uv_ck":"出库用户"}

def user_analysis_diff(datetype,date,flag):


    #共遍历4+44+4+4+4=60
    for source in ['all', '1', '2', '3', '4']:                # 下单平台 all-all 1主站 2天猫 3抖音 4拼多多

        if source != '1':
            parent_platformlist = ['all']
        else:                                                 # 点击主站可以下钻APP、小程序、H5、PC、其他
            parent_platformlist = ['1', '2', '3', '4','5']

        for parent_platform in parent_platformlist:

            if parent_platform == '1':
                platformlist = ['1', '2']                    # android 、 ios

            elif parent_platform == '2':
                platformlist = ['all','3', '4', '5', '6', '7','8']           # 微信小程序、百度小程序、头条、支付宝、qq、360

            else:
                platformlist = ['all']

            for platform in platformlist:

                    #core-index
                    data = {'source': source, 'parent_platform': parent_platform, 'platform': platform, 'date_str': date}
                    data['date_type'] = datetype



                    if flag==1:
                        usroverall(data)
                    else:
                        usrzuanqu(data)

def jingying_diff(data):
    log('经营首页')
    # data = {'source': '1', 'parent_platform': 'all', 'platform': 'all',
    #         'bd_id': 'all', 'shop_type': 'all', 'tichu_type': 'all',
    #         'sale_type': 'zf', \
    #         'date_type': 'day', 'date_str': '2020-09-01'}



    apidata = api.overviewrequest('core_index', data)

    sqldata=sql.overviewselect('core_index', data)

    log(apidata)
    log(sqldata)

    if valuediff(sqldata,apidata):

         log('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx-Fail-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')
    pass

def jingyingzuanqu_diff(sd_zf_ck,data):

    zuanqudict=dict(zuanqu_common_dict)
    if sd_zf_ck=="sd":
        name="收订"
        zuanqudict.update(cancel_dict)
    elif sd_zf_ck=='zf':
        name="支付"
        zuanqudict.update(cancel_dict)
        zuanqudict.update(zuanqu_zf_ck_dict)
    else:
        name="出库"
        zuanqudict.update(zuanqu_zf_ck_dict)
        zuanqudict.update(zuanqudict_ck)

    for zuanquitem in zuanqudict.keys():

        log('经营分析-' + name+ zuanqudict[zuanquitem] + '钻取')
        # data = {'source': '1', 'parent_platform': 'all', 'platform': 'all',
        #         'bd_id': 'all', 'shop_type': 'all', 'tichu_type': 'all',
        #         'sale_type': 'zf', \
        #         'date_type': 'day', 'date_str': '2020-09-10'}
        # zuanquitem='out_pay_amount'

        data['field_str'] = zuanquitem


        apidata = api.overviewrequest('xiazuan', data)
        sqldata = sql.overviewselect('xiazuan', data)

        log(apidata)
        log(sqldata)
        if strvaluediff('经营分析-' + zuanqudict[zuanquitem] + '钻取', data, apidata, sqldata):
            log(
                'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx-Fail-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')


def getliucunsql(action,datetype):

    # 数据库判断
    table = 'bi_mdata.mdata_interface_{}_users_return_all_{}'
    if action == 'active':
        table1 = 'visit'

        field = 'uv'
        field_ratio = 'return_uv_ratio'
        subfield = 'data_date-date_str as time '

    else:
        table1 = 'order'

        field = 'cust_num'
        field_ratio = 'return_cust_num_ratio'
        subfield = 'toDate(update_date)-date_str as time'


    sql="select "+field+","+field_ratio+","+subfield +" from "+table.format(table1,datetype)


    return sql

def getdate_str(datetype,datelist):            #取首尾日期的周一

    startday=datetime.datetime.strptime(datelist[0],'%Y-%m-%d')
    endday =datetime.datetime.strptime(datelist[1],'%Y-%m-%d')

    if datetype=='d':               #天

        delta=(endday-startday).days+1    #多少天
        start=datelist[0]

    elif datetype=='w':

        weekday1=startday.strftime("%w")    #周几
        weekday2=endday.strftime("%w")

        start=startday-datetime.timedelta(int(weekday1)-1)
        # end=endday-datetime.timedelta(int(weekday2)-1)

        i=1
        tempdate=start
        while tempdate<endday:
            i+=1
            tempdate+=datetime.timedelta(days=7)

        delta=i
        start=start.strftime('%Y-%m-%d')

    else:                                #多少月

        start=datelist[0]
        end=datelist[1]
        startday=start.split('-')
        endday=end.split('-')

        delta=int(endday[1])-int(startday[1])+1

        start=startday[0]+"-"+startday[1]+'-01'
    return start,delta

    pass




def liucun_diff(date_type,datelist):

    datetype=date_type[0]

    for action in ['active', 'repurchase']:       #活跃留存、复购留存

        sql = getliucunsql(action,datetype)
        date_str=getdate_str(datetype,datelist)

        for source in ['all', '1', '2', '3', '4']:  # 下单平台 all-all 1主站 2天猫 3抖音 4拼多多

            if source != '1':
                parent_platformlist = ['all']
            else:  # 点击主站可以下钻APP、小程序、H5、PC、其他
                parent_platformlist = ['1', '2', '3', '4', '5']

            for parent_platform in parent_platformlist:

                if parent_platform == '1':
                    platformlist = ['1', '2']  # android 、 ios

                elif parent_platform == '2':
                    platformlist = ['all', '3', '4', '5', '6', '7', '8','9']  # 快应用、微信小程序、百度小程序、头条、支付宝、qq、360

                else:
                    platformlist = ['all']

                for platform in platformlist:

                    for new_id in ['all','1','2']:          #所有，1-新，2-老


                        # core-index
                        data = {'source': source, 'parent_platform': parent_platform, 'platform': platform,'action':action,
                                'start_date': datelist[0],'end_date':datelist[1],'user_type':new_id}

                        data['date_type']=datetype

                        liucun(sql,date_str,data)

def liucun(sqlstr,date_str,data):

    # data = {'source': '1', 'parent_platform': '3', 'platform': 'all', 'action': 'active',
    #         'start_date': '2020-09-07', 'end_date': '2020-09-07', 'user_type': 'all'}
    #
    # data['date_type'] = 'w'
    log('留存分析--'+data['action'])
    apidata = api.liucunrequest(data)
    sqldata = sql.liucunselect(sqlstr,date_str,data)

    if not diff(sqldata, apidata):
        log(apidata)
        log(sqldata)


        log(
                'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx-Fail-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')

def usroverall(data):
    # data = {'source': '1', 'parent_platform': '2', 'platform': '4', \
    #         'date_type': 'day', 'date_str': '2020-08-26'}
    # data['field_str'] = 'new_create_parent_uv_sd'

    log('用户分析首页')
    apidata = api.userrequest('core_index', data)
    sqldata = sql.userselect('core_index', data)

    if not diff(sqldata, apidata):


        log(apidata)
        log(sqldata)
        log('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx-Fail-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')


def usrzuanqu(data):
    # 钻取
    for zuanquitem in userzuanqudict.keys():

        log('用户分析-' + userzuanqudict[zuanquitem] + '钻取')

        data['field_str'] = zuanquitem

        # data = {'source': '1', 'parent_platform': '2', 'platform': '6',
        #         'date_str': '2020-09-09'}
        # data['date_type'] = 'day'
        # data['field_str']='uv'



        apidata = api.userrequest('xiazuan', data)
        sqldata = sql.userselect('xiazuan', data)


        log(apidata)
        log(sqldata)
        if strvaluediff('用户分析-' + userzuanqudict[zuanquitem] + '钻取', data, apidata, sqldata):
            log(
                'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx-Fail-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')




def trend_analysis(datetype,date,dwmkey):
    '''趋势分析详情'''
    for source in ['all','1']:                 #平台来源      all-all 1主站
        if source!='1':
            parent_platformlist = ['all']
        else :                                               #点击主站可以下钻APP、轻应用、H5、PC
            parent_platformlist=[ '1', '2', '3','4']

        for parent_platform in parent_platformlist:

            if parent_platform == '1':                         #APP下属类
                platformlist = ['1', '2']                      # android 、 ios

            elif parent_platform == '2':                       #轻应用下属类
                platformlist = ['all','3', '4']                # all、快应用、微信小程序
            else:
                platformlist = ['all']

            for platform in platformlist:

                 for user_type in ['all','1','2']:               # 1-新房客，2-老访客

                      data = {'source': source, 'parent_platform': parent_platform, 'platform': platform,
                                    'user_type':user_type , 'date_type':datetype,'date': date}

                      log('渠道分析-趋势分析')

                      apidata=api.trend_analysis_api(data,dwmkey)
                      sqldata=sql.trend_analysis_sql(data,dwmkey)

                      if channel_trend_diff(apidata,sqldata):
                          log(apidata)
                          log(sqldata)
                          log('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx-Fail-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')


def client_analysis(datetype, date, dwmkey):
    '''客户端分析'''
    for source in ['1']:                 #平台来源      all-all 1主站

        parent_platformlist=['all', '1', '2', '3','4']      #点击主站可以下钻APP、轻应用、H5、PC

        for parent_platform in parent_platformlist:

            if parent_platform == '1':                         #APP下属类
                platformlist = ['1', '2']                      # android 、 ios

            elif parent_platform == '2':                       #轻应用下属类
                platformlist = ['all','3', '4']                # all、快应用、微信小程序
            else:
                platformlist = ['all']

            for platform in platformlist:

                 for user_type in ['all','1','2']:               # 1-新房客，2-老访客

                      data = {'source': source, 'parent_platform': parent_platform, 'platform': platform,
                                    'user_type':user_type , 'date_type':datetype,'date': date,'parent_channel':'all','parent_second_channel':'all','channel':'all'}

                      # data = {'source': '1', 'parent_platform': '1', 'platform': '2',
                      #         'user_type': user_type, 'date_type': datetype, 'date': date, 'parent_channel': 'all',
                      #         'parent_second_channel': 'all', 'channel': 'all'}

                      apidata=api.client_analysis_api(data,dwmkey,'client')
                      sqldata=sql.client_analysis_sql(data,dwmkey)

                      if diff(apidata,sqldata):
                          log(apidata)
                          log(sqldata)
                          log('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx-Fail-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')

channeldict={
                    'all-全部':{'1':'应用市场','2':'快应用渠道','3':'精准投放','4':'联盟','5':'分享活动','6':'搜索','7':'品专','8':'广告'},
                     '1-应用市场':{'1':'华为智汇云','2':'小米','3':'OPPO市场','4':'VIVO商店','5':'魅族','6':'百度','7':'360助手',
                               '8':'应用宝（自然）','9':'应用宝（付费）','10':'UC浏览器（付费）','11':'其他','12':'App Store'},
                     '2-快应用渠道':{'1':'华为快应用','2':'OPPO快应用','3':'VIVO快应用','4':'小米快应用','5':'魅族快应用',
                                        '6':'努比亚快应用','7':'其他快应用'},
                     '3-精准投放': {'all':'全部'},
                     '4-联盟': {'1':'手机联盟', '2':'评论联盟', '3':'微信联盟'},
                    '5-分享活动': {'1':'打卡', '2':'0元领', '3':'一分抽奖', '4':'步数赚钱', '5':'天天领现金', '6':'1元砍价', '7':'读书计划',
                     '8':'抓娃娃', '9':'答题领红包', '10':'当当抽大奖', '11':'助力免单'},
                    # '6-搜索': {'1':'SEM', '2':'免费搜索'},
                      '6-搜索':{'1':{'1':'百度SEM','2':'搜狗SEM','3':'神马SEM','4':'360SEM'},
                                   '2':{'1':'搜狗免费搜索','2':'神马免费搜索','3':'360免费搜索','4':'百度免费搜索'}},
                     '7-品专': {'1':'百度品专', '2':'搜狗品专', '3':'神马品专', '4':'360品专'},
                     '8-广告': {'1':'app广告', '2':'微信小程序广告'}
                     }

def channel_site(datetype, date, dwmkey):
    '''渠道拆解'''
    parent_channellist = [ele for ele in channeldict.keys()]

    for parent_channle in parent_channellist:

        if parent_channle=='all-全部':
            parent_second_channellist=['all']
        else:
            parent_second_channellist=['all']+[ele for ele in channeldict[parent_channle].keys()]

        parent_channle=parent_channle.split('-')[0]

        for parent_second_channel in parent_second_channellist:


             for user_type in ['all', '1', '2']:  # 1-新房客，2-老访客

                    data = {'source': '1', 'parent_platform': 'all', 'platform': 'all',
                            'user_type': user_type, 'date_type': datetype, 'date': date, 'parent_channel': parent_channle,
                            'parent_second_channel': parent_second_channel, 'channel': 'all'}

                    # data = {'source': 'all', 'parent_platform': 'all', 'platform': 'all',
                    #         'user_type': user_type, 'date_type': datetype, 'date': date, 'parent_channel': '8',
                    #         'parent_second_channel': 'all', 'channel': 'all'}

                    apidata = api.client_analysis_api(data, dwmkey,'site')
                    sqldata = sql.channel_site_sql(data, dwmkey)

                    if diff(apidata, sqldata):
                        log(apidata)
                        log(sqldata)
                        log('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx-Fail-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')


def channel_analysis(datetype,date,dwmkey):


    #趋势分析
    # trend_analysis(datetype,date,dwmkey)
    #客户端分析
    # client_analysis(datetype, date, dwmkey)

    #渠道拆解
    channel_site(datetype,date,dwmkey)



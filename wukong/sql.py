from clickhouse_driver import Client,connect
from decimal import Decimal
from concurrent import futures
import configparser
import datetime
import math
import util

def readini(path):
    cf=configparser.ConfigParser()

    cf.read(path,encoding='utf-8')

    return cf

cf=readini('./config.ini')

host=cf.get('db','host')
port=cf.get('db','port')
user=cf.get('db','user')
pwd=cf.get('db','password')

# client=Client(host=host,port=port,user=user,password=pwd)
conn=connect(host=host,database='bi_mdata',user=user,password=pwd)
cursor = conn.cursor()

commonkeyslist = ['create_price', 'create_price_lastyear_ratio', 'create_price_pre_ratio',
                 'create_parent_amt', 'create_parent_amt_lastyear_ratio', 'create_parent_amt_pre_ratio',
                 'priceByParent','priceByParent_lastyear_ratio','priceByParent_pre_ratio',
                  'priceByPerson','priceByPerson_lastyear_ratio','priceByPerson_pre_ratio',
                  'create_order_amt','create_order_amt_lastyear_ratio','create_order_amt_pre_ratio']

cancelkeyslist=['cancel_rate','cancel_rate_lastyear_ratio','cancel_rate_pre_ratio']
transratekeyslist=['transRate','transRate_lastyear_ratio','transRate_pre_ratio']

zfkeyslist=['out_profit','out_profit_lastyear_ratio','out_profit_pre_ratio',
            'out_pay_amount','out_pay_amount_lastyear_ratio' ,'out_pay_amount_pre_ratio']

ckkeyslist= ['out_profit','out_profit_lastyear_ratio','out_profit_pre_ratio',
            'out_pay_amount','out_pay_amount_lastyear_ratio' ,'out_pay_amount_pre_ratio',
            'gross_profit','gross_profit_lastyear_ratio','gross_profit_pre_ratio',
            'gross_profit_rate','gross_profit_rate_lastyear_ratio','gross_profit_rate_pre_ratio']

commontable=' bi_mdata.mdata_interface_city_{}_{}_v3  '
translatetable= ' bi_mdata.mdata_interface_transrate_users_{}_{}_v3  '

_transratesql='select transRate,transRate_lastyear_ratio,transRate_pre_ratio from  ' \
                     'bi_mdata.mdata_interface_transrate_users_{}_{}_v3  '



transcommonsql_from=' FROM bi_mdata.mdata_interface_transrate_users_{}_{}_v3  '


commonwhere = " where source = '{}'  AND parent_platform = '{}'  AND platform ='{}' " \
                "AND bd_id ='{}' AND shop_type = '{}' AND tichu_type = '{}' and date_str = '{}' "


transratesubwhere= " and app_version = 'all' and new_id ='all' and cat_id ='all'"

def overviewselect(module=None,datadict=None):

    where = ''
    overviewwhere = commonwhere.format(datadict['source'],
                                       datadict['parent_platform'],
                                       datadict['platform'],
                                       datadict['bd_id'],
                                       datadict['shop_type'],
                                       datadict['tichu_type'],
                                       datadict['date_str'])

    saletype=datadict['sale_type']
    tempsql = ''

    if saletype== 'sd':
        saletype = 'sd'

        for key in commonkeyslist+cancelkeyslist:
            tempsql += key + ","

        sql="SELECT "+tempsql.strip(',')+ " FROM "+commontable.format(saletype,datadict['date_type'])
        # sd_sql=sql+overviewwhere+" and city_id='all'"

    elif saletype=='zf':
        saletype = 'pay'

        for key in commonkeyslist+zfkeyslist+cancelkeyslist:
            tempsql += key + ","

        sql="SELECT "+tempsql.strip(',')+ " FROM "+commontable.format('zf',datadict['date_type'])
        # zf_sql=sql+overviewwhere+" and city_id='all'"

    else:
        saletype = 'ck'
        for key in commonkeyslist + ckkeyslist:
            tempsql += key + ","

        sql = "SELECT " + tempsql.strip(',') + " FROM " + commontable.format(saletype, datadict['date_type'])
        # ck_sql = sql + overviewwhere + " and city_id='all'"

    overviewsql=sql + overviewwhere + " and city_id='all'"


    if module=='core_index':

        #数据库查询
        # ans1 = client.execute(core_index_sql)
        cursor.execute(overviewsql)
        ans1=cursor.fetchall()


        #封装overview查询信息
        overviewinfodict={}
        rawdict = {}
        if len(ans1)>=1 :

            ansinfolist=[round(float(ele),2) if ele is not None else "--" for ele in list(ans1[0]) ]           #为简单起见，所有value值均保留小数点后两位
            # ansinfolist = [round(float(ele), 2)  for ele in list(ans1[0]) if ele is not None ]

            if saletype == 'sd':
                rawdict=dict(zip(commonkeyslist+cancelkeyslist,ansinfolist))
            elif saletype=='zf' or saletype=='pay':
                rawdict = dict(zip(commonkeyslist+zfkeyslist+cancelkeyslist, ansinfolist))
            else:
                rawdict = dict(zip(commonkeyslist + ckkeyslist, ansinfolist))

        overviewinfodict.update(rawdict)           #更新字典

        # 转化率读其他表
        transdict={}
        transratesql =_transratesql.format(saletype,datadict['date_type'])+overviewwhere+transratesubwhere

        # ans2 = client.execute(transsql)
        cursor.execute(transratesql)
        ans2=cursor.fetchall()

        if len(ans1) > 0:
            if len(ans2)>=1:

                    ansinfolist2 = list(ans2[0])

                    ansinfolist = [round(float(ele), 2) if ele is not None else "--" for ele in ansinfolist2]
                    # ansinfolist = [round(float(ele), 2) for ele in ansinfolist2 if ele is not None ]

                    transdict=dict(zip(transratekeyslist,ansinfolist))
            else:

                    transdict ={ 'transRate': '--', 'transRate_lastyear_ratio': '--', 'transRate_pre_ratio': '--'}

        overviewinfodict.update(transdict)            #再次更新
        #返回整个字典
        return overviewinfodict


    if module=='xiazuan':

        xiazuaninfo={}

        #用户转化率特殊处理
        fieldstr=datadict['field_str']

        subwhere=''
        if fieldstr=='transRate':

            commsql= 'select {} {} as totalprice from bi_mdata.mdata_interface_transrate_users_{}_{}_v3 '

            subwhere=" and cat_id='all' and (new_id = '1') "+"ORDER BY totalprice DESC LIMIT 10"


        else:

            commsql = 'select {} {} as totalprice from bi_mdata.mdata_interface_cat_{}_{}_v3 '

            subwhere=" and cat_id='all' and (client_version_flag = 'all')  "


        commwhere=" where  source='{}' and parent_platform='{}' " \
                   "and platform='{}' and bd_id='{}' and shop_type = '{}' and tichu_type = '{}' and date_str='{}'"


        sql = commsql.format('{}', datadict['field_str'], datadict['sale_type'], datadict['date_type'])

        where = commwhere.format(datadict['source'], datadict['parent_platform'], datadict['platform'],
                                 datadict['bd_id'], datadict['shop_type'], datadict['tichu_type'], datadict['date_str'])


        # trend
        trenddict = {}  # 存放trend 结果

        trendsql=sql.format('')
        trendsql = trendsql +where+subwhere

        # ans = client.execute(trendsql)
        cursor.execute(trendsql)
        ans = cursor.fetchall()

        if len(ans) > 0 and ans[0][0] is not None:                     #key值换算
            enddate = datadict['date_str']
            templist = enddate.split('-')

            if datadict['date_type']=='day':
                key=templist[1]+'/'+templist[2]

            elif datadict['date_type']=='wtd':
                a = datetime.datetime.strptime(enddate, '%Y-%m-%d')
                key='W'+str(a.isocalendar()[1])
            elif datadict['date_type']=='mtd':
                key=str(int(templist[1]))+'月'
            else:
                key='Q'+str(math.ceil(int(templist[1])/3))

            # trenddict[key] =int(Decimal(ans[0][0]).quantize(Decimal(1)))
            trenddict[key] =round(ans[0][0],2)

        xiazuaninfo['trend'] = trenddict

        '''
        #如果是天，往前推7天
        if datadict['date_type']=="day":

            for i in range(0,7):
                a=datetime.datetime.strptime(enddate, '%Y-%m-%d')
                tempdate=a-datetime.timedelta(days=i)

                date += [datetime.datetime.strftime(tempdate, '%Y-%m-%d')]

        #如果是周，
        if datadict['date_type']=="wtd":
            # 星期换算
            a = datetime.datetime.strptime(enddate, '%Y-%m-%d')

            xq = a.weekday()  # 该方法返回数字0—6，依次代表周一到周日

            for i in range(xq + 1):
                tempdate = a - datetime.timedelta(days=i)
                date += [datetime.datetime.strftime(tempdate, '%Y-%m-%d')]

        #如果是月，往前推7月（只限于本年度）
        if datadict['date_type']=="mtd":

            datelist=enddate.split('-')

            year=int(datelist[0])
            month=int(datelist[1])

            if month-7<=0:
                monthstart=1
            else:
                monthstart=month-6

            date=[]
            for i in range(monthstart,month):

                if i == 4 or i==6 or i==9 or i==11:
                    day="30"
                elif i==2:
                    if  year % 400==0 or (year%4==0 and year %100!=0):  #如果是闰年
                        day="29"
                    else :
                        day="28"
                else:
                    day="31"

                if len(str(i))<2:
                    monthstr="0"+str(i)

                date += [str(year) + "-"+monthstr+"-" + day]
            date+=[enddate]       #最后一个日期

        #如果是季度
        if datadict['date_type'] == "qtd":
                datelist = enddate.split('-')
                month=int(datelist[1])
                date=[]
                if month<=3:
                    date += [enddate]          # 最后一个日期
                elif month>3 and month<7:
                    date=[datelist[0]+'-'+'03'+"-31",enddate]

                elif month>=7 and month<=9:
                    date=[datelist[0]+'-'+'03'+"-31",datelist[0]+'-'+'06'+"-30",enddate]

                else:

                    date = [datelist[0] + '-' + '03' + "-31", datelist[0] + '-' + '06' + "-30",
                            datelist[0] + '-' + '09' + "-30",enddate]
        '''

        #事业部分布

        bdsql = sql.format('bd_name,')

        if datadict['bd_id']in ['1']:

            bdsql = sql.format('cat_name,')
            bdwhere=where+subwhere.replace('cat_id=','cat_id!=')

        elif datadict['bd_id']in ['2','3','4']:

            bdwhere = where

        else:
            bdwhere=where.replace('bd_id=','bd_id!=')+subwhere

        bdsql = bdsql + bdwhere
        # ans = client.execute(bdsql)      #[('其他', 2), ('日百服', 73531), ('出版物事业部', 242043), ('ALL', 318536), ('文创事业部', 2966)]
        cursor.execute(bdsql)
        ans = cursor.fetchall()

        bddict={}
        if len(ans)>0:
            for ele in ans:
                if ele[0]!='ALL' and ele[1] is not None:
                    bddict[ele[0][:2]]=round(float(ele[1]),2)   #取前两个中文字符


        if bddict.__contains__('出版'):
            bddict['出版物']=bddict['出版']
            del(bddict['出版'])

        if bddict.__contains__('其他'):
            bddict['其它'] = bddict['其他']
            del (bddict['其他'])
        if bddict.__contains__('ot'):
            bddict['others'] = bddict['ot']
            del (bddict['ot'])

        xiazuaninfo['bd'] = bddict

        #平台分布
        platdict={}

        platindexdict = {'1': "主站", '2': "天猫", '3': "抖音", '4': "拼多多"}


        source="='{}'".format(datadict['source'])
        parent_platform="='{}'".format(datadict['parent_platform'])
        platform = "='{}'".format(datadict['platform'])

        if datadict['source'] == '1':                           #主站

            platkey='platform'
            if datadict['parent_platform'] == '1':              #选择了app

                platindexdict = {'1': "安卓", '2': "iOS"}
                if datadict['platform'] in ['1','2']:
                    platform='='+"'"+datadict['platform']+"'"
                else:
                    platform="!='all'"
            elif datadict['parent_platform'] == '2':           #选择了小程序
                platindexdict = {'3': "快应用", '4': "微信",'5':'百度','6':'头条','7':'支付宝','8':'qq','9':'360'}
                if datadict['platform'] in ['3','4','5','6','8']:
                    platform = '=' + "'" + datadict['platform'] + "'"
                else:
                    platform = "!='all'"
            elif datadict['parent_platform'] == '3':  # 选择了H5
                platindexdict = {'all':'H5'}
                platform = '=' + "'" + datadict['platform'] + "'"

            elif datadict['parent_platform'] == '4':  # 选择了PC
                platindexdict = {'all': 'PC'}
                platform = '=' + "'" + datadict['platform'] + "'"

            else:
                platindexdict = {'1': 'APP','2':'小程序','3':'H5',"4":'PC','5':'其它'}
                platkey = 'parent_platform'
                platform = "='{}'".format(datadict['platform'])
                parent_platform="!='all'"
        elif datadict['source'] == 'all':

            source="!='all'"
            platkey='source'

        else:
            platform = "='{}'".format(datadict['platform'])
            platkey='source'


        platsql =sql.format(platkey+",")

        platwhere=" where  source{} and parent_platform{} and platform{} " \
                   " and bd_id='{}' and shop_type = '{}' and tichu_type = '{}' and date_str='{}'".format(source,parent_platform,
                                                                                                         platform,datadict['bd_id'],
                                                                                                         datadict['shop_type'],
                                                                                                         datadict['tichu_type'],
                                                                                                         datadict['date_str']

        )+subwhere

        platsql = platsql + platwhere
        # ans = client.execute(platsql)
        cursor.execute(platsql)
        ans = cursor.fetchall()

        if len(ans) > 0:
            for ele in ans:
                if ele[1] is not None and ele[0] not in ['90','91']:
                    platdict[platindexdict[ele[0]]] = round(float(ele[1]),2)
            pass

        xiazuaninfo['platform'] = platdict

        #新老客分布
        sqlinfo = {}
        customersql =sql.format('new_name,').replace('cat','new')

        customerwhere=where+" and new_name!='ALL'" +subwhere.replace("and cat_id='all'",'')

        customersqlsql = customersql + customerwhere
        # ans = client.execute(customersqlsql)
        cursor.execute(customersqlsql)
        ans = cursor.fetchall()


        if len(ans) > 0:
            for ele in ans :
                if ele[1] is not None:
                    sqlinfo[ele[0]] = round(float(ele[1]),2)

        xiazuaninfo['customer'] = sqlinfo


        #城市排名或 app版本分布
        sqlinfo={}

        if fieldstr=='transRate':
            app_citysql=sql.format('app_version,')
            app_citywhere=where+subwhere
            app_citykey='app'

        else:
            app_citysql = sql.format('city_name,').replace('cat','city')

            app_citywhere = where+" and city_id!='all' "+"order by totalprice desc limit 20"
            app_citykey='city'

        app_citysql=app_citysql+app_citywhere
        # ans = client.execute(citysql)
        cursor.execute(app_citysql)
        ans = cursor.fetchall()

        if len(ans)>0:
            for ele in ans :     #如果值为空，跳过这个城市
                if ele[1] is not None:
                    sqlinfo[ele[0]]=round(float(ele[1]),2)

        xiazuaninfo[app_citykey]=sqlinfo

        return xiazuaninfo




def userselect(module=None,datadict=None):
    userinfodict = {}

    #

    if module == 'core_index':

        usersql1 = 'SELECT new_uv ,new_uv_lastyear_ratio,new_uv_pre_ratio,' \
                   'new_uv_ratio,new_uv_ratio_lastyear_ratio,new_uv_ratio_pre_ratio,' \
                   'register_number,register_number_lastyear_ratio,register_number_pre_ratio,' \
                   'uv,uv_lastyear_ratio,uv_pre_ratio  ' \
                   'FROM bi_mdata.mdata_interface_plat_users_{}_v3 '.format(datadict['date_type'])

        usersql2 = 'select new_create_parent_uv_sd,new_create_parent_uv_sd_lastyear_ratio,new_create_parent_uv_sd_pre_ratio ' \
                   'from bi_mdata.mdata_interface_new_cust_sd_{}_v3'

        usersql3 = 'select new_create_parent_uv_zf,new_create_parent_uv_zf_lastyear_ratio,new_create_parent_uv_zf_pre_ratio ' \
                   'from bi_mdata.mdata_interface_new_cust_zf_{}_v3'

        usersql4 = 'select new_create_parent_uv_ck,new_create_parent_uv_ck_lastyear_ratio,new_create_parent_uv_ck_pre_ratio ' \
                   'from bi_mdata.mdata_interface_new_cust_ck_{}_v3'

        usersql5 = 'select create_parent_uv_sd,create_parent_uv_sd_lastyear_ratio,create_parent_uv_sd_pre_ratio ' \
                   'from bi_mdata.mdata_interface_new_sd_{}_v3'

        usersql6 = 'select create_parent_uv_zf,create_parent_uv_zf_lastyear_ratio,create_parent_uv_zf_pre_ratio ' \
                   'from bi_mdata.mdata_interface_new_zf_{}_v3'

        usersql7 = 'select create_parent_uv_ck,create_parent_uv_ck_lastyear_ratio,create_parent_uv_ck_pre_ratio ' \
                   'from bi_mdata.mdata_interface_new_ck_{}_v3'

        usersql8='select daycount_ratio_sd,daycount_ratio_sd_lastyear_ratio,daycount_ratio_sd_pre_ratio ' \
                   'from bi_mdata.mdata_interface_new_sd_{}_v3'

        usersql9='select daycount_ratio_zf,daycount_ratio_zf_lastyear_ratio,daycount_ratio_zf_pre_ratio ' \
                   'from bi_mdata.mdata_interface_new_zf_{}_v3'


        where1 = " where  source='{}' and parent_platform = '{}'  AND platform ='{}' " \
                 "AND date_str='{}';".format(datadict['source'], datadict['parent_platform'], datadict['platform'],
                                             datadict['date_str'])

        where2 = " where tichu_type = 'all' AND new_id = 'all' AND client_version_flag = 'all' and bd_id='all' and shop_type='all'" \
                 " and source='{}' and parent_platform = '{}'  AND platform ='{}' " \
                 "AND date_str='{}';".format(datadict['source'], datadict['parent_platform'], datadict['platform'],
                                             datadict['date_str'])

        where3 = " where tichu_type = 'all' AND new_id = '1' AND client_version_flag = 'all' AND bd_id = 'all' " \
                 " and shop_type='all' and source='{}' and parent_platform = '{}'  AND platform ='{}' " \
                 "AND date_str='{}';".format(datadict['source'], datadict['parent_platform'], datadict['platform'],
                                             datadict['date_str'])

        sql=usersql1+where1
        # ans1 = client.execute(sql)
        cursor.execute(sql);
        ans1 = cursor.fetchall()

        if len(ans1) >= 1:
            ansinfotuple = ans1[0]
            ansinfolist = list(ansinfotuple)

            for i in range(len(ansinfolist)):

                ele = ansinfolist[i]
                if ele is not None:
                    if i % 3 != 0 or i == 3 and ele!=0:
                        ansinfolist[i] = str(ansinfolist[i]) + "%"

                    else:

                        jingdu = '0.00'  # 默认精度

                        # 判断大小带单位
                        if ele > 100000000:
                            ansinfolist[i] = str((Decimal(ele / 100000000)).quantize(Decimal(jingdu)))
                        elif (ele > 10000 and ele < 100000000) or ele < -10000:
                            ansinfolist[i] = str((Decimal(ele / 10000)).quantize(Decimal(jingdu)))
                        elif ele>0 and ele <10000:

                            strele = str(ele).split('.')
                            if len(strele) == 1 or strele[1] == '0' or strele[1]=='00':
                                ansinfolist[i] = strele[0]
                            else:
                                ansinfolist[i] = str((Decimal(ele)).quantize(Decimal(jingdu)))
                        elif ele==0:

                            ansinfolist[i]='--'
                        else:
                            pass

                else:
                    ansinfolist[i] = '--'


            userinfodict['new_uv'] = {'value':ansinfolist[0] ,'value_tb': ansinfolist[1], 'value_hb':ansinfolist[2]}

            userinfodict['new_uv_ratio'] = {'value': ansinfolist[3] ,'value_tb': ansinfolist[4], 'value_hb':ansinfolist[5]}

            userinfodict['register_number'] = {'value': ansinfolist[6] ,'value_tb': ansinfolist[7], 'value_hb':ansinfolist[8]}

            uvdict = {'value': ansinfolist[9], 'value_tb': ansinfolist[10], 'value_hb': ansinfolist[11]}
        else:
            userinfodict['new_uv'] = {'value': '--', 'value_tb': '--', 'value_hb': "--"}
            userinfodict['new_uv_ratio']={'value': '--', 'value_tb': '--', 'value_hb': "--"}
            userinfodict['register_number']={'value': '--', 'value_tb': '--', 'value_hb': "--"}
            uvdict = {'value': '--', 'value_tb': "--", 'value_hb': "--"}

        i=1




        for sql in [(usersql2,'new_create_parent_uv_sd'),(usersql3,'new_create_parent_uv_zf'),
                    (usersql4,'new_create_parent_uv_ck'),(usersql5,'create_parent_uv_sd'),
                    (usersql6,'create_parent_uv_zf'),(usersql7,'create_parent_uv_ck'),
                    (usersql8,'daycount_ratio_sd'),(usersql9,'daycount_ratio_zf')]:

            tempdict={}

            if sql[1].startswith('new'):
                newsql = sql[0].format(datadict['date_type']) + where3
            else:
                newsql = sql[0].format(datadict['date_type']) + where2

            # ans2 = client.execute(newsql)
            cursor.execute(newsql);
            ans2 = cursor.fetchall()

            if len(ans2) >= 1:
                ansinfotuple = ans2[0]

                ansinfolist2 = list(ansinfotuple)

                if ansinfolist2[0] is None:
                    value='--'
                elif ansinfolist2[0]<10000:
                    value=str(round(ansinfolist2[0],2))
                else:
                    value=str((Decimal(ansinfolist2[0]/10000)).quantize(Decimal("0.00")))

                tempdict[sql[1]] = {
                        'value':value  , \
                        'value_tb': str(ansinfolist2[1])+"%", 'value_hb': str(ansinfolist2[2])+"%"}
            else:
                tempdict[sql[1]] = {'value': "--", 'value_tb':  "--", 'value_hb':  "--"}

            userinfodict.update(tempdict)
            if  i==3:
                userinfodict['uv'] = uvdict

            i+=1


        return userinfodict

    if module=='xiazuan':
        xiazuaninfodict={}

        enddate=datadict['date_str']
        templist = enddate.split('-')

        if datadict['date_type'] == 'day':
            key = templist[1] + '/' + templist[2]

        elif datadict['date_type'] == 'wtd':
            a = datetime.datetime.strptime(enddate, '%Y-%m-%d')
            key = 'W' + str(a.isocalendar()[1])
        elif datadict['date_type'] == 'mtd':
            key = str(int(templist[1])) + '月'
        else:
            key = 'Q' + str(math.ceil(int(templist[1]) / 3))


        fieldstr=datadict['field_str']

        commsql= 'select {} {} from bi_mdata.{}_{}_v3 '.format('{}','{}','{}',datadict['date_type'])

        commwhere=" where source='{}' and parent_platform='{}' and platform='{}' and date_str='{}'".format(datadict['source'],
                                                                                                     datadict['parent_platform'],
                                                                                                     datadict['platform'],enddate)

        subwhere=''
        if fieldstr in ['new_uv', 'new_uv_ratio', 'register_number', 'uv']:
            sql = commsql.format('{}',datadict['field_str'], 'mdata_interface_plat_users')

            where = commwhere

        else:

            # 判断sd\zf、ck

            sd_zf_ck = datadict['field_str'][-2:]

            # trendsql='select new_create_parent_uv_{} from bi_mdata.mdata_interface_new_cust_{}_{}_v3'.format(sd_zf_ck,sd_zf_ck,datadict['date_type'])


            if fieldstr.startswith('new') or fieldstr.startswith('day'):
                sql = commsql.format('{}', fieldstr, 'mdata_interface_new_cust' + "_" + sd_zf_ck)

                subwhere=" and tichu_type = 'all' AND new_id = '1' AND client_version_flag = 'all' AND bd_id = 'all' " \
                                         " and shop_type='all'"

                where = commwhere + subwhere

            else:
                sql=commsql.format('{}', fieldstr, 'mdata_interface_cat' + "_" + sd_zf_ck)

                subwhere=" and tichu_type = 'all' AND client_version_flag = 'all' AND bd_id = 'all' " \
                                         " and shop_type='all' and cat_id='all' "

                where = commwhere + subwhere

        #trend
        trenddict={}

        trendsql=sql.format('')+where

        # ans = client.execute(trendsql)

        cursor.execute(trendsql);ans = cursor.fetchall()


        if len(ans) > 0 and ans[0][0] is not None and ans[0][0]>0:
            ansvalue = ans[0][0]
            if isinstance(ansvalue,Decimal):
                ansvalue=float(ansvalue)
            trenddict[key] = round(ansvalue, 2)


        xiazuaninfodict['trend'] = trenddict


        #platform or app
        bothshow=1
        platdict = {}

        platindexdict = {'1': "主站", '2': "天猫", '3': "抖音", '4': "拼多多"}

        source = "='{}'".format(datadict['source'])
        parent_platform = "='{}'".format(datadict['parent_platform'])
        platform = "='{}'".format(datadict['platform'])

        if datadict['source'] == '1':  # 主站

            platkey = 'platform'
            if datadict['parent_platform'] == '1':  # 选择了app

                platindexdict = {'1': "安卓", '2': "iOS"}
                if datadict['platform'] in ['1', '2']:

                    platform = '=' + "'" + datadict['platform'] + "'"
                else:
                    platform = "!='all'"
            elif datadict['parent_platform'] == '2':  # 选择了轻应用
                platindexdict = {'3': "快应用", '4': "微信", '5': '百度', '6': '头条', '7': '支付宝', '8': 'qq','9':'360'}
                if datadict['platform'] in ['3', '4', '5', '6','7', '8']:
                    platform = '=' + "'" + datadict['platform'] + "'"
                else:
                    platform = "!='all'"
            elif datadict['parent_platform'] == '3':  # 选择了H5
                platindexdict = {'all': 'H5'}
                platform = '=' + "'" + datadict['platform'] + "'"

            elif datadict['parent_platform'] == '4':  # 选择了PC
                platindexdict = {'all': 'PC'}
                platform = '=' + "'" + datadict['platform'] + "'"

            elif datadict['parent_platform'] == '5':  # 选择了其它
                platindexdict = {'all': '其它'}
                platform = '=' + "'" + datadict['platform'] + "'"
            else:
                platindexdict = {'1': 'APP', '2': '小程序', '3': 'H5', "4": 'PC'}
                platkey = 'parent_platform'
                platform = "='{}'".format(datadict['platform'])
                parent_platform = "!='all'"
        elif datadict['source'] == 'all':

            source = "!='all'"
            platkey = 'source'

        else:
            platform = "='{}'".format(datadict['platform'])
            platkey = 'source'

        platsql = sql.format(platkey+",")
        platwhere = " where  source{} and parent_platform{} and platform{}  and date_str='{}'".format(source,parent_platform,
                                                                                                         platform,datadict['date_str'])


        sql = platsql + platwhere +subwhere
        # ans = client.execute(sql)
        cursor.execute(sql);
        ans = cursor.fetchall()

        if len(ans) > 0:
            for ele in ans:
                if ele[1] is not None and ele[0] not in ['91','90'] and ele[1] >0 :
                    ansvalue = ele[1]
                    if isinstance(ansvalue, Decimal):


                        ansvalue = float(ansvalue)
                    if ele[0] in platindexdict.keys():
                        platdict[platindexdict[ele[0]]] = round(ansvalue, 2)
            pass

        xiazuaninfodict['platform'] = platdict

        #版本分布






        return xiazuaninfodict




def liucunselect(sql,date_str,data):



    datetype=data['date_type']
    if datetype=="d":
        subkey='日'
        datekey=''
        step=1
        num=16

    elif datetype=='w':
        subkey="周"
        datekey="当周"
        step=7
        num=8
    else:
        subkey="月"
        datekey = ''
        step=30
        num=4

    where = " where source='{}' and parent_platform='{}' and platform='{}' and new_id='{}' ".format(
        data['source'], data['parent_platform'], data['platform'], data['user_type'])

    selectsql = sql + where + " and date_str='{}' " + " order by time limit "+str(num)

    startdate = date_str[0]
    date_num = date_str[1]

    tempdate = startdate
    liucun = {}


    for i in range(date_num):
        item={}
        date=tempdate
        finalsql=selectsql.format(date)

        # finalsql="select uv,return_uv_ratio,data_date-date_str as time from bi_mdata.mdata_interface_visit_users_return_all_w " \
        #          "where source='1' and parent_platform='3' and platform='all' and new_id='all'  and date_str='2020-09-07'  order by time limit 8"

        try:
             cursor.execute(finalsql)
             ans = cursor.fetchall()
        except Exception as e:
            ans=[]

        if len(ans)>0:
            for ele in ans[1:]:

                item.update({str(ele[2]//step)+subkey:round(float(ele[1]),2)})

            if ans[0][0]<10000:
                people=ans[0][0]
            else:
                people=ans[0][0]/10000
            item['人数']=round(people,2)
        #日期

        i+=1
        if datetype!='m':
            liucun[date + datekey] = item
            tempdate=(datetime.datetime.strptime(tempdate,'%Y-%m-%d')+datetime.timedelta(days=step)).strftime('%Y-%m-%d')
        else:
            tempdatelist=tempdate.split('-')
            m_key=tempdatelist[0]+"-"+tempdatelist[1]
            liucun[m_key]=item

            tempdate=tempdatelist[0]+"-0"+str(int(tempdatelist[1])+1)+'-01'


    return liucun




trendsql='SELECT uv, uv_lastyear_ratio, uv_pre_ratio, uv_dau, uv_dau_lastyear_ratio, uv_dau_pre_ratio, ' \
         'pay_uv, pay_uv_lastyear_ratio, pay_uv_pre_ratio, send_uv, send_uv_lastyear_ratio, send_uv_pre_ratio, ' \
         'next_day_retention_rate,next_day_retention_rate_lastyear_ratio, next_day_retention_rate_pre_ratio '

channel_trendsql='select parent_second_channel,uv_dau '

trendwhere="FROM bi_mdata.mdata_channel_mobile_{} WHERE new_id ='{}' AND source = '{}' AND parent_platform ='{}' AND " \
            "platform = '{}' AND date_str = '{}'  "

channeltrendkeydict={
                    'all':{'1':'应用市场','2':'快应用渠道','3':'精准投放','4':'联盟','5':'分享活动','6':'搜索','7':'品专','8':'广告'},
                     '1':{'1':'华为智汇云','2':'小米','3':'OPPO市场','4':'VIVO商店','5':'魅族','6':'百度','7':'360助手',
                               '8':'应用宝（自然）','9':'应用宝（付费）','10':'UC浏览器（付费）','11':'其他','12':'App Store'},
                     '2':{'1':'华为快应用','2':'OPPO快应用','3':'VIVO快应用','4':'小米快应用','5':'魅族快应用',
                                        '6':'努比亚快应用','7':'其他快应用'},
                     '3': {'all':'精准投放'},
                     '4': {'1':'手机联盟', '2':'评论联盟', '3':'微信联盟','4':'微信小程序','5':'二级分销','6':'PC联盟'},
                    '5': {'1':'打卡', '2':'0元领', '3':'一分抽奖', '4':'步数赚钱', '5':'天天领现金', '6':'1元砍价', '7':'读书计划',
                     '8':'抓娃娃', '9':'答题领红包', '10':'当当抽大奖', '11':'助力免单'},
                    '6': {'1':'SEM', '2':'免费搜索'},
                    #   '6':{'1':{'1':'百度SEM','2':'神马SEM','3':'360SEM','4':'搜狗SEM'},
                    #                '2':{'1':'搜狗免费搜索','2':'神马免费搜索','3':'360免费搜索','4':'百度免费搜索'}},
                     '7': {'1':'百度品专', '2':'搜狗品专', '3':'神马品专', '4':'360品专'},
                     '8': {'1':'app广告', '2':'微信小程序广告'}
                     }


def trend_analysis_sql(data,dwmkey):

    overtrendkey=dwmkey[0]

    data = dict(data)

    _table=data['date_type']
    _newid=data['user_type']
    _source=data['source']
    _parent_platform=data['parent_platform']
    _platform=data['platform']
    # _date_str = data['date']
    _date_str = dwmkey[2]


    trend_analysis={}

    #trend
    trend={}
    trendwhere_format=trendwhere.format(_table,_newid,_source,_parent_platform,_platform,_date_str)
    where=trendwhere_format+" AND parent_channel = 'all' AND parent_second_channel = 'all' AND channel = 'all'"
    sql=trendsql+where

    cursor.execute(sql)
    ans1 = cursor.fetchall()

    if len(ans1) >= 1:
        ansinfotuple = ans1[0]
        ansinfolist = list(ansinfotuple)

        ansinfolist=[round(float(ele)/10000,2) if ele is not None and ele>10000 else round(float(ele),2) for ele in ansinfolist]

        trend={
               '渠道占比总UV':(ansinfolist[3],ansinfolist[4],ansinfolist[5]),
               '渠道UV': (ansinfolist[0], ansinfolist[1], ansinfolist[2]),
               '支付人数':(ansinfolist[6],ansinfolist[7],ansinfolist[8]),
               '出库人数':(ansinfolist[9],ansinfolist[10],ansinfolist[11]),
               '次日留存率':(ansinfolist[12],ansinfolist[13],ansinfolist[14])}




    trend_analysis['trend'] = trend

    #overall_trend
    overall_trend={}
    if len(ans1) >= 1:
        if ansinfolist[3] is not None:
            overall_trend={overtrendkey:ansinfolist[3]}

    trend_analysis['overall_trend'] = overall_trend


    #channel_trend

    channel_trend = {}

    for firstchannelkey in channeltrendkeydict.keys():

        # parent_channel = firstchannelkey.split('-')[0]
        parent_channel=firstchannelkey
        # parent_second_channellist=[]
        # for secondchannelkey in channeltrendkeydict[firstchannelkey]:
        #     # parent_second_channellist.append(secondchannelkey.split('-')[0])

        parent_second_channellist=list(channeltrendkeydict[firstchannelkey].keys())

        #sql拼接

        #搜索做特殊处理
        if parent_channel=='6':

            channel_trendsql='select parent_second_channel, channel,uv_dau '
            parent_second_channel = str(tuple(parent_second_channellist))

            channel_where = " and parent_channel='6' AND parent_second_channel in {} AND channel in {}".format(
                parent_second_channel,
                "('1','2','3','4')")


        else:

            channel_trendsql = 'select parent_second_channel,uv_dau '



            if len(parent_second_channellist)==1:
                parent_second_channel="('"+parent_second_channellist[0]+"')"
            else:
                parent_second_channel = str(tuple(parent_second_channellist))
            channel_where=" and parent_channel= '{}' AND parent_second_channel in {} AND channel = 'all'".format(parent_channel,
                                                                                                                  parent_second_channel)
            if parent_channel == 'all':
                channel_trendsql = 'select parent_channel,uv_dau '
                channel_where = " and parent_channel in {} AND parent_second_channel = '{}' AND channel = 'all'".format(
                    "('1', '2', '3', '4', '5', '6', '7', '8')", 'all')


        sql=channel_trendsql+trendwhere_format+channel_where

        try:
             cursor.execute(sql)
             ans = cursor.fetchall()
        except Exception as e:
            ans=[]

        templist = set()
        if len(ans)>0:

            if parent_channel == '6':

                # SEM_SOUSU={'1': {'1': '百度SEM', '2': '搜狗SEM', '3': '神马SEM', '4': '360SEM'},
                #  '2': {'1': '搜狗免费搜索', '2': '神马免费搜索', '3': '360免费搜索', '4': '百度免费搜索'}}
                SEM_SOUSU = {'1': {'1': '百度SEM', '2': '搜狗SEM', '3': '神马SEM', '4': '360SEM'},
                             '2': {'1': '百度免费搜索', '2': '搜狗免费搜索', '3': '神马免费搜索', '4': '360免费搜索'}}
                #todo
                for ele in ans:
                    if ele[2] != 0:
                        templist.add((SEM_SOUSU[ele[0]][ele[1]], round(float(ele[2]), 2)))


                    pass
            else:

                for ele in ans:
                    if ele[1]!=0:
                        templist.add((channeltrendkeydict[firstchannelkey][ele[0]],round(float(ele[1]),2)))


        if len(templist)>0:
            channel_trend[firstchannelkey]=templist


    trend_analysis['channel_trend'] = channel_trend


    return trend_analysis



client_keys={'1':{'1':"安卓",'2':"iOS"},                #APP
                '2':{'3':'快应用','4':'微信'},             # 轻应用
                '3':{'4':'联盟','6':'搜索','7':'品专','8':'广告'},#H5
                '4':{'4':'联盟','6':'搜索','7':'品专'}        #PC
                }

#0代表uv占比，稍后计算
clientsql='SELECT  uv,0, uv_lastyear_ratio, uv_pre_ratio, create_price, create_rate,create_uv, ' \
         ' pay_price, pay_rate, pay_uv, send_price, send_rate,send_uv,%(plat)s ' \


clientwhere="FROM bi_mdata.mdata_channel_mobile_{} WHERE new_id ='{}' AND source = '{}' AND parent_platform {} AND " \
            "platform {} and parent_channel {} and parent_second_channel{} and channel='all' AND date_str = '{}'  "

form_title=["UV", "UV占比", "同比", "环比", "收订金额", "收订转化率", "收订人数",
                "支付金额", "支付转化率", "支付人数", "出库金额", "出库转化率", "出库人数"]
def client_analysis_sql(data,dwmkey):
    overtrendkey = dwmkey[0]

    data = dict(data)
    client={}

    _table = data['date_type']
    _newid = data['user_type']
    _source = data['source']
    _parent_platform = data['parent_platform']
    _platform = data['platform']
    # _date_str = data['date']
    _date_str = dwmkey[2]


    #根据platform 拼接sql

    if _parent_platform=='all':
        client_keydict = {'1': 'APP',
                           '2': '轻应用',
                           '3': 'H5',
                           '4': 'PC'}
        plat='parent_platform '

        parentplatform=" in "+str(tuple(list(client_keydict.keys())))
        platform="='all'"

        parentchannel="='all'"
        parentsecondchannel="='all'"

    elif _parent_platform in ['1','2']:

        if _platform == 'all':

            client_keydict=client_keys[_parent_platform]

            plat = 'platform'

            parentplatform = "='" + _parent_platform + "'"
            platform = " in " + str(tuple(list(client_keys[_parent_platform].keys())))

            parentchannel = "='all'"
            parentsecondchannel = "='all'"
        else:

            if _parent_platform == '1':
                client_keydict = {
                                  '3': '精准投放','1': '应用市场',
                                  }
            else:

                if _platform=='3':
                    client_keydict = {
                        '2': '快应用渠道',
                    }
                else:

                    client_keydict = {
                                  '5': '分享活动','8': '抓娃娃',
                                  }

            plat = 'parent_channel'

            parentplatform = "='" + _parent_platform + "'"
            platform = "='" + _platform + "'"

            parentchannel = " in ('" + "','".join(list(client_keydict.keys()))+"')"
            parentsecondchannel = "='all'"


    else:

        client_keydict = client_keys[_parent_platform]
        plat='parent_channel'

        parentplatform="='"+_parent_platform+"'"
        platform=" ='all'"

        parentchannel = " in " + str(tuple(list(client_keys[_parent_platform].keys())))
        parentsecondchannel = "='all'"


    sql=clientsql % {'plat':plat}
    where=clientwhere.format(_table,_newid,_source,parentplatform,platform,parentchannel,parentsecondchannel,_date_str)

    sql=sql+where
    uv_total_sql = "select sum(uv) " + where
    try:
        cursor.execute(sql)
        ans = cursor.fetchall()

        cursor.execute(uv_total_sql)
        ans1 = cursor.fetchall()

    except Exception as e:
        ans = []


    if len(ans) > 0:

        for eletuple in ans:
            templist = []
            key=eletuple[-1]

            if ans1[0][0]==0:
                uvzhanbi=100
            else:
                uvzhanbi=round((eletuple[0]/ans1[0][0])*100,2)

            i=0

            for ele in eletuple[:-1]:

                if ele is None or isinstance(ele,str):
                    continue
                elif   float(ele) > 10000 and i in [0,4,6,7,9,10,12]:
                    t=round(float(ele) / 10000, 2)
                else:
                    t=round(float(ele), 2)

                templist.append(t)
                i+=1
            templist[1] = uvzhanbi  # UV占比

            client[client_keydict[eletuple[-1]]]=list(zip(form_title,templist))

    return client


sitewhere="FROM bi_mdata.mdata_channel_mobile_{} WHERE new_id ='{}' AND source = '1' AND parent_platform{} AND " \
            "platform='all' and parent_channel {} and parent_second_channel{} and channel{} AND date_str = '{}'  "

def channel_site_sql(data,dwmkey):
    overtrendkey = dwmkey[0]

    data = dict(data)
    site={}

    _table = data['date_type']
    _newid = data['user_type']
    # _date_str = data['date']
    _date_str = dwmkey[2]

    _parent_channel=data['parent_channel']
    _parent_second_channel=data['parent_second_channel']

    #根据xxx 拼接sql

    parent_channellist=list(channeltrendkeydict.keys())
    channel = "='all'"

    site_keydict={}
    if _parent_channel=='all':

        plat='parent_channel '

        parent_platform = "='all'"

        parentchannellist=list(channeltrendkeydict[_parent_channel].keys())
        parentchannel=" in "+str(tuple(parentchannellist))

        parentsecondchannel="='{}'".format(_parent_channel)


    elif _parent_channel!='all' and _parent_channel!='3' and _parent_channel!='8' and _parent_second_channel=='all':

        plat='parent_second_channel'

        parent_platform = "='all'"

        parentchannel="='{}'".format(_parent_channel)
        parentsecondchannel = " in "+str(tuple(list(channeltrendkeydict[_parent_channel])))

    else:

        if _parent_channel in ['1', '3']:
            site_keydict = {
                '1': 'APP'
            }
        if _parent_channel in ['2', '5']:
            site_keydict = {
                '2': '轻应用'
            }

        if _parent_channel in ['4','8']:
            site_keydict = {
                '1':'APP',
                '2':'轻应用',
                '3': 'H5'
            }

        if _parent_channel in [ '8']:
            if _parent_second_channel in ['1']:
                site_keydict = {
                    '3': 'H5'
                }
            if _parent_second_channel in ['2'] :

                site_keydict = {
                    '2': '轻应用'
                }

        if _parent_channel in ['7']:
            site_keydict = {
                              '3': 'H5',
                              '4': 'PC'}


        plat = 'parent_platform'

        parent_platform = " in ('" + "','".join(list(site_keydict.keys()))+"')"



        parentchannel = "='{}'".format(_parent_channel)
        parentsecondchannel = "='{}'".format(_parent_second_channel)


        if _parent_channel in ['6'] and _parent_second_channel in ['1','2']:
            site_keydict={'1': {'1': '百度SEM', '2': '搜狗SEM', '3': '神马SEM', '4': '360SEM'},
                             '2': {'1': '百度免费搜索', '2': '搜狗免费搜索', '3': '神马免费搜索', '4': '360免费搜索'}}

            plat = 'channel'
            parent_platform ="='all'"
            channel = " in "+str(tuple(list(site_keydict[_parent_second_channel].keys())))


    where=sitewhere.format(_table,_newid,parent_platform,parentchannel,parentsecondchannel,channel,_date_str)

    sql=clientsql %  {'plat':plat} +where

    uv_total_sql="select sum(uv) " +where

    try:
        cursor.execute(sql)
        ans = cursor.fetchall()

        cursor.execute(uv_total_sql)
        ans1=cursor.fetchall()

    except Exception as e:
        ans = []


    if len(ans) > 0:

        uv_total=0
        for eletuple in ans:
            templist = []
            key=eletuple[-1]

            if ans1[0][0]==0:
                uvzhanbi=100
            else:
                uvzhanbi=round((eletuple[0]/ans1[0][0])*100,2)

            i=0
            for ele in eletuple[:-1]:

                if ele is None or isinstance(ele,str):
                    continue
                elif   float(ele) > 10000 and float(ele)<100000000 and i in [0,4,6,7,9,10,12]:
                    t=round(float(ele) / 10000, 2)
                elif float(ele)>100000000:
                    t = round(float(ele) / 100000000, 2)
                else:
                    t=round(float(ele), 2)

                templist.append(t)
                i+=1

            templist[1]=uvzhanbi              #UV占比

            if  _parent_channel!='3' and _parent_second_channel=='all':

                if _parent_channel=='8':
                    site[site_keydict[eletuple[-1]]] = list(zip(form_title, templist))
                else:
                    site[channeltrendkeydict[_parent_channel][eletuple[-1]]] = list(zip(form_title, templist))
                pass
            else:
                if _parent_channel=='6':
                    site[site_keydict[_parent_second_channel][eletuple[-1]]] = list(zip(form_title, templist))
                else:
                    site[site_keydict[eletuple[-1]]] = list(zip(form_title, templist))
                pass



    return site














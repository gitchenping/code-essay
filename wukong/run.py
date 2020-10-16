import diff
import util

date = '2020-10-08'

liucunstart_date='2020-09-07'
liucunend_date='2020-09-07'

date_type = ['day', 'wtd','mtd', 'qtd']  # 日、周、月、季度


if __name__ == '__main__':

    datelist=[liucunstart_date,liucunend_date]


    for datetype in date_type:
        # 经营状况,1-overview,2-钻取
        # diff.overview_diff(datetype,date,1)
        # diff.overview_diff(datetype,date,2)

        # 用户分析,1-overview,2-钻取

        # diff.user_analysis_diff(datetype, date,1)
        # diff.user_analysis_diff(datetype, date, 2)


        # 留存分析
        # if datetype!='qtd':
        #     diff.liucun_diff(datetype,datelist)

        #渠道分析
        dwmkey=util.gettrendkey(datetype,date)             #获取日期对应的键，如08/24、 W35
        diff.channel_analysis(datetype,date,dwmkey)

























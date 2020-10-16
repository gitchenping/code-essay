import datetime
import math

def gettrendkey(datetype,date):

    templist=date.split('-')
    if datetype == 'day':
        key = templist[1] + '/' + templist[2]
        apidate=date
        sqldate=date

    elif datetype == 'wtd':
        a = datetime.datetime.strptime(date, '%Y-%m-%d')
        key = 'W' + str(a.isocalendar()[1])
        apidate = templist[0] + "-" + key

        sqldate=daytoweek_lastday(date)

    elif datetype == 'mtd':
        key = str(int(templist[1])) + '月'

        apidate = templist[0] + "-M" + templist[1].lstrip('0')       #去除字符串左边的0

        sqldate=month_to_day()

    else:
        key = 'Q' + str(math.ceil(int(templist[1]) / 3))

        apidate = templist[0] + "-" + key

        sqldate=month_to_day()


    return key,apidate,sqldate


def month_to_day():
    adate = datetime.datetime.now()-datetime.timedelta(days=1)
    strtime=adate.strftime('%Y-%m-%d')

    return strtime


def daytoweek_lastday(date):
    a = datetime.datetime.strptime(date, '%Y-%m-%d')

    delta=6-a.weekday()

    return datetime.datetime.strftime(a + datetime.timedelta(days=delta), '%Y-%m-%d')
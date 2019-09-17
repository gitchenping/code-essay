#encoding=utf-8
import sys,os
import requests
import time,random
import json
import re

'''
常量
'''

BASE_URL='http://www.gongfupian.com'

YOUKU_URL='https://youku.com-ok-163.com'

key_word=sys.argv[1]

url_downrangelist=sys.argv[2]

current_timestamp=int(time.time()*1000)

QUERY_URL='/index.php/ajax/suggest?mid=1&wd='+key_word+'&limit=10&'+str(current_timestamp)

DETAIL_QUERY_URL='/index.php/voddetail/'

'''
获取资源英文名字
'''
def get_url_enname(queryurl):
    
    url=BASE_URL+queryurl
    r=requests.get(url)
    
    data=json.loads(r.text)
    
    enname=data['list'][0]['en']
    
    return enname

'''
资源详细信息列表
'''
def get_url_info(detailqueryurl):
    
    enname=get_url_enname(QUERY_URL)
    
    r=requests.get(detailqueryurl+enname)
    
    rawlist=re.findall('<ul (?:.*)id="con_playlist_1">(.*?)</ul',r.text,flags=re.DOTALL)
    
    reslist=re.findall('<li><a href="(.*?)">',rawlist[0])
    
    return reslist
    

detailqueryurl=BASE_URL+DETAIL_QUERY_URL

urllist=get_url_info(detailqueryurl)

rangelist=url_downrangelist.split('-')
if len(rangelist)==1:
	endurlnum=int(rangelist[0])
else:
	endurlnum=int(rangelist[1])

starturlnum=int(rangelist[0])

for num in range(starturlnum,endurlnum+1):
    
    sigleresource_pageinfo_url=BASE_URL+urllist[num-1]
    r=requests.get(sigleresource_pageinfo_url)
    signurl=re.findall('player_data={(?:.*)"url":"(.*?)"',r.text)[0].replace('\\','')
    
    r=requests.get(signurl)
    shareurl=re.findall('main = "(.*?)";',r.text)[0]
    mainurl_prefix=re.findall('(.*?)/index',shareurl)[0]
    
    r=requests.get(YOUKU_URL+shareurl)
    mainurl_res=r.text.split('\n')[-1]
    tslisturl=YOUKU_URL+mainurl_prefix+"/"+mainurl_res
    
    r=requests.get(tslisturl)
    tslist=re.sub('#(?:.*)','',r.text).strip('\n').split('\n\n')
    
    tsurl_prefix=YOUKU_URL+mainurl_prefix+"/"+re.findall('(.*?)/index',mainurl_res)[0]+'/'
    
    index=0
    for ts in tslist:
        url=tsurl_prefix+ts
        os.system('wget -O '+str(index)+".ts "+url)
        index+=1
        
    print("now begin to combine...")
    i=1    
    while i<index:
        a=os.system('cat '+str(i)+".ts >> 0.ts")
	i+=1

    a=os.system('mv 0.ts '+key_word+"_"+str(num)+".ts")
    print("done！")


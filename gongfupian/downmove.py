#encoding=utf-8
import sys,os
import requests
import time,random
import json
import re

'''
常量
'''

BASE_URL='http://www.gfpian.com'

#YOUKU_URL='https://youku.com-ok-163.com'

YOUKU_URL='https://baidu.com-ok-baidu.com'

key_word=sys.argv[1]    #电影搜索关键字


current_timestamp=int(time.time()*1000)

QUERY_URL='/index.php/ajax/suggest?mid=1&wd='+key_word+'&limit=10&'+str(current_timestamp)

DETAIL_QUERY_URL='/index.php/voddetail/'

DOWN_CHANNEL=1         #下载通道

start=1200

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
    
    rawlist=re.findall('<ul (?:.*)id="con_playlist_'+str(DOWN_CHANNEL)+'">(.*?)</ul',r.text,flags=re.DOTALL)
    
    #print(rawlist)
    
    reslist=re.findall('<li><a href="(.*?)">',rawlist[0])
    
    return reslist
    

detailqueryurl=BASE_URL+DETAIL_QUERY_URL

vodplayurl=BASE_URL+get_url_info(detailqueryurl)[0]
    
r=requests.get(vodplayurl)


shareurl=re.findall('player_data={(?:.*)"url":"(.*?)"',r.text)[0].replace('\\','')    
r=requests.get(shareurl)


mainurl=re.findall('main = "(.*?)";',r.text)[0]

mainurl_prefix=re.findall('(.*?)/index',mainurl)[0]

print(mainurl)
#exit(0)

r=requests.get(YOUKU_URL+mainurl)   #获取 1000k/hls/index.m3u8
mainurl_res=r.text.split('\n')[-1]

tslisturl=YOUKU_URL+mainurl_prefix+"/"+mainurl_res #获取资源列表
r=requests.get(tslisturl)
tslist=re.sub('#(?:.*)','',r.text).strip('\n').split('\n\n')
#print(tslist)

tsurl_prefix=YOUKU_URL+mainurl_prefix+"/"+re.findall('(.*?)/index',mainurl_res)[0]+'/'

length=len(tslist)

index=start
for ts in tslist[start:]:
	url=tsurl_prefix+ts
	os.system('wget -O '+str(index)+".ts "+url)
	index+=1
	

print("now begin to combine...")

i=0
while i<length:
      	a=os.system('cat '+str(i)+".ts >> 0.ts")
        i+=1
a=os.system('mv 0.ts '+key_word+".ts")
print('down')

'''    
    
    mainurl_res=r.text.split('\n')[-1]
    tslisturl=YOUKU_URL+mainurl_prefix+"/"+mainurl_res
    
    print(mainurl_res)

    r=requests.get(tslisturl)
    tslist=re.sub('#(?:.*)','',r.text).strip('\n').split('\n\n')
    
    #print(tslist)
    tsurl_prefix=YOUKU_URL+mainurl_prefix+"/"+re.findall('(.*?)/index',mainurl_res)[0]+'/'
    
    index=0
    for ts in tslist:
        url=tsurl_prefix+ts
        os.system('wget -O '+str(index)+".ts "+url)
tslist=re.sub('#(?:.*)','',r.text).strip('\n').split('\n\n')        index+=1
        
    print("now begin to combine...")
    i=1    
    while i<index:
        a=os.system('cat '+str(i)+".ts >> 0.ts")
	i+=1

    a=os.system('mv 0.ts '+key_word+"_"+str(num)+".ts")
    print("done！")
'''

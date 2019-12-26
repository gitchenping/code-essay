#encoding=utf-8

import sys
import requests
import time
import re
import os

from multiprocessing import Pool

PROCESSNUM=3

def downloadts(url,index):
        os.system('wget -O '+index+" "+url)


def multiprocess_download(tsurl_prefix,tslist):

   
    pool=Pool(processes=PROCESSNUM)

    for ts in tslist:
        url=tsurl_prefix+ts
        index=ts[-6:]
        pool.apply_async(downloadts,(url,index))
    
    pool.close()
    pool.join()   


#默认下载线路
chanel_default="okm3u8"
allchanel=[u'okm3u8',u'kum3u8', u'zuidam3u8', u'奇艺视频', u'qq播客', u'土豆视频', u'乐视视频', u'PPTV视频', u'搜狐视频']

BASE_URL="https://m.xunleiyy.com"
PLAY_URL="https://player.gxtstatic.com"

VIP_PLAY_URL=PLAY_URL+"/vipplay.php"
SEARCH_URL=BASE_URL+"/search.php"

#SOURCE_URL=BASE_URL+"/movie/"
headers={"user-agent":"Xiaomi_2016051_TD-LTE/V1 Linux/3.18.22 Android/6.0 Release/8.8.2016 Browser/AppleWebKit537.36 Mobile Safari/537.36 System/Android 6.0 XiaoMi/MiuiBrowser/2.4.9"}

keyword=sys.argv[1]

#下载的资源编号，默认从0开始
res_num=sys.argv[2]

r_search=requests.get(SEARCH_URL+"?searchword="+keyword)

#根据关键字，找出id
url=''

hreflist=re.findall('<a (.*?)</a>',r_search.text)

urllist=[ele for ele in hreflist if keyword.decode('utf-8') in ele and 'alt' not in ele]

url_name_list=[ele.strip('href="').split('">') for ele in urllist]

if len(urllist)>1:
    print "there are "+str(len(urllist))+" resources,please choose favoured:"
    index=0
    for url_name in url_name_list:
        index+=1
        print '【'+str(index)+'】 '+url_name[1].encode('utf-8')+" : "+url_name[0].encode('utf-8')
    
    urlnum=input("输入顺序号或q(quit)，并按回车:")
    if urlnum=='q':
	sys.exit()
    url=url_name_list[urlnum-1][0]
else:
    url=url_name_list[0][0]


if url =="":
    print('resource is not found')
    sys.exit()

#如https://m.xunleiyy.com/movie/id182872.html
r_movie=requests.get(BASE_URL+url)

#可用通道,[u'kum3u8', u'zuidam3u8', u'奇艺视频', u'qq播客', u'土豆视频', u'乐视视频', u'PPTV视频', u'搜狐视频']
chanellist=re.findall('<h3 (?:.*?)>(.*?)</h3',r_movie.text,flags=re.DOTALL)[1:-1]
chanellist=[chanel for chanel in chanellist if chanel in allchanel]

if chanel_default.decode('utf-8') not in chanellist:
    
    chanel=chanellist[0]
else:
    chanel=chanel_default

#资源特征，如/play/182872-2-0.html
resstr=re.findall(chanel+"(?:.*?)href='(.*?)' target",r_movie.text)[0]

#实际资源编号
tezhenlist=resstr.split('-')
realresstr=tezhenlist[0]+"-"+tezhenlist[1]+"-"+str(int(res_num)-1)+'.html'

#资源播放请求地址
playurl=BASE_URL+realresstr
print playurl

rawresurl=requests.get(playurl,headers=headers).text
#得到资源的播放地址，如https://player.gxtstatic.com/vipplay.php?url=https://163.com-163cdn.com/20190911/538_06df94e5/index.m3u8&h=240
resplayurl=re.findall('src="'+VIP_PLAY_URL+'(.*?)"',rawresurl)[0]
print resplayurl

#得到资源的存放地址，如
resstoreurl=requests.get(VIP_PLAY_URL+resplayurl).text

resstoreurl=re.findall('src="'+PLAY_URL+'(.*?)"',resstoreurl)[0]

print resstoreurl

#实际vedio地址
vediourl=requests.get(PLAY_URL+resstoreurl).text

video=re.findall("video:'(.*?)'",vediourl)[0]

print "视频播放地址:"+video.encode('utf-8')

#资源入口，https://163.com-163cdn.com/20190911/538_06df94e5/index.m3u8

#1000k/hls/index.m3u8
resindex=requests.get(video).text.split('\n')[2]

res_ts_list_url=video.strip('index.m3u8')+resindex

#获取ts 列表
r=requests.get(res_ts_list_url)
tslist=re.sub('#(?:.*)','',r.text).strip('\n').split('\n\n')

#
lents=len(tslist)
print "需要下载 "+str(lents)+" 个资源,请耐心等待"

tsurl_prefix=res_ts_list_url.strip('index.m3u8')

multiprocess_download(tsurl_prefix,tslist)
'''
for ts in tslist:
    url=tsurl_prefix+ts
    index=ts[-6:]
    os.system('wget -O '+str(index)+" "+url)
'''
 
i=1    
while i<=lents:
    a=os.system('cat '+tslist[i-1][-6:]+" >> 0.ts")
    i+=1
a=os.system('mv 0.ts '+keyword+"_"+res_num+".ts")
print("done！")

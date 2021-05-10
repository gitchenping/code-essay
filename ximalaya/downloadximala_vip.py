#encoding=utf-8

import requests
import json
import sys
import os
import time

input_arg=sys.argv  #输入参数
search_keyword='棉花帝国'
if len(input_arg)>1:
    search_keyword=input_arg[1]
else:
    print('need a keyword to search !')
    # exit(0)

#ximalaya
PAGE=1
UID=195711234
#headers
useragent="ting_6.6.21(Redmi+K30+5G+Speed,Android29)"
host="audiopay.cos.tx.xmcdn.com"
headers={"user-agent":useragent,"Host":host}

#mobile cookie
# with open('loginmobile.txt','r') as fp:
#         rawcookies=fp.read()
#         rawcookieslist=rawcookies.strip().split(";")
#
#         cookiesdict={}
#         for cookiesitem in rawcookieslist:
#                 cookiesitemlist=cookiesitem.split('=')
#                 cookiesdict[cookiesitemlist[0]]=cookiesitemlist[1]

search_url_prefix='http://search.ximalaya.com'
search_url_suffix="/front/v1?appid=0&categoryId=-1&condition=relation&core=album&device=android&" \
                  "deviceId=1e43bdd2-c7f5-33fb-b745-f9c3e291d5a2&fq=categoryId:-1&"+  \
                    "kw={searchword}&live=true&network=wifi&operator=0&page=1&paidFilter=false&" \
                    "plan=c&recall=normal:group2&rows=20&search_version=2.6&"+  \
                    "spellchecker=true&uid={userid}&version=6.6.21"
#搜索地址
search_kw_url=search_url_prefix+search_url_suffix.format(searchword=search_keyword,userid=UID)

#可能的资源列表
album_rawinfo=requests.get(search_kw_url).text
album_rawinfo_json=json.loads(album_rawinfo)

albuminfo=album_rawinfo_json['response']['docs']

#list all resources
search_resource_list=[]
if len(albuminfo)>0:
    print('find relating resources listed below:')
    i=1
    for ele in albuminfo:
        resource_name=ele['title']
        resource_zhubo=ele['nickname']
        info=str(i)+'、名称:'+resource_name+'  主播:'+resource_zhubo
        i+=1
        print(info)
else:
    print("bad search,no resource find!")

choice_num=input("which resource would you like to download:")

resource_selected=albuminfo[int(choice_num)-1]

#资源的集数、albumId
tracks_num=resource_selected['price_types'][0]['total_track_count']
albumid=resource_selected['price_types'][0]['id']

#获取资源下各集的名字
starttime=str(int(round(time.time()*1000)))
url_for_resource_name="http://mobile.ximalaya.com/mobile/v1/album/track/ts-{}?" \
                      "albumId={}&device=android&isAsc=true&isQueryInvitationBrand=true&" \
                      "pageId=1&pageSize={}&pre_page=0".format(starttime,albumid,tracks_num)
r=requests.get(url_for_resource_name)

rjson=json.loads(r.text)
resourcelist=rjson['data']['list']

each_resource_name_list=[ele['title'] for ele in resourcelist]

#手动获取各资源下载地址
urllist=[]
with open('Untitled.csv','r') as fp:
    for line in fp:
        url=line.split(',')[0]
        if '.m4a' in url:
            urllist.append(url)

def progressbar(processnum,totalnum):
    scale=int(float(processnum)/totalnum*100)
    j = '#'*scale
    print('【'+j+'】->'+str(scale)+'%\r'),
    sys.stdout.flush()

print("there are total "+str(len(resourcelist))+" 集")
for resource_name,url in zip(each_resource_name_list,urllist):
    cmd = "curl -s -o '" + resource_name.encode(
        'utf-8') + ".m4a' " + '-H "user-agent:' + useragent + '"' + ' -H "Host:' + host + '"' + " -b loginmobile.txt '" + url + "'"

    ret = os.popen(cmd)

    progressbar(i, len(resourcelist))
    i += 1


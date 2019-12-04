#encoding=utf-8
import requests
import json
import sys
import os
import time
import readline

PAGE=1
UID=195711234
BASEURL='http://search.ximalaya.com'
SEARCH_KW_URL_PREFIX="http://searchwsa.ximalaya.com/front/v1"
KW_URL_PARA="?appid=0&categoryId=-1&condition=relation&core=album&device=android&deviceId=1e43bdd2-c7f5-33fb-b745-f9c3e291d5a2&fq=categoryId:-1&"+  \
"kw={searchword}&live=true&network=wifi&operator=0&page=1&paidFilter=false&plan=c&recall=normal:group2&rows=20&search_version=2.6&"+  \
"spellchecker=true&uid={userid}&version=6.6.21"

#headers
useragent="ting_6.6.21(Redmi+Note+4,Android23)"
host="audio.pay.xmcdn.com"


keyword=sys.argv[1]
search_kw_url=SEARCH_KW_URL_PREFIX+KW_URL_PARA.format(searchword=keyword,userid=UID)


album_rawinfo=requests.get(search_kw_url).text

album_rawinfo_json=json.loads(album_rawinfo)


albuminfo=album_rawinfo_json['response']['docs'][0]
albumid=str(albuminfo['price_types'][0]['id'])
uid=albuminfo['uid']


starttime=str(int(round(time.time()*1000)))
AUDIO_DOWNURL='http://61.179.224.86/mobile/download/v1/album/paid/'+albumid+"/"+str(PAGE)+"/true/"+str(starttime)+"?albumId="+albumid+"&isAsc=true&pageId=1&trackQualityLevel=0"

#rawcookies=sys.argv[2]
with open('loginmobile.txt','r') as fp:
	rawcookies=fp.read()

#cookies 处理
rawcookieslist=rawcookies.strip().split(";")

cookiesdict={}
for cookiesitem in rawcookieslist:
	cookiesitemlist=cookiesitem.split('=')
	cookiesdict[cookiesitemlist[0]]=cookiesitemlist[1]

headers={"user-agent":"ting_6.6.21(Redmi+Note+4,Android23)","Host":"mobile.ximalaya.com"}


#获取资源列表
r=requests.get(AUDIO_DOWNURL,headers=headers,cookies=cookiesdict)

rjson=json.loads(r.text)
resourcelist=rjson['tracks']['list']

#print resourcelist
#exit(0)

#for pc cookie
print("there are total "+str(len(resourcelist))+" ji")
resource_name_trackid=[]
for resource in resourcelist:
	title=resource['title']
	trackId=resource['trackId']
	uid=resource['uid']
	
	url=input("skip this item,please input number 0,now begin to download : "+title.encode('utf-8')+"\n")	

	if url==0:
		continue
	
	cmd="curl -s -o '"+title.encode('utf-8')+".m4a' " + '-H "user-agent:'+useragent+'"'+' -H "Host:'+host+'"' +" -b loginmobile.txt '"+ url+"'"
	
	ret=os.popen(cmd)	

	#resource_name_trackid.append((title,trackId))
	
	'''
	starttime=str(int(round(time.time()*1000)))
	sendtime=str(int(round(time.time()*1000)))
	audiourl=AUDIO_DOWNURL_PREFIX+str(uid)+'/track/'+str(trackId)+'/ts-'+starttime+'?clientTraffic =0&device=android&downloadPercent=0&sendDataTime ='+  \
		starttime+'&'+'startTime='+starttime+'&trackId='+str(trackId)+'&uid='+str(uid)
	
	resourceinfo=requests.get(audiourl,headers=headers,cookies=cookiesdict)
	resourceinfojson=json.loads(resourceinfo.text)
	try:
		downurl=resourceinfojson['downloadUrl']   #音频下载地址
	except KeyError,e:
		print("can't find downurl")
		continue
        print("begin download "+title)
	raudio=requests.get(downurl)
	with open('./'+title+'.m4a','w') as fp:
		fp.write(raudio.content)
	print('done!')
	'''
#print resource_name_trackid	


#print tracks
#sys.exit(0)
#########



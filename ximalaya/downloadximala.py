
#encoding=utf-8
import requests
import json
import sys
import os

BASEURL='http://search.ximalaya.com'
RESOURCE_BASE_URL='http://mobile.ximalaya.com'

nickname=sys.argv[3]
title=sys.argv[2]
kw=sys.argv[1]

QUERY_PATH='/front/v1?core=all&kw='+kw+'&page=1&rows=30&spellchecker=true'

SEARCH_URL=BASEURL+QUERY_PATH

#根据nickname\title取资源id
r=requests.get(SEARCH_URL)

data=json.loads(r.text)

#相关资源列表
resource_list=data['album']['docs']



#####
#print resource_list[8]
#sys.exit(0)
#####
nickname=nickname.decode('utf-8')
title=title.decode('utf-8')
#kw=kw.decode('utf-8')
statmodule=u'全部_专辑'.encode('utf-8')

findflag=0
for resource in resource_list:
	values=resource.values()
	#if nickname in values and title in values:
	if nickname==resource['nickname'] and title == resource['title']:
		print "find article: "+title.encode('utf-8')+" read by "+nickname.encode('utf-8')
		findflag=1
		break
if findflag!=1:
	print "can't find the resource ,please check your arguments"
	print "you may find these resources listed below:"
	
	for resource in resource_list:
		
		if nickname in resource['nickname']:
			
			print resource['nickname'].encode('utf-8'),resource['title'].encode('utf-8')
	
	sys.exit(0)


print resource
#resource_id=resource_list[i]['id']
resource_id=resource['id']

#该资源的集数
tracks=resource['tracks']

if tracks %20==0:
	query_times=tracks//20
else:
	query_times=tracks//20+1

#########
#print resource_id
#print tracks
#sys.exit(0)
#########

resource_id=str(resource_id)

i=1
res_dict_list=[]
while i<=query_times:
	
	if i==1:
		
		RESOURCE_PATH=RESOURCE_BASE_URL+'/mobile/others/album/track?albumId='+resource_id+'&device=android&isAsc=true&pageId=1&pageSize=20&pre_page=0&source=3&statEvent=pageview/album@'+resource_id+'&statModule='+statmodule+'&statPage=search@'+kw+'&statPosition=1&url_from=2'
		
		r=requests.get(RESOURCE_PATH)
	else:
		
		RESOURCE_PATH_2=RESOURCE_BASE_URL+'/mobile/others/ca/album/track/'+resource_id+'/true/'+str(i)+'/20?albumId='+resource_id+'&count=20&device=android&isAsc=true&page=2&pre_page=0&statEvent=pageview/album@'+resource_id+'&statModule='+statmodule+'&statPage=search@'+kw+'&statPosition=2&url_from=1'
		r=requests.get(RESOURCE_PATH_2)
		
	data=json.loads(r.text)
	
	res_list=data['tracks']['list']
	
	for var in res_list:
		title=var['title']
		playurl=var['playUrl32']
		
		res_dict_list.append((title,playurl))
	
	i+=1

#########
#print res_dict_list
#sys.exit(0)
#########

num=0	
#执行下载操作，并重命名
while len(res_dict_list)!=0:
	
	res=res_dict_list.pop(0)
	
	print "now begin download "+res[0] +" :"+ res[1]
	try:
		num+=1
		finalname=res[0].encode('gbk')
	except:
		finalname=str(num)

	finally:
                #print "now begin download "+finalname +" :"+ res[1]
                  	
		os.system('wget -O "'+finalname+'".mp3 '+res[1].encode('utf-8') + " >>/dev/null")




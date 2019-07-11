#encoding=utf-8
import requests
import json
import sys
import os
import re

BOOKURL=sys.argv[1]
BASEURL="https://max.book118.com"
pngURL='https://max.book118.com/index.php?g=Home&m=Ajax&a=getPreviewData'

r=requests.get(BOOKURL,verify=False)
view_token=re.findall("view_token = '(.*?)';",r.text)[0]
aid=re.findall('/(.[^/]*?).shtm',BOOKURL)[0]
page_num=re.findall(u'id="pagenumber"> (\d{0,})页</span',r.text)[0]
#page_num=28

step=6

for i in range(283,int(page_num)+1,step):
	payload={"aid":int(aid),'page_number':i,'view_token':view_token}
	rtext=requests.post(pngURL,data=payload,verify=False)
	data_dict=json.loads(rtext.text)['data']
	for j in range(0,len(data_dict)):
		pngurl="https:"+data_dict[str(j+i)]
		#download &rename
		os.system('wget -O "'+str(i+j)+'".png ' +pngurl+" >>/dev/null")


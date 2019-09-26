#encoding=utf-8
import requests
import json
import sys
import os
import re
import time
import random

from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


BOOKURL=sys.argv[1]
BASEURL="https://max.book118.com"
pngURL='https://max.book118.com/index.php?g=Home&m=Ajax&a=getPreviewData'

r=requests.get(BOOKURL,verify=False)
view_token=re.findall("view_token = '(.*?)';",r.text)[0]

print view_token

aid=re.findall('/(.[^/]*?).shtm',BOOKURL)[0]
page_num=re.findall(u'id="pagenumber"> (\d{0,})页</span',r.text)[0]
#page_num=28

step=6

if len(sys.argv)==4:
	startpagenum=sys.argv[2]
	endpagenum=sys.argv[3]
else:
	startpagenum=1
	endpagenum=int(page_num)

#for i in range(103,108,step):
for i in range(int(startpagenum),int(endpagenum)+1,step):
	payload={"aid":int(aid),'page_number':i,'view_token':view_token}
	rtext=requests.post(pngURL,data=payload,verify=False)
        
        time.sleep(0.3*random.random()) 
	
	data_dict=json.loads(rtext.text)['data']
	print("now running the urls list below:")
        print(data_dict)
	for j in range(0,len(data_dict)):
		print("now begin download:"+data_dict[str(j+i)])
		pngurl="https:"+data_dict[str(j+i)]
		#download &rename
		os.system('wget -O "'+str(i+j)+'".png ' +pngurl+" >>/dev/null")


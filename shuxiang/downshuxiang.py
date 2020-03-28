#encoding=utf-8

import requests
import time
import random
import os,sys
import hashlib

#usage:python downshuxiang.py http://shuxiang.chineseall.cn/v3/book/read/Mu4yg/pdf/ 326

#bookurl='http://shuxiang.chineseall.cn/v3/book/content/isCmg/pdf/'
#totalpage=326
#cookies={'_Tvt5MJ89bV_':'605E52501D5E3259C8B628C3A95B7B3DB143DAAE10C929CE9CA9D88DB5991EA6FFCAF2B47CD0A00F1F9B6E34EFE097E19B14A0EE5D082FBE891C6FAE0253BFCC'}

headers={'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36',\
'Host': 'shuxiang.chineseall.cn','Referer':'http://shuxiang.chineseall.cn/sso/login.jsps?redirectUrl=http://shuxiang.chineseall.cn',\
'Content-Type':'application/x-www-form-urlencoded'}

#login
s=requests.Session()

userpass=hashlib.md5('Sxmymm4321').hexdigest()
data={"userName":"tschenping","userPass":userpass,"redirectUrl":"http://shuxiang.chineseall.cn"}
loginurl='http://shuxiang.chineseall.cn/sso/ajaxLogin.action'

r=s.post(loginurl,data=data)



bookurl=sys.argv[1]
totalpage=int(sys.argv[2])

bookurl=bookurl.replace('read','content')
bookurl=bookurl.replace('PDF','pdf')

def progressbar(processnum,totalnum):
    scale=int(float(processnum)/totalnum*100)
    j = '#'*scale
    print('【'+j+'】->'+str(scale)+'%\r'),
    sys.stdout.flush()

try:
	for i in range(1,totalpage+1):
	
		delay=random.randint(1,10)*0.3

		time.sleep(delay)
		
		atime=time.time()*1000
		requesttime=str(atime).split('.')[0]
 
	
		#rtext=requests.get(bookurl+str(i)+'?t='+str(requesttime),headers=headers,cookies=cookies)
	
		rtext=s.get(bookurl+str(i)+'?t='+str(requesttime))		

		with open(str(i)+'.pdf','wb') as f:
			f.write(rtext.content)

		progressbar(i,totalpage)
except Exception as e:
	print(e)
finally:

	print("")
	#logout
	s.get('http://shuxiang.chineseall.cn/sso/logout.jsps')

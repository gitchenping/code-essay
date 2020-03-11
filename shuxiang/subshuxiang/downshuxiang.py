import requests
import time
import random


bookurl='http://shuxiang.chineseall.cn/v3/book/content/vXPgj/pdf/'
totalpage=161
cookies={'_Tvt5MJ89bV_':'D2EC06A3BF71EE91B2B1A3EC6EC3C33D37DAE10E4CCBD6F1B9B0C6CEBA9870A661A34AF09537213EB7BEE5A8A5C8A4580466BE1BCF9373DA21C655215BE0D4B1'}

headers={'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36','Host': 'shuxiang.chineseall.cn'}


for i in range(1,totalpage+1):
	
	delay=random.randint(1,10)*0.3

	time.sleep(delay)
		
	atime=time.time()*1000
	requesttime=str(atime).split('.')[0]
 
	
	rtext=requests.get(bookurl+str(i)+'?t='+str(requesttime),headers=headers,cookies=cookies)

	with open(str(i)+'.pdf','wb') as f:
		f.write(rtext.content)

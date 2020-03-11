import requests
import time
import random


bookurl='http://shuxiang.chineseall.cn/v3/book/content/Mu4yg/pdf/'
totalpage=326
cookies={'_Tvt5MJ89bV_':'A22737851ECCDF52E474A79D3DFC1295DBB1F631052C5F70829F790B2658FD8B43177294B373FF4AAD047522759976B85B358008E3DD44F52E217B7412EB142E'}

headers={'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36','Host': 'shuxiang.chineseall.cn'}


for i in range(1,totalpage+1):
	
	delay=random.randint(1,10)*0.3

	time.sleep(delay)
		
	atime=time.time()*1000
	requesttime=str(atime).split('.')[0]
 
	
	rtext=requests.get(bookurl+str(i)+'?t='+str(requesttime),headers=headers,cookies=cookies)

	with open(str(i)+'.pdf','wb') as f:
		f.write(rtext.content)

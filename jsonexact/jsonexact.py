import json
import requests

with open('audios','r') as fp:
     datadict=json.load(fp) 

contentlist=datadict['data']['list']

#key:value
contentdict={}
for content in contentlist:
	if content['article_title'] not in contentdict:
		#contentdict=content['article_title']
		contentdict[content['article_title']]=content['audio_download_url']

#print(contentdict)

for key,value in contentdict.items():
	
	r=requests.get(value)
	with open(key+'.mp3'.encode('utf-8'),'wb') as fp:
		fp.write(r.content)

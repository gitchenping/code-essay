#encoding=utf-8
import sys,os
import requests
import time,random
import re

#headers
user_agent="Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36"

heads={"User-Agent":user_agent,"Referer":"https://mail.qq.com/"}

start_url=sys.argv[1]

r=requests.get(start_url,headers=heads)      #里面发生重定向一次

print r.encoding
#取重定向页面url
first_redict_url=r.history[0].__dict__['headers']['Location']
#域名
domain_redict_url=re.findall('(.*?)(?:/n.*)',first_redict_url)[0]

#取cookie
cookie=dict(r.cookies)
#新的请求头
newheads={"User-Agent":user_agent,"Referer":first_redict_url}

#页码范围
pagerange=re.findall('ps:\'(.*?)\',',r.text)[0].split('-')
startpage=pagerange[0]
endpage=pagerange[1]

#title
title=re.findall(u'<title>(.*?)</',r.text)[0]


tar_file=title+"_"+startpage+"-"+endpage+".tar.gz"
 
jpgpath=re.findall('jpgPath:"(.*?)",',r.text)[0]


#clear previous png
os.system("cd /newtest;rm -f *.png")

#

for i in range(int(startpage),int(endpage)+1):
    
    stri=str(i)
    if len(stri)==1:
        strpngnum="00000"+stri
    elif len(stri)==2:
        strpngnum="0000"+stri
    else:
        strpngnum="000"+stri
        
    downurl=domain_redict_url+jpgpath+strpngnum+"?zoom=0"
    rr=requests.get(downurl,headers=newheads,cookies=cookie)
    with open("/newtest/"+str(i)+".png","w") as fp:
        fp.write(rr.content)
        print("第 %d 图片下载成功。"% i)
    time.sleep(5*random.random())

print("begin to tar ,tar name is "+tar_file)
os.system("cd /newtest;tar zcvf '"+tar_file.encode('utf-8')+"' *.png")

'''
#
http://www.xinyunfuwu.com/firsttransfer.jsp?enc=07c988faaf5c6bc76d7a3c866ca369bebef1365ff2f2b306e0fa70824eb59c7eff8fc0a8914efee67958df89a9cd02aaf43e39565a5cc676b88467134807b716972510f628d514bd8fcd8310521e4d08a3694447e96e47b2fa466d43bed1b10701986f970237d2353881601f5a637e2f1f684cd180f5a88a2d44deb2dbf02896a20eb35cb5b667f77a9ca7644e6aef74&unitid=7320

#
http://www.shehuikxzl.cn/n/drspath/book/base/96094284/a33ab62af5c94948870be3fe06adeb5e/2e373820eb2bf8fcd8c8c0cfa54fdf5f.shtml?bt=2019-07-18&dm=1145044855&et=2019-08-07&fid=54299368&username=tspingchen

http://www.shehuikxzl.cn/n/7119b8017b66b0352d0ecef6b7a7035fMC239571670327/img1/7F79F269C40514F8AECAE4A6D7D5562151A2CA3FBD6ACE93FD8FBFE8EDA1240B7CE1BEBAA886ADBFF3B326A676F632264981D2C5E09E75A26C396F9A14BE9DD354A31CC7392C1C1B5464A30C04CF2E91BF4138670E5C549E6D3C7D4C0204A514CA5820B58DA6E1C805FC95727B07B7B4E7AE/nf1/drs/96094284/A33AB62AF5C94948870BE3FE06ADEB5E/000261?zoom=07F79F269C40514F8AECAE4A6D7D5562151A2CA3FBD6ACE93FD8FBFE8EDA1240B7CE1BEBAA886ADBFF3B326A676F632264981D2C5E09E75A26C396F9A14BE9DD354A31CC7392C1C1B5464A30C04CF2E91BF4138670E5C549E6D3C7D4C0204A514CA5820B58DA6E1C805FC95727B07B7B4E7AE/nf1/drs/96094284/A33AB62AF5C94948870BE3FE06ADEB5E/000261?zoom=0


http://npng1.5read.com/image/ss2jpg.dll?did=nf1&pid=7F79F269C40514F8AECAE4A6D7D5562151A2CA3FBD6ACE93FD8FBFE8EDA1240B7CE1BEBAA886ADBFF3B326A676F632264981D2C5E09E75A26C396F9A14BE9DD354A31CC7392C1C1B5464A30C04CF2E91BF4138670E5C549E6D3C7D4C0204A514CA5820B58DA6E1C805FC95727B07B7B4E7AEA33AB62AF5C94948870BE3FE06ADEB5E&jid=/000261.jpg&a=E41541EAA65658C3BD81C3CFE152CFCD3337B2ECE1F2559C58B54904023066EF857C7DCFF399DC9F5D368FFEE6B2A9386AFFC03A477B3AB2B3225DD2333DF62AADE3&zoom=0&f=0
'''

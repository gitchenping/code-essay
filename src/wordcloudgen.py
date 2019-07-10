#encoding=utf-8

import os
from os import path
from PIL import Image
import numpy as np
import matplotlib.pyplot as plt

from wordcloud import WordCloud, STOPWORDS, ImageColorGenerator

# Read the whole text.
workdir=os.getcwd()
text = open(workdir+'/wordcollections.txt').read()

#read mask picure,词云默认是矩形的，本代码采用图片作为蒙版

maskimage=np.array(Image.open(workdir+'/pythonpng.png'))

# 设置停用词,即过滤掉不希望出现在词云上的词
stopwords = set(STOPWORDS)
stopwords.add("new")

#创建wc对象，mask参数指定了词云形状
wc = WordCloud(scale=4,background_color="white", max_words=2000, mask=maskimage,
               stopwords=stopwords, max_font_size=40, random_state=42)

# generate word cloud
wc.generate(text)

#下面两句按照给定的图片底色布局生成字体颜色策略，可省略
image_colors = ImageColorGenerator(maskimage)
wc.recolor(color_func=image_colors)
#显示、保存词云
#plt.imshow(wc)
#plt.axis("off") 
#plt.show()
wc.to_file('result.png') 



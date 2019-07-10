#encoding=utf-8

import os
from os import path
from PIL import Image
import numpy as np
import matplotlib.pyplot as plt

from wordcloud import WordCloud, STOPWORDS, ImageColorGenerator,get_single_color_func


class GroupedColorFunc(object):
    """Create a color function object which assigns DIFFERENT SHADES of
       specified colors to certain words based on the color to words mapping.

       Uses wordcloud.get_single_color_func

       Parameters
       ----------
       color_to_words : dict(str -> list(str))
         A dictionary that maps a color to the list of words.

       default_color : str
         Color that will be assigned to a word that's not a member
         of any value from color_to_words.
    """

    def __init__(self, color_to_words, default_color):
        self.color_func_to_words = [
            (get_single_color_func(color), set(words))
            for (color, words) in color_to_words.items()]

        self.default_color_func = get_single_color_func(default_color)

    def get_color_func(self, word):
        """Returns a single_color_func associated with the word"""
        try:
            color_func = next(
                color_func for (color_func, words) in self.color_func_to_words
                if word in words)
        except StopIteration:
            color_func = self.default_color_func

        return color_func

    def __call__(self, word, **kwargs):
        return self.get_color_func(word)(word, **kwargs)



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

#自定义单词的颜色
color_to_words = {
    # words below will be colored with a green single color function
    '#00ff00': ['file', 'try', 'list', 'sparse',
                'os', 'object', 'dict',
                'sys'],
    # will be colored with a red single color function
    'red': ['python', 'class', 'complex', 'complicated', 'nested',
            'if', 'special', 'errors']
}

#不属于上述设定的颜色词的词语会用灰色来着色
default_color = 'grey'
# Create a color function with multiple tones
grouped_color_func = GroupedColorFunc(color_to_words, default_color)


wc.recolor(color_func=grouped_color_func)
#显示、保存词云
#plt.imshow(wc)
#plt.axis("off") 
#plt.show()
wc.to_file('result.png') 



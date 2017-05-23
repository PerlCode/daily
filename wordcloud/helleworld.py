import imread
import wordcloud
bg_pic = imread.imread('22.jpg')
wc = wordcloud.WordCloud(
    background_color = 'white',
    max_words=200,
    max_font_size=100,
    mask=bg_pic
)
text = open('JaneEyre.txt').read()
wc.generate(text)
wc.to_file('word22.png')
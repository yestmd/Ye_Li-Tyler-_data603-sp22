#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests as req
import re
from lxml import html


# In[2]:


#choose html version of the book for this homework

content_html=req.get("https://www.gutenberg.org/files/2600/2600-h/2600-h.htm")
tree=html.fromstring(content_html.content)

#only select title, author, chapter number and content for analysis
book=tree.xpath('//h1/text()') + tree.xpath('//h2[@class="no-break"]/text()') + tree.xpath('//div[@class="chapter"]/h2/text()') + tree.xpath('//div[@class="chapter"]/p/text()')    

#string data cleanup
book=' '.join(book).lower()
book=re.sub(r'[^\w\s]', '', book).replace('\r\n','')
word=book.split()

#count unique word number
print("unique word number:" + str(len(set(word))))


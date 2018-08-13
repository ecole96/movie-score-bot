# cinemascore.py
# Licensed under the MIT License
# github: https://github.com/xenonnsmb/cinemascore.py
import requests
import re
from base64 import b64encode
from lxml import html

def search(query):
	encodedQuery = b64encode(query.encode('ascii')).decode('utf-8')
	response = html.fromstring(requests.get("https://www.cinemascore.com/publicsearch/ajax/title/" + encodedQuery).content)
	movies = response.xpath('//td[@class="movie"]/text()')
	scores = response.xpath('//img/@alt')
	returndict = {}
	x = 0
	while x != len(movies):
		movie = movies[x]
		score = scores[x]
		returndict[movie] = score
		x += 1
	return returndict
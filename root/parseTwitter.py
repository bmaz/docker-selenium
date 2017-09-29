#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Mon Dec 06 11:54:04 2016

@author: zpehlivan@ina.fr

It is a test to get all tweets for a query by using advanced search page of twitter.

"""

from pyvirtualdisplay import Display
from selenium import webdriver
from bs4 import BeautifulSoup
import time 
from selenium.webdriver.firefox.firefox_binary import FirefoxBinary
from datetime import  datetime, timedelta
import os 
import random
# from twython import Twython
# from config import *
import urllib.request
import csv
# key = ACCESS[0]
# twitter = Twython(
#         key["consumer_key"],
#         key["consumer_secret"],
#         key["oauth_token"],
#         key["oauth_token_secret"]
#         )


                                       
# os.environ['HTTP_PROXY'] = ''
# os.environ['HTTPS_PROXY'] = ''
# os.environ['NO_PROXY'] = '127.0.0.1,localhost'

display = Display(visible=0, size=(800, 600))
display.start()


def getDriver(user_agent):
    # path = "/usr/local/bin/geckodriver"
    # binary = FirefoxBinary(r'C:\Program Files (x86)\Mozilla Firefox\firefox.exe')
    profile = webdriver.FirefoxProfile()
    # profile.set_preference("network.proxy.type", 1)
    # profile.set_preference("network.proxy.socks", "127.0.0.1")
    # profile.set_preference("network.proxy.socks_port", 9050)
    # profile.set_preference("network.proxy.https", "firewall.ina.fr")
    # profile.set_preference("network.proxy.https_port", 81)
    # profile.set_preference("network.proxy.ssl",  "firewall.ina.fr")
    # profile.set_preference("network.proxy.ssl_port", 81)
    profile.set_preference("general.useragent.override", user_agent)
    profile.set_preference( "permissions.default.image", 2 )
    profile.update_preferences()
    
    driver = webdriver.Firefox(firefox_profile=profile)
    # executable_path=path
    # driver = webdriver.Chrome()
    return driver


def scrollPage(driver):

	lastHeight = driver.execute_script("return document.body.scrollHeight")
	driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
	delta = 0
	while True:
		driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
		newHeight = driver.execute_script("return document.body.scrollHeight")
		if newHeight == lastHeight:
			time.sleep(random.uniform(0.5 + delta, 1 + delta))
			delta += 0.2
			newHeight = driver.execute_script("return document.body.scrollHeight")
			if newHeight == lastHeight:
				break
		lastHeight = newHeight
	return driver


def getTweetIds(driver, url):
	
	page_source = driver.page_source
	share_types = ["retweet", "reply", "like"]
	tweetids= []
	soup = BeautifulSoup(page_source, "lxml")
	for tweet in soup.findAll("li"):
		if tweet.has_attr('data-item-id'):
			retweet = {"urls":{"url": url}}
			retweet["id"] = str(tweet['data-item-id'])
			for chunk in tweet.findAll("div"):
				if chunk.has_attr("class"):
					if chunk["class"] == ['stream-item-header']:
						for part in chunk.findAll("a"):
							if part.has_attr("title"):
								retweet["created_at"] = part["title"]
								# also available : "data-time" or "data-time-ms"
					elif chunk["class"] == ['js-tweet-text-container']:
						for part in chunk.findAll("a"):
							if part.has_attr("data-expanded-url"):
								retweet["urls"]["expanded_url"] = unshorten_url(str(part['data-expanded-url']))
					elif chunk["class"] == ["stream-item-footer"]:
						for part in chunk.findAll("span"):
							if part.has_attr("data-aria-label-part"):
								for text in share_types:
									if text in part.get_text():
										retweet[text + "_count"] = part.parent["data-tweet-stat-count"]
								if "replies" in part.get_text():
									retweet["reply_count"] = part.parent["data-tweet-stat-count"]
				if chunk.has_attr('data-screen-name'):
					retweet["user_screen_name"] = str(chunk['data-screen-name'])
				# retweet.append(str(chunk))
				if chunk.has_attr('data-retweet-id'):
					retweet["retweet_id"] = str(chunk['data-retweet-id'])
					retweet["retweet_screen_name"] = str(chunk['data-retweeter'])
				if chunk.has_attr("data-is-reply-to"):
					if str(chunk['data-is-reply-to']) == "true":
						# Information about original tweet is not available in page html.
						# I use Twitter API to get the original tweet's ID
						# try:
						# 	reply_to = twitter.show_status(id=retweet["id"])
						# except Exception as error:
						# 	print(error)
						reply_to={}
						reply_to["in_reply_to_status_id"]="unknown"
						reply_to["in_reply_to_screen_name"]="unknown"

						retweet["reply_id"] = retweet["id"]
						retweet["reply_screen_name"] = retweet["user_screen_name"]
						retweet["id"] = reply_to["in_reply_to_status_id"]
						retweet["user_screen_name"] = reply_to["in_reply_to_screen_name"]

			if "reply_id" in retweet:
				retweet["reply_created_at"] = retweet["created_at"]
				retweet.pop("created_at")
			if "retweet_id" in retweet:
				for text in share_types:
					if text + "_count" in retweet:
						retweet.pop(text + "_count")
			else:
				for text in share_types:
					if text + "_count" not in retweet:
						retweet[text + "_count"] = 0
			tweetids.append(retweet)

	return tweetids

def unshorten_url(url):
	try:
		response = urllib.request.urlopen(url, data=None, timeout=10)
		url = response.geturl()
	except urllib.error.URLError:
		pass
	except urllib.error.HTTPError:
		pass
	except UnicodeEncodeError:
		pass
	except Exception as error:
		print(error)
	return url


# def getDateEnd(interval, date_start):
#     u = datetime.strptime(date_start, "%Y-%m-%d") 
#     d = timedelta(days=interval)
#     t = u + d
#     return t.strftime( "%Y-%m-%d"),t
   
def getUrl(row, user_agents, filedir):
	itemlist = []
	if len(row) > 3 :
		row[2] = ",".join(row[2:])
		row = row[0:3]
	url = "https://twitter.com/search?f=tweets&vertical=default&q=" + row[2] + "%20include%3Anativeretweets&src=typd"
	agent = random.choice(user_agents)
	# need to create a new driver to change user-agent 
	driver = getDriver(agent)
	driver.get(url)
	# driver.find_element_by_link_text("Latest").click()
	driver = scrollPage(driver)
	itemlist = getTweetIds(driver, row[2])
	driver.quit()
	driver = None
	return itemlist

        # time.sleep(random.choice(list(range(5, 15,1))))

# def getUrlList(interval, query, date_start,date_stop):
    
#     urls =[]
#     date_stop_time = datetime.strptime(date_stop, "%Y-%m-%d") 
#     date_end, dateTo = getDateEnd(interval, date_start)
# #    print(date_start, dateTo)
# #    print((datenow - dateTo ).days)
#     while (date_stop_time - dateTo ).days > 0:
#         url = "https://twitter.com/search?f=tweets&vertical=default&q=" + query + "%20since%3A" + date_start +  "%20until%3A" + date_end + " include%3Anativeretweets"
#         urls.append(url)
#         date_start = date_end
#         date_end,dateTo= getDateEnd(interval, date_start)    
# #        print(date_start, dateTo)
        
        
#     date_end = date_stop
#     url = "https://twitter.com/search?f=tweets&vertical=default&q=" + query + "%20since%3A" + date_start +  "%20until%3A" + date_end + " include%3Anativeretweets"
#     urls.append(url)
#     date_start = date_end
#     date_end,dateTo= getDateEnd(interval, date_start)
#     return urls
   
   
if __name__ == "__main__":
	user_agents = ["Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:55.0) Gecko/20100101 Firefox/55.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.101 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/603.3.8 (KHTML, like Gecko) Version/10.1.2 Safari/603.3.8",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36"]
	filedir = "/home/bmazoyer/Dev/Twitter/"
	infile = filedir + "OuestFranceUrls.csv"
	with open(infile) as csvfile:
		reader = csv.reader(csvfile)
		for row in reader:
			tweets = getUrl(row, user_agents,filedir)
			print(tweets)
# date_stop = "2013-12-31" 
	# date_start = "2012-12-31"
	# query = "http://www.lefigaro.fr/conjoncture/2013/11/05/20002-20131105ARTFIG00528-qui-se-cache-derriere-les-bonnets-verts.php"
	# urls = getUrlList(400, query, date_start, date_stop)
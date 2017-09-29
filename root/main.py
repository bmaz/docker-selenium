#!/usr/bin/env python
# -*- coding: utf-8 -*-

from queue import Queue
from queue import Empty
import threading
from parseTwitter import *
import smtplib
from datetime import datetime
 
# server = smtplib.SMTP('smtp.gmail.com', 587)
# server.starttls()
# server.login("beatrice.mazoyer@sciencespo.fr", "SciencesPoREYOZAM123.")

class PrintThread(threading.Thread):
    def __init__(self, queue, headers, file):
        threading.Thread.__init__(self)
        self.queue = queue
        self.headers = headers
        self.counter = 1
        self.file = file
        
    def run(self):
        fieldnames = ["id", "media", "url"]
        share_types = ["tweet", "retweet", "reply", "like"]
        for a in ["direct_", "indirect_"]:
            for b in share_types:
                fieldnames.append(a + b + "_count")
        if not self.headers:
            with open(self.file, 'a') as output:
                writer = csv.writer(output, delimiter=";", quoting= csv.QUOTE_ALL)
                writer.writerow(fieldnames)

        def printtime():
            timelapse = 60.0
            # set a timer calling the printtime() function itself 60 sec later
            threading.Timer(timelapse, printtime).start()
            print(self.counter)
            if self.counter == 0:
                # exit program if the counter wasn't incremented in 60 seconds
                exit.set()
            self.counter = 0
        printtime()
        while True:
            # wait 2 minutes for queue to send item and doesn't increment counter if nothing is sent
            result = self.queue.get(block=True, timeout=120)
            with open(self.file, 'a') as output:
                writer = csv.DictWriter(output, fieldnames=fieldnames, delimiter=";", quoting= csv.QUOTE_ALL)
                writer.writerow(result)
            # self.printfiles(result)
            print(result["direct_tweet_count"])
            self.queue.task_done()
            self.counter += 1

class PrintTweetsThread(threading.Thread):
    def __init__(self, queue, file):
        threading.Thread.__init__(self)
        self.queue = queue
        self.file = file

    def run(self):
        while True:
            # try:
            tweets = self.queue.get()
            # except Empty as error:
            #     print(error)
            #     msg = str(datetime.now()) + "\nAucun nouveau tweet depuis 1 minute."
            #     server.sendmail("beatrice.mazoyer@sciencespo.fr", "beatrice.mazoyer@gmail.com", msg)
            #     server.quit()
            #     exit.set()
            with open(self.file, 'a+') as fp:
                for item in tweets:
                    fp.write(str(item) + "\n")
            self.queue.task_done()


class ProcessThread(threading.Thread):
    def __init__(self, in_queue, tweets_queue, out_queue):
        threading.Thread.__init__(self)
        self.in_queue = in_queue
        self.out_queue = out_queue
        self.tweets_queue = tweets_queue

    def run(self):
        while True:
            row = self.in_queue.get()
            for attempt in range(2):
                try:
                    tweets_list = getUrl(row, user_agents, filedir)
                except Exception as error:
                    print("thread", self.name, error, row[2])
                    time.sleep(60)
                else:
                    break
            else:
            # python "for ... else" construction let us do something only if the for loop didn't break
                continue

            self.tweets_queue.put(tweets_list)
            shares_count = getTweetsStats(tweets_list)
            for (n, key) in enumerate(["id", "media", "url"]):
                shares_count[key] = row[n]
            self.out_queue.put(shares_count)
            self.in_queue.task_done()


def increment(stats, tweet, tweet_type):
    share_types = ["reply_count", "retweet_count", "like_count"]
    for text in share_types:
        stats[tweet_type + text] += int(tweet[text])
    stats[tweet_type + "tweet_count"] += 1

def getTweetsStats(tweets):
    empty_dict = {"reply_count":0, "retweet_count":0, "like_count":0, "tweet_count":0}
    stats = {"direct_" + k : v for k,v in empty_dict.items()}
    stats.update({"indirect_" + k : v for k,v in empty_dict.items()})
    for tweet in tweets:
        if "retweet_id" in tweet:
            pass
        elif "reply_id" in tweet:
            increment(stats, tweet, "indirect_")
        else:
            increment(stats, tweet, "direct_")
    return stats

if __name__ == "__main__":
    urlqueue = Queue()
    tweetsqueue = Queue()
    resultqueue = Queue()
    filedir = "/data/"
    outfile = "aggregated_results.csv"
    infile = "urls.csv"

    user_agents = [
    # "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:54.0) Gecko/20100101 Firefox/54.0",
    # "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:55.0) Gecko/20100101 Firefox/55.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/60.0.3112.78 Chrome/60.0.3112.78 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/59.0.3071.109 Chrome/59.0.3071.109 Safari/537.36"
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36",
    # "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0",
    # "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.101 Safari/537.36",
    # "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36",
    # "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.101 Safari/537.36"

    ]

    # Check already handled lines
    id_set = set()
    with open(filedir + outfile, 'r') as output:

        reader = csv.reader(output, delimiter=";", quoting= csv.QUOTE_ALL)
        # skip headers
        headers = next(reader, None)
        for row in reader:
            id_set.add(row[0])

    # Create exit event
    exit = threading.Event()

    # Create processing threads
    for i in range(0, 2):
        t = ProcessThread(urlqueue, tweetsqueue, resultqueue)
        t.daemon = True
        t.name = i
        t.start()

    # Create printing threads
    t = PrintThread(resultqueue, headers, filedir+outfile)
    t.daemon = True
    t.start()

    t = PrintTweetsThread(tweetsqueue, filedir+"tweets")
    t.daemon = True
    t.start()

    # Put urls in queue
    with open(filedir + infile) as csvfile:
        reader = csv.reader(csvfile, delimiter=";", quoting= csv.QUOTE_ALL)
        for row in reader:
            if row[0] not in id_set:
                urlqueue.put(row[0:3])

    if urlqueue.empty():
        print("all URLs treated.")
        exit.set()

    # Wait for queues to get empty
    exit.wait()
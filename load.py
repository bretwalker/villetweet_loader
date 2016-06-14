import datetime
import time
import random
from threading import Thread
import logging
import signal
import sys
import os
import simplejson as json
import requests
from redis import StrictRedis, Redis, ConnectionPool
from TwitterAPI import TwitterAPI

api = TwitterAPI(consumer_key, consumer_secret, access_token_key, access_token_secret)
red = StrictRedis(host='localhost', port=6379, db=0)

seen_tweets = 'seen_tweets'

profile_pic_prefix = 'pic_'

archive_set_suffux = ':30day:' # then add date

prolific_score = 'prolific_score'
prolific_updates = 'stream.prolific_updates'

mentioned_score = 'mentioned_score'
mentioned_updates = 'stream.mentioned_updates'

hashtag_score = 'hashtag_score'
hashtag_updates = 'stream.hashtag_updates'

recent_tweets = 'recent_tweets'
recent_tweets_updates = 'stream.recent_tweets_updates'

tweet_count = 'tweet_count'
tweet_count_updates = 'stream.tweet_count_updates'

def do_streaming(exception_cb):
    try:
        logging.info('Starting streaming thread.')
        r = api.request('statuses/filter', {'locations':'-85.95104,37.9971,-85.4051,38.374596'})
        for item in r.get_iterator():
            process_tweet(item)
    except:
        exception_cb(sys.exc_info())
        raise

def do_search(exception_cb):
    try:
        logging.info('Starting search thread.')
        api_sleep_time = 0
        while True:
            # Could add some pagination logic here, but I doubt we'll see 100+ tweets/3 sec in Louisville.
            r = api.request('search/tweets', {'q': '', 'geocode': '38.17,-85.70,20mi', 'result_type': 'recent', 'count': 100}).response
            if (r.status_code == 200):
                api_sleep_time = 0
                for t in r.json()['statuses']:
                    time.sleep(random.uniform(.1,.3))
                    process_tweet(t)
            elif(r.status_code == 420 or r.status_code == 429):
                api_sleep_time = api_sleep_time + 5
                time.sleep(api_sleep_time)
            else:
                log.error('Search API error: %s' % r.json())
                exception_cb('Search API error: %s' % r.json())
            time.sleep(3)
    except:
        exception_cb(sys.exc_info())
        raise

def get_keys(metric_name, today, days):
    keys = []
    for i in range(1,days):
        keys.append(metric_name + archive_set_suffux + (today + datetime.timedelta(-i)).strftime("%Y%m%d"))
    return keys

def process_tweet(item):
    if(red.sismember(seen_tweets, item['id'])):
        return
    with red.pipeline() as pipe:
        today = datetime.datetime.now()
        today_text = today.strftime("%Y%m%d")
        if not red.exists(prolific_score + archive_set_suffux + today_text):
            pipe.zunionstore(prolific_score + '_30drollup', get_keys(prolific_score, today, 30))
            pipe.zunionstore(mentioned_score + '_30drollup', get_keys(mentioned_score, today, 30))
            pipe.zunionstore(hashtag_score + '_30drollup', get_keys(hashtag_score, today, 30))

            pipe.zunionstore(prolific_score + '_7drollup', get_keys(prolific_score, today, 7))
            pipe.zunionstore(mentioned_score + '_7drollup', get_keys(mentioned_score, today, 7))
            pipe.zunionstore(hashtag_score + '_7drollup', get_keys(hashtag_score, today, 7))

            old_set_date_text = (today + datetime.timedelta(-31)).strftime("%Y%m%d")
            pipe.delete(prolific_score + archive_set_suffux + old_set_date_text)
            pipe.delete(mentioned_score + archive_set_suffux + old_set_date_text)
            pipe.delete(hashtag_score + archive_set_suffux + old_set_date_text)

        red.sadd(seen_tweets, item['id'])

        pipe.zincrby(prolific_score, item['user']['screen_name'], 1) # all-time
        pipe.zincrby(prolific_score + '_30drollup', item['user']['screen_name'], 1) # today's 30d rollup
        pipe.zincrby(prolific_score + '_7drollup', item['user']['screen_name'], 1) # today's 7d rollup
        pipe.zincrby(prolific_score + archive_set_suffux + today_text, item['user']['screen_name'], 1) # today's history
        pipe.set(profile_pic_prefix + item['user']['screen_name'], item['user']['profile_image_url'])
        pipe.publish(prolific_updates, item['user']['screen_name'])

        if item['entities']['user_mentions']:
            for tag_data in item['entities']['user_mentions']:
                pipe.zincrby(mentioned_score, tag_data['screen_name'], 1) # all-time
                pipe.zincrby(mentioned_score + '_30drollup', tag_data['screen_name'], 1) # today's 30d rollup
                pipe.zincrby(mentioned_score + '_7drollup', tag_data['screen_name'], 1) # today's 7d rollup
                pipe.zincrby(mentioned_score + archive_set_suffux + today_text, tag_data['screen_name'], 1) # today's history
                pipe.publish(mentioned_updates, tag_data['screen_name'])
        if item['entities']['hashtags']:
            for tag_data in item['entities']['hashtags']:
                pipe.zincrby(hashtag_score, tag_data['text'], 1) # all-time
                pipe.zincrby(hashtag_score + '_30drollup', tag_data['text'], 1) # today's 30d rollup
                pipe.zincrby(hashtag_score + '_7drollup', tag_data['text'], 1) # today's 7d rollup
                pipe.zincrby(hashtag_score + archive_set_suffux + today_text, tag_data['text'], 1) # today's history
                pipe.publish(hashtag_updates, tag_data['text'])

        tweet = json.dumps({
            'id': item['id'],
            'screen_name': item['user']['screen_name'],
            'text': item['text'],
            'profile_image_url': item['user']['profile_image_url'],
            'name': item['user']['name'],
#            'possibly_sensitive': item['possibly_sensitive'],
            'created_at': datetime.datetime.strptime(item['created_at'], "%a %b %d %H:%M:%S +0000 %Y").strftime("%Y-%m-%dT%H:%M:%SZ")
        }, ensure_ascii=False)

        pipe.lpush(recent_tweets,  tweet)
        pipe.ltrim(recent_tweets, 0, 199)
        pipe.publish(recent_tweets_updates,  tweet)

        pipe.incr(tweet_count)
        pipe.publish(tweet_count_updates,  "new_tweets")

        pipe.execute()

if __name__ == '__main__':
    def exeption_cb(exception):
        print exception
        os._exit(1)

    streaming_thread = Thread(target=do_streaming, args=(exeption_cb,))
    streaming_thread.daemon = True
    streaming_thread.start()

    search_thread = Thread(target=do_search, args=(exeption_cb,))
    search_thread.daemon = True
    search_thread.start()

    while(True):
        time.sleep(1)

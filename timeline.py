import tweepy as twp
import json, time, sys
import re
import pymongo as pm
from datetime import datetime
import time
import os


def init_connection():
    """
    Connect to MongoDB
    :return: Set Database connection handle globally
    """
    global db
    try:
        mongo_client = os.environ['TWR_MONGO_DB']
        conn = pm.MongoClient(mongo_client)
        db = conn.twitterTweets
        print("DB connected.")
    except BaseException as e:
        print("Connection error: " + str(e))


def get_latest():
    """
    To read only the latest tweets from user's screen. Get the ID of last collected tweet available in database.
    :return: Set the tweet ID
    """
    for k, v in cache.items():
        print("User screen name: ", k)
        doc = db.userTimeline.find({'username': k}).sort('tweetId', pm.DESCENDING).limit(1)
        for d in doc:
            if d:
                cache[k]["maxId"] = d['tweetId']
            else:
                print("ID not found :", k)


def get_rate_limit():
    """
    Get number of remaining API calls allowed to read user_timeline end point
    :return: Count of remaining calls
    """
    data = api.rate_limit_status()
    # print('wrote', data['resources']['statuses']['/statuses/user_timeline']['remaining'])  # verify the end point key to read
    limit = data['resources']['statuses']['/statuses/user_timeline']['remaining']
    return limit


def get_tweets(screen_name):
    """
    Collect latest tweets from twitter users' screen
    :param screen_name: Twitter users' handle
    :return: None. Insert the tweets into Mongo DB
    """
    limit = get_rate_limit()

    # user_timeline end point takes approx 16 request page page
    if limit < 200:
        print('Sleeping in get_tweets for 15 mins.')
        time.sleep(1000)
        limit = get_rate_limit()

    print('Before limit: ', limit)

    new_tweets = twp.Cursor(api.user_timeline, screen_name=screen_name).items()

    i = 0
    maxId = cache[screen_name]["maxId"]
    for status in new_tweets:
        if i == 0:
            print("Tweet/Status id: ", cache[screen_name]["maxId"])

        if cache[screen_name]["flag"] == True or status.id > maxId:
            if status.id == cache[screen_name]["maxId"]:  # Break when fetching old tweets
                print("Old tweets ID: ", status.id)
                break

            if not cache[screen_name]["flag"]:  # Set for first tweet
                cache[screen_name]["flag"] = True
                maxId = status.id
                i += 1
                db.userTimeline.insert_one({'username': screen_name, 'tweetId': status.id, \
                                            'created_at': datetime.now(), '_json': status._json})
                print("Id: ", status.id)

            else:
                db.userTimeline.insert_one({'username': screen_name, 'tweetId': status.id, \
                                            'created_at': datetime.now(), '_json': status._json})
                i += 1

        else:
            print("Old tweets ID: ", status.id)
            break  # No new tweets found.

    if cache[screen_name]["flag"]:
        cache[screen_name]["flag"] = False  # Completed reading user's timeline
    cache[screen_name]["maxId"] = maxId  # ID of latest tweet

    limit = get_rate_limit()
    print('After limit: ', limit)

    assert (status.id != cache[screen_name]["maxId"]), "Tweet MaxID error"
    print("Scree name: {} New tweets: {} {}".format(screen_name, i, datetime.now))


if __name__ == "__main__":

    ACCESS_TOKEN = os.environ['TWR_ACCESS_TOKEN']
    ACCESS_SECRET = os.environ['TWR_ACCESS_SECRET']
    CONSUMER_KEY = os.environ['TWR_CONSUMER_KEY']
    CONSUMER_SECRET = os.environ['TWR_CONSUMER_SECRET']

    OAUTH_KEYS = {
        'consumer_key': CONSUMER_KEY, 'consumer_secret': CONSUMER_SECRET,
        'access_token_key': ACCESS_TOKEN, 'access_token_secret': ACCESS_SECRET
    }

    auth = twp.OAuthHandler(OAUTH_KEYS['consumer_key'], OAUTH_KEYS['consumer_secret'])
    auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
    api = twp.API(auth)

    # Set screen name of twitter accounts to collect tweets from their timeline
    user_handles = '{"CBinsights": {"maxId": -1, "flag": false}}'
    cache = json.loads(user_handles)

    init_connection()
    get_latest()  # Update the latest ID

    while True:
        try:
            start_time = datetime.now()
            for k, v in cache.items():
                get_tweets(k)

            end_time = datetime.now()
            diff = end_time - start_time

            if diff.seconds < 500:
                print("No bulk tweets found. Sleeping for 15 min.")
                time.sleep(1000)

        except BaseException as e:
            print("Unknown error: ", str(e))
            print('Sleeping in main for 20 min. :(')
            time.sleep(1200)

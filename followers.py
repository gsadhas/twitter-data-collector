import tweepy as twp
import json, time, sys
import re
import pymongo as pm
from datetime import datetime
import time
import os


def init_connection():
    global db
    try:
        mongo_client = os.environ['TWR_MONGO_DB']
        conn = pm.MongoClient(mongo_client)
        db = conn.twitterTweets
        print("DB connected.")
    except BaseException as e:
        print("Connection error: ", str(e))


def get_rate_limit():
    """
    Get number of remaining API calls allowed to invoke followers end point
    :return: remaining number of API calls
    """
    data = api.rate_limit_status()
    limit = data['resources']['followers']['/followers/list']['remaining']  # user followers
    return limit


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
    api = twp.API(auth, wait_on_rate_limit=True)

    init_connection()

    # include list of twitter screen names to get their follower's profile
    experts = ["businessinsider", "LetsTalkPaymnts"]

    while True:
        try:
            for useracc in experts:
                count = 0
                dup = 0
                print("Logging for user account: {}".format(useracc))
                try:
                    users = twp.Cursor(api.followers, screen_name=useracc).items()
                    for user in users:
                        count += 1
                        res = db.followers.find({'follower_json.screen_name': user.screen_name, 'source': useracc}).limit(1)
                        if res.count() > 0:
                            dup += 1
                            print("No of duplicate record found: {}".format(dup))
                        else:
                            db.followers.insert_one({'source': useracc, 'created_at': datetime.now(), 'follower_json': user._json})

                        if count % 1000 == 0:
                            print("No. of {} followers profile found : {} ".format(useracc, count))

                except StopIteration:
                    print("{} total followers: {}".format(useracc, count))
            break

        except BaseException as e:
            print("Unknown error in main: ", str(e))
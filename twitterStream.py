import tweepy as twp
import json, time, sys
import re
import pymongo as pm
import time
import os

class TwitterListener(twp.StreamListener):
    def __init__(self, api=None):
        self.api = api
        self.counter = 0
        mongo_client = os.environ['TWR_MONGO_DB']
        self.conn = pm.MongoClient(mongo_client)
        self.db = self.conn.twtstream
        self.words_to_track = ['weather', 'snow', 'winter', 'spring', 'thunderstroms', 'thunderstrom',
                     'rain', 'raining', 'strom', 'thunder', 'wind', 'tornado']

    def on_status(self, status):
        """
        Collect tweets from twitter live stream. Filter english language tweets that have given key words
        :param status: tweets from live stream
        :return: None
        """
        if status._json['lang'] == "en":
            if status.retweeted:
                return
            track = self.words_to_track
            if any(x in status._json['text'] for x in track):
                self.db.weather.insert(status._json)
                self.counter += 1
                if self.counter % 1000 == 0:
                    print('Records inserted: ', self.counter)
                return True
        else:
            return

    def on_error(self, status_code):
        if status_code == 420:
            # returning False in on_data disconnects the stream
            print("Error at on_error:", status_code)
            return False

    def on_limit(self, track):
        print("On limit: ", track)
        time.sleep(60 * 15)
        return

    def on_timeout(self):
        time.sleep(60)
        return


if __name__ == "__main__":
    ACCESS_TOKEN = os.environ['TWR_ACCESS_TOKEN']
    ACCESS_SECRET = os.environ['TWR_ACCESS_SECRET']
    CONSUMER_KEY = os.environ['TWR_CONSUMER_KEY']
    CONSUMER_SECRET = os.environ['TWR_CONSUMER_SECRET']

    OAUTH_KEYS = {
        'consumer_key': CONSUMER_KEY, 'consumer_secret': CONSUMER_SECRET,
        'access_token_key': ACCESS_TOKEN, 'access_token_secret': ACCESS_SECRET
    }
    while True:
        try:
            auth = twp.OAuthHandler(OAUTH_KEYS['consumer_key'], OAUTH_KEYS['consumer_secret'])
            auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)

            stream_listener = TwitterListener()
            stream = twp.Stream(auth, listener=stream_listener)
            # Specify location to track; Coordinates position: SouthWest Corner(Long, Lat), NorthEast Corner(Long, Lat)
            stream.filter(locations=[-122.75, 36.8, -121.75, 37.8]) #San Fransico

        except Exception as e:
            print("Error !", str(e))
            print("Disconnecting from stream.")
            stream.disconnect()


# Twitter data collection using Tweepy and MongoDB
This repo has Python scripts to collect data from Twitter using tweepy framework. Click here for <a href="http://docs.tweepy.org/en/v3.6.0/getting_started.html"> Tweepy introduction</a>, and <a href="https://github.com/tweepy/tweepy">Tweepy Github repo</a>

The scripts collect tweets from Twitter user's timeline and Twitter live stream. In addition, it dumps all the followers profile in the given user's timeline. 
- timeline.py - collect tweets from user's timeline
- twitterStream.py - collect tweets from Twitter live stream with filters such as geo locations and key words
- followers.py - collect followers profile of the Twitter user

#### How to run
- Things to do before running the scripts,
  - Please set the Twitter credentials in envrionment variables
  - All the data from Twitter are stored in MongoDB. You need to set your MongoDB client in init_connection()
- timeline.py and followers.py - Modify your Twitter users's screen name in respective location where it is commented. By default, example screen names included
- twitterStream.py - Change the lat/lon coordinates and key words list according to you need. Default lat/lon coordinates and tweets key words included in the file
- Run in the terminal!

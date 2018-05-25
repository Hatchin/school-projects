import sys
import tweepy
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


def loadkeys(filename):
    """"
    load twitter api keys/tokens from CSV file with form
    consumer_key, consumer_secret, access_token, access_token_secret
    """
    with open(filename) as f:
        items = f.readline().strip().split(', ')
        return items


def authenticate(twitter_auth_filename):
    """
    Given a file name containing the Twitter keys and tokens,
    create and return a tweepy API object.
    """
    consumer_key, consumer_secret, \
    access_token, access_token_secret \
        = loadkeys(twitter_auth_filename)
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)
    return api


def fetch_tweets(api, name):
    """
    Given a tweepy API object and the screen name of the Twitter user,
    create a list of tweets where each tweet is a dictionary with the
    following keys:

       id: tweet ID
       created: tweet creation date
       retweeted: number of retweets
       text: text of the tweet
       hashtags: list of hashtags mentioned in the tweet
       urls: list of URLs mentioned in the tweet
       mentions: list of screen names mentioned in the tweet
       score: the "compound" polarity score from vader's polarity_scores()

    Return a dictionary containing keys-value pairs:

       user: user's screen name
       count: number of tweets
       tweets: list of tweets, each tweet is a dictionary
       
    """
    accountdic = {}
    accountdic['user'] = name
    accountdic['count'] = api.get_user(name).statuses_count

    analyser = SentimentIntensityAnalyzer()
    twlist = []
    for status in tweepy.Cursor(api.user_timeline, id=name).items(100):
        tweetdic = {}
        tweetdic['id'] = status.id
        tweetdic['created'] = status.created_at
        tweetdic['retweeted'] = status.retweet_count
        tweetdic['text'] = status.text
        hashtaglist = []
        for hashtag in status.entities.get('hashtags'):
            hashtaglist.append(hashtag['text'])
        tweetdic['hashtags'] = hashtaglist
        urllist = []
        for url in status.entities.get('urls'):
            urllist.append(url['url'])
        tweetdic['urls'] = urllist
        mentionlist = []
        for mention in status.entities.get('user_mentions'):
            mentionlist.append(mention['screen_name'])
        tweetdic['mentions'] = mentionlist
        score = analyser.polarity_scores(status.text)
        tweetdic['score'] = score['compound']
        twlist.append(tweetdic)

    accountdic['tweets'] = twlist
    return accountdic


def fetch_following(api,name):
    """
    Given a tweepy API object and the screen name of the Twitter user,
    return a a list of dictionaries containing the followed user info
    with keys-value pairs:

       name: real name
       screen_name: Twitter screen name
       followers: number of followers
       created: created date (no time info)
       image: the URL of the profile's image

    To collect data: get a list of "friends IDs" then get
    the list of users for each of those.
    """
    friendlist = []
    for id in api.friends_ids(name):
        friend = api.get_user(id)
        frddic = {}
        frddic['name'] = friend.name
        frddic['screen_name'] = friend.screen_name
        frddic['followers'] = friend.followers_count
        frddic['created'] = friend.created_at.date()
        frddic['image'] = friend.profile_image_url_https
        friendlist.append(frddic)
    return friendlist

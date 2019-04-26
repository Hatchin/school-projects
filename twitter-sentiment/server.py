import sys
from flask import Flask, render_template
from tweetie import *
from colour import Color

from numpy import median

app = Flask(__name__)

def add_color(tweets):
    """
    Given a list of tweets, one dictionary per tweet, add
    a "color" key to each tweets dictionary with a value
    containing a color graded from red to green. 
    """
    colors = list(Color("red").range_to(Color("green"), 100))
    for t in tweets:
        print t
        score = t['score']
        colorscore = (score + 1) / 2 * 100
        color = colors[int(colorscore)]
        t['color'] = color

    return tweets


@app.route("/<name>")
def tweets(name):
    "Display the tweets for a screen name color-coded by sentiment score"
    record = fetch_tweets(api,name)
    record['tweets'] = add_color(record['tweets'])
    score = []
    for element in record['tweets']:
        score.append(element['score'])
    medianscore = median(score)
    return render_template('tweets.html', record = record, medianscore = medianscore)


@app.route("/following/<name>")
def following(name):
    """
    Display the list of users followed by a screen name, sorted in
    reverse order by the number of followers of those users.
    """
    friendlist = fetch_following(api, name)
    newlist = sorted(friendlist, key = lambda k:k['followers'], reverse = True)
    return render_template('following.html', friends = newlist, name = name)


i = sys.argv.index('server:app')
twitter_auth_filename = sys.argv[i+1] # e.g., "/Users/parrt/Dropbox/licenses/twitter.csv"
# twitter_auth_filename = sys.argv[1]
api = authenticate(twitter_auth_filename)

# app.run(host='0.0.0.0')

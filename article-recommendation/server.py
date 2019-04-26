from flask import Flask, render_template
from doc2vec import *
import sys

app = Flask(__name__)

@app.route("/")
def articles():
    """Show a list of article titles"""
    return render_template('articles.html', article_list = articles)

@app.route("/article/<topic>/<filename>")
def article(topic,filename):
    """
    Show an article with relative path filename. Assumes the BBC structure of
    topic/filename.txt so our URLs follow that.
    """
    articlename = '/%s/%s' %(topic, filename)
    for a in articles:
        if a[0] == articlename:
            article = a
    recommend = recommended(article, articles, 5)
    title = article[1]
    textlist = article[2].split('\n\n')
    return render_template('article.html', title = title, textlist = textlist, rec_list = recommend)


# initialization
i = sys.argv.index('server:app')
glove_filename = sys.argv[i+1]
articles_dirname = sys.argv[i+2]

gloves = load_glove(glove_filename)
articles = load_articles(articles_dirname, gloves)

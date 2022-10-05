import mysql.connector
from mysql.connector import Error
import tweepy
import json
from dateutil import parser

def connect(username, created_at, tweet):
    try:
        con = mysql.connector.connect(host='localhost', database='playdata', user='bigdata', password='bigdata', charset='utf8')
        if con.is_connected():
            cursor = con.cursor()
            query = "INSERT INTO twitter (username, created_at, tweet) VALUES (%s, %s, %s)"
            cursor.execute(query, (username, created_at, tweet))
            con.commit()
    except Error as e:
        print(e)
    
    
    cursor.close()
    con.close()
    return

class stream(tweepy.StreamingClient):

    def on_tweet(self, tweet):
        if 'text' in tweet:
            username = tweet.id
            created_at = tweet.created_at
            tweet = tweet.text
     
            connect(username, created_at, tweet)
            print("Success Streaming to SQL")
        
    
    
printer = stream('AAAAAAAAAAAAAAAAAAAAADOIgwEAAAAA%2BoqRHwnfn5fdvhKZ8ZFq3VYzlnI%3Df1i1FMpdOKVwld0dEMKqWU0rs41k7VFvh78jUe8XLchLZLta02')
printer.add_rules(tweepy.StreamRule("coffee"))
printer.filter(tweet_fields=["created_at","lang"])
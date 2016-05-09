"""

This FILE CONTAINS LOGIC TO CRAWL TWEETS MADE BY HANDLES.

(AVAILABLE IN ADMIN SCREEN).
"""
import tweepy
import handles_conf
import handle_process
import pandas as pd


def connect_twitter(credentials):
    """Function to get Connected with Twitter API End Point."""
    validated = True
    try:
        auth = tweepy.OAuthHandler(credentials['CONSUMER_KEY'],
                                   credentials['CONSUMER_SECRET'])
        auth.set_access_token(credentials['ACCESS_TOKEN'],
                              credentials['ACCESS_TOKEN_SECRET'])
        api = tweepy.API(auth, parser=tweepy.parsers.JSONParser())
    except:
        api = False
        validated = False
    return validated, api


def get_timeline(handle, api, admin_hdl, handle_name):
    """Get User Time Line Data."""
    data = pd.DataFrame(columns=handles_conf.TWEETS_COLS)
    try:
        status_list = api.user_timeline(
            screen_name=handle,
            exclude_replies=True,
            count=200,
            result_type='recent')
    except:
        status_list = []

    if status_list:
        data_list = []
        data_list = handle_process.json_to_csv(
            status_list, data_list, handle_name)
        if len(data_list) > 0:
            data = pd.DataFrame(data_list, columns=handles_conf.TWEETS_COLS)
    return data

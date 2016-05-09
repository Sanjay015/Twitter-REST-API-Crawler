"""This File contains Functions to process tweets."""
import datetime
from dateutil.parser import parse
from bs4 import BeautifulSoup
import re
import sys
reload(sys)
sys.setdefaultencoding("utf-8")


def encoding_handle(message, regex, **kwargs):
    """Function to handle encoding errors."""
    replace = kwargs.get('replace', False)
    limit = kwargs.get('limit', False)
    if message:
        try:
            message = regex.sub(' ', message)
            if replace:
                message = message.replace("'", "''")
                message = message.replace("&", " ")
                message = message.replace("$", " ")
            if limit:
                message = message[:3997]
        except:
            try:
                message = message.encode('utf-8')
                message = regex.sub(' ', str(message))
                if replace:
                    message = message.replace("'", "''")
                    message = message.replace("&", " ")
                    message = message.replace("$", " ")
                if limit:
                    message = message[:3997]
            except:
                try:
                    message = message.decode('utf-8', "replace")
                    message = regex.sub(' ', str(message))
                    if replace:
                        message = message.replace("'", "''")
                        message = message.replace("&", " ")
                        message = message.replace("$", " ")
                    if limit:
                        message = message[:3997]
                except:
                    pass
    else:
        message = ''

    return message


def json_to_csv(data, data_list, handle_name):
    """Function to load raw json tweets to CSV and Bank Classification."""
    regex = re.compile('[\n\r\t]')
    for line in data:
        if len(line) > 0:
            if 'limit' not in line:
                try:
                    # ----------------------------------------- #
                    entities = line.get('entities')
                    # ----------------------------------------- #
                    status_id = line.get('in_reply_to_status_id_str')
                    status_id = str(long(status_id)) if status_id else 0
                    # print status_id
                    # ----------------------------------------- #
                    favorited = line.get('favorited')
                    # ----------------------------------------- #
                    filter_level = line.get('filter_level')
                    # ----------------------------------------- #
                    retweeted = line.get('retweeted')
                    # ----------------------------------------- #
                    timestamp_ms = line.get('timestamp_ms')
                    timestamp_ms = str(
                        long(timestamp_ms)) if timestamp_ms else timestamp_ms
                    # ----------------------------------------- #
                    coordinates = line.get('coordinates')
                    coordinates = ', '.join(
                        str(i) for i in coordinates.get('coordinates')
                    ) if coordinates else None
                    # ----------------------------------------- #
                    possibly_sensitive = line.get('possibly_sensitive')
                    # ----------------------------------------- #
                    tweet = encoding_handle(line.get('text'), regex)
                    # ----------------------------------------- #
                    tweet_id = str(long(line.get('id_str')))
                    favorite_count = line.get('favorite_count')
                    # ----------------------------------------- #
                    retweet_count = line.get('retweet_count')
                    # mentions = len(entities.get('user_mentions', []))
                    mentions = len(entities.get('user_mentions', []))
                    retweete_cum = 0
                    if 'retweeted_status' in line:
                        retweeted_status = line.get('retweeted_status')
                        # retweet_count = retweeted_status.get('retweet_count')
                        favorite_count = retweeted_status.get('favorite_count')
                    # ----------------------------------------- #
                    created_at = str(line.get('created_at'))
                    created_at = parse(created_at) + datetime.timedelta(
                        hours=5, minutes=30)

                    # ----------------------------------------- #
                    _handle = entities.get('user_mentions')
                    if _handle:
                        _handle = ', '.join(str(i.get('screen_name'))
                                            for i in _handle)
                    else:
                        _handle = ''

                    # ----------------------------------------- #
                    hashtags_list = entities.get('hashtags')
                    if len(hashtags_list):
                        hashtags = ', '.join(i.get('text')
                                             for i in hashtags_list)
                    else:
                        hashtags = ''
                    # ----------------------------------------- #
                    urls = entities.get('urls')
                    if urls:
                        urls = ', '.join(i.get('url') for i in urls)
                    else:
                        urls = ''
                    # ----------------------------------------- #
                    trends = entities.get('trends')
                    if trends:
                        trends = ', '.join(i.get('name') for i in trends)
                    else:
                        trends = ''
                    # ----------------------------------------- #
                    symbols = ''
                    if entities.get('symbols'):
                        symbols = ', '.join(i.get('text')
                                            for i in entities.get('symbols'))
                    # ----------------------------------------- #
                    try:
                        source = BeautifulSoup(str(line.get('source')))
                        source = source.a.string.encode(
                            'utf-8') if source else ''
                    except:
                        source = ''
                    # ----------------------------------------- #
                    users = line.get('user')
                    user_name = users.get('screen_name')
                    prof_image = users.get('profile_image_url')
                    profile_use_ = users.get('profile_use_background_image')
                    default_image = users.get('default_profile_image')
                    user_id = users.get('id')
                    user_ver = users.get('verified')
                    user_fc = users.get('followers_count')
                    user_lc = users.get('listed_count')
                    user_offset = users.get('utc_offset')
                    user_st_cou = users.get('statuses_count')
                    user_des = users.get('description')
                    user_des = encoding_handle(user_des, regex)
                    user_frnd = users.get('friends_count')
                    user_loc = users.get('location')
                    user_loc = encoding_handle(user_loc, regex)

                    user_following = users.get('following')
                    user_geo = users.get('geo_enabled')
                    user_lang = users.get('lang')
                    user_bg_ti = users.get('profile_background_tile')
                    user_fav_count = users.get('favourites_count')
                    user_cont = users.get('contributors_enabled')
                    user_tz = users.get('time_zone')
                    user_fol_req = users.get('follow_request_sent')
                    user_prof_create = str(users.get('created_at'))
                    user_prof_create = parse(
                        user_prof_create) + datetime.timedelta(
                        hours=5, minutes=30)
                    user_prof_create = user_prof_create.strftime(
                        '%d-%m-%Y %H:%M')
                    created_at = created_at.strftime('%d-%m-%Y %H:%M')
                    _row = ['Twitter', handle_name, tweet_id,
                            created_at,
                            favorite_count, retweet_count,
                            mentions,
                            tweet, symbols, status_id, retweeted,
                            filter_level, favorited, timestamp_ms,
                            coordinates, possibly_sensitive, hashtags,
                            _handle, urls, trends, user_name,
                            prof_image, profile_use_, default_image,
                            user_id, user_ver, user_fc, user_lc,
                            user_offset, user_st_cou, user_des,
                            user_frnd, user_loc, user_following,
                            user_geo, user_lang, user_bg_ti,
                            user_fav_count, user_prof_create,
                            user_cont, user_tz, user_fol_req,
                            retweete_cum, favorite_count, tweet_id]
                    data_list.append(_row)
                except:
                    pass
    return data_list

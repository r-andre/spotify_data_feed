#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Spotify data feed:
Exctract, transform, and load Spotify user data
'''

import pandas as pd
import sqlalchemy
#from sqlalchemy.orm import sessionmaker
import requests
import json
import sqlite3
# from datetime import datetime
import datetime

import user # module that stores Spotify username and access token

############
# EXCTRACT #
############

# Connecting to the database:
DATABASE_LOCATION = 'sqlite:///my_played_tracks.sqlite'
USERNAME = user.NAME
TOKEN = user.TOKEN

# Starting the extraction process:
if __name__ == '__main__':

    # Defining header:
    headers = {
        'Accept' : 'application/json',
        'Content-Type' : 'application/json',
        'Authorization' : 'Bearer {token}'.format(token=TOKEN)
    }

    # Setting todays and yesterdays dates and converting to unix timestamp:
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix = int(yesterday.timestamp()) * 1000
    # Setting the link to the Spotify API:
    link = 'https://api.spotify.com/v1/me/player/recently-played?after={time}'\
        .format(time=yesterday_unix)

    # Requesting the data from the API:
    r = requests.get(link, headers=headers)

    # Storing the data as a json:
    data = r.json()

    songs_list = [] # Stores song titles
    artists_list  = [] # Stores artists of songs
    played_at_list  = [] # Stores time and day of the song being played
    timestamps_list = [] # Stores day of the song being played

    # Extracting each entry in the list of last played songs:
    for song in data['items']:
        songs_list.append(song['track']['name'])
        artists_list .append(song['track']['album']['artists'][0]['name'])
        played_at_list.append(song['played_at'])
        timestamps_list.append(song['played_at'][0:10])

    # Creating a dictionary of last played songs:
    song_dict = {
        'Song' : songs_list,
        'Artist' : artists_list,
        'Played_at' : played_at_list, # Exact time the song was played
        'Timestamp' : timestamps_list # Day the song was played
    }

    # Creating a dataframe of last played songs:
    song_df = pd.DataFrame(
        song_dict, columns={'Song', 'Artist', 'Played_at', 'Timestamp'}
        ).sort_values('Played_at', ascending=True)

    print(song_df)

#############
# TRANSFORM #
#############

# WIP

########
# LOAD #
########

# WIP
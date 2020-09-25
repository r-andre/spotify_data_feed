#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Spotify data feed:
Exctract, transform, and load Spotify user data
'''

# Import libraries:
import pandas as pd
import sqlalchemy
#from sqlalchemy.orm import sessionmaker
import requests
import json
import sqlite3
import datetime

# Import modules:
import user # module that stores Spotify username and access token

USERNAME = user.NAME
TOKEN = user.TOKEN

# Function to validate the data:
def check_if_valid_data(df:pd.DataFrame)->bool:
    # Give an error message when the dataframe is empty:
    if df.empty:
        print("Error 01: No songs downloaded...execution stopped!")
        return False

    # Primary key check:
    if pd.Series(df['Played_at']).is_unique:
        pass
    else:
        raise Exception("Error 02: Primary Key Check failed!")

    # Check for NaN values:
    if df.isnull().values.any():
        raise Exception("Error 03: NaN detected!")

    # Verify only songs from the past 24 hours are stored:
    yesterday = datetime.datetime.now() - datetime.datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    timestamps = df['Timestamp'].tolist()
    for t in timestamps:
        if datetime.datetime.strptime(t, "%Y-%m-%d") != yesterday:
            raise Exception(
        "Error 04: >= 1 listed song was not played in the last 24 hours!"
        )

# Starting the ETL procedure:
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

    # Validating the data:
    if check_if_valid_data:
        print("Data valid...proceeding to Load stage.")

    # Establishing database connection:
    DATABASE = 'my_played_tracks.sqlite' # database name
    engine = sqlalchemy.create_engine('sqlite:///' + DATABASE)
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()

    # Defining the SQL query:
    sql_query = '''
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        Song VARCHAR(200),
        Artist VARCHAR(200),
        Played_at VARCHAR(200),
        Timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY_KEY (Played_at)
    )
    '''

    cursor.execute(sql_query)
    print("Successfully opened database")

    # Storing the data in the database:
    try:
        song_df.to_sql('my_played_tracks', engine, index=False, if_exists='append')
    except:
        print("Warning! Data already exists in the database!")

    print("Closing the database...")
    conn.close()

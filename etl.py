import os
import glob
import psycopg2
from psycopg2 import errors, DatabaseError 
import pandas as pd
from sql_queries import *


'''
Purpose: loading songs data from JSON file, extract information and pass them into INSERT sql command to add into songs & artists tables.

Input:
- connection and cursor to work with postgres database.
- filepath: path of folder where songs data JSON files located.


How does it work:

This function will process a single JSON file of songs data.
- It will read data from JSON file into a dataframe. Using astype('object') to convert dataframe columns to native python data type.
- Since each JSON file has only one record so need to select the first row of dataframe using `iloc[0]`
- For song data, select values from 'song_id', 'title', 'artist_id', 'year', 'duration' columns.
- For artist data, select values from 'artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude' columns.
- Keep those values into a list format before passing into INSERT sql command to add new record.

'''
def process_song_file(conn, cur, filepath):
    # read data of JSON file
    df = pd.read_json(filepath, lines=True).astype('object')
    
    # select values of song_data
    song_data = list(df[['song_id', 'title', 'artist_id', 'year', 'duration']].iloc[0])
    
    # select values of artist data
    artist_data = list(df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].iloc[0])
    
    cur.execute(song_table_insert, song_data)
    cur.execute(artist_table_insert, artist_data)
    conn.commit()

    

        
'''
Input:
- connection and cursor to work with postgres database.
- fileapth: path of folder where logs data JSON files located.


How does it work:
This function will process a single JSON file of logs data.
It will read data from JSON file into a dataframe. Using astype('object') to convert dataframe columns to native python data type.
Just select records with page 'NextSong' into another dataframe.
Then pass that dataframe into functions to add records into: time, users and songplays tables.
'''

def process_log_file(conn, cur, filepath):
    # read logs data from JSON file
    df = pd.read_json(filepath, lines=True).astype('object')
    
    # select records with page 'NextSong'
    df_nextsong = df.loc[df['page'] == 'NextSong']
    
    # call function to adding records into time table
    adding_time_records(conn, cur, df_nextsong);

    # call function to adding records into users table
    adding_users_records(conn, cur, df_nextsong);

    # call function to adding records into users table
    adding_songplays_records(conn, cur, df_nextsong);
    

'''
Input: logs dataframe with page 'NextSong'.

How does it work:
- Select values from only column 'ts'
- Then break a single individual value into serveral parts of datetime type, such as: timestamp, hour, day, week, month, year and weekday.
- Define a list of column labels for those datetime values.
- Merge datetime values with list column labels together and build another dataframe
- Finally, Loop through each row of dataframe and pass row data into INSERT sql to insert records into time table.
'''
def adding_time_records(conn, cur, df):
    
    # just select columns 'ts'
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # break timestamp data into different parts of datetime value
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday) 
    
    # define list column labels for each datetime value above
    column_labels = ('time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    
    # join the column labels with datetime values, then build a dataframe
    time_df = pd.DataFrame.from_dict({ column_labels[i]:time_data[i] for i in range(len(column_labels)) }).astype('object')
    
    # looping through each row, pass values into INSERT sql command and execute
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))
        conn.commit()
        

'''
Input: logs dataframe with page 'NextSong'.

How does it work:
- Just select values from 'userId', 'firstName', 'lastName', 'gender', 'level' columns into another dataframe
- Then loop through each row and insert its data into users table.
'''

def adding_users_records(conn, cur, df):
    # select data of 'userId', 'firstName', 'lastName', 'gender', 'level' columns
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    
    # looping through each row, pass values into INSERT sql command and execute
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, list(row))
        conn.commit()


        
'''
Input: logs dataframe with page 'NextSong'.

How does it work:
- Loop through each row of original dataframe.
- looking for song id and artist id from songs and artists tables
- select values of 'userId', 'level', 'sessionId', 'location', 'userAgent' columns, plus song id & artist id above to build a list
- passing that list into INSERT sql command to insert new record into songplays table.
'''
def adding_songplays_records(conn, cur, df):
    # perform looping
    for index, row in df.iterrows():
        # looking for song id and artist id from songs and artists tables
        results = cur.execute(song_select, (row.song, row.artist, row.length))
        
        # retrieve songid and artistid values
        songid, artistid = results if results else None, None

        # select values from columns, plus songid & artistid to build a list
        songplay_data = (pd.to_datetime(row['ts'], unit='ms'), row['userId'], row['level'], songid, artistid, row['sessionId'], row['location'], row['userAgent'])
        
        # passing list values into songplay_table_insert query which is imported by sql_queries.py
        cur.execute(songplay_table_insert, songplay_data)
        conn.commit()



'''
Input:
    - cursor and connection to postgres database.
    - filepath: the folder where songs or logs data JSON file located.
    - func: the function will be perform for either logs or songs data.

What does it do:
    - This function will look for individual single JSON file inside subfolder, then add its path into a list.
    - After that, it will perform a loop through each file path of list and invoke a function either for logs or songs data.
    
    We have 2 functions here:
        * process_log_file(): which will handle logs data.
        * process_song_file(): which will handle songs data.
    
    Purpose of those 2 functions are quite similar. 
        * They will loading data from JSON file into pandas dataframe.
        * Select values from specific columns and transform them if required.
        * Then pass those values into INSERT sql command to add new records into tables.

'''
def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(conn, cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))



def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    """call process_data function for songs data"""
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    
    """call process_data function for logs data"""
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
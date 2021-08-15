import os
import glob
import psycopg2
import psycopg2.extras as extras
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath,lines=True)

    # get next file Id
    FileName = os.path.basename(filepath)
    cur.execute(file_next_Id_select, [FileName])
    results = cur.fetchone()
    Next_File_Id = results[0]

    # insert song record
    song_data = df.loc[:, ['song_id', 'title', 'artist_id', 'year', 'duration']]
    song_data = song_data.drop_duplicates()
    song_data['FILE_ID'] = Next_File_Id
    song_nn = [tuple(i) for i in song_data.to_numpy()]
    extras.execute_values(cur, song_table_insert, song_nn)

    # insert artist record
    artist_data = df.loc[:, ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    artist_data = artist_data.drop_duplicates()
    artist_data['FILE_ID'] = Next_File_Id
    art = [tuple(i) for i in artist_data.to_numpy()]
    extras.execute_values(cur, artist_table_insert, art)

    #insert file information into files
    cur.execute(file_table_insert, (Next_File_Id, FileName, 'SNG'))


def process_log_file(cur, filepath):
    # open log file
    df = 

    # filter by NextSong action
    df = 

    # convert timestamp column to datetime
    t = 
    
    # insert time data records
    time_data = 
    column_labels = 
    time_df = 

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = 

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = 
        cur.execute(songplay_table_insert, songplay_data)


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
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
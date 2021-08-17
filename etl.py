import os
import glob
import psycopg2
import psycopg2.extras as extras
import pandas as pd
from sql_queries import *

def insert_file_metadata(cur,fileId,fileName,type):
    """
        Description: This function save the file metadata which is being processed into FILES table

        Arguments:
            cur: the cursor object.
            fileId: A unique identifier for the file to be saved.
            fileName: The file name.
            type: The type of file which is being processed ('SNG','LOG')

        Returns:
            None
    """
    # insert file information into files
    cur.execute(file_table_insert, (fileId, fileName, type))


def process_song_file(cur, filepath):
    """
        Description: This function is responsible for processing the song files and populate
        SONGS and ARTISTS tables

        Arguments:
            cur: the cursor object.
            filepath: The full path for the song file to be processed

        Returns:
            None
    """

    # get next file Id
    FileName = os.path.basename(filepath)
    cur.execute(file_next_Id_select, [FileName])
    results = cur.fetchone()
    Next_File_Id = results[0]

    # open song file
    df = pd.read_json(filepath,lines=True)

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
    insert_file_metadata(cur,Next_File_Id,FileName,"SNG")


def process_log_file(cur, filepath):
    """
        Description: This function is responsible for processing the log files and populate
        USERS,TIME and SONGPLAYS tables

        Arguments:
            cur: the cursor object.
            filepath: The full path for the log file to be processed

        Returns:
            None
    """

    #get next File Id
    FileName = os.path.basename(filepath)
    #print(FileName)
    cur.execute(file_next_Id_select, [FileName])
    results = cur.fetchone()
    Next_File_Id = results[0]

    # open log file
    df = pd.read_json(filepath,lines=True)

    # filter by NextSong action
    df = df[df.page=='NextSong']

    # convert timestamp column to datetime
    t=pd.DataFrame({"tf":pd.to_datetime(df['ts'],unit='ms'),"ts":df['ts']})
    t = t.drop_duplicates()
    
    # insert time data records
    time_data = (t['ts'], t['tf'].dt.hour, t['tf'].dt.day,
                 t['tf'].dt.week, t['tf'].dt.month, t['tf'].dt.year, t['tf'].dt.weekday)
    column_labels = ('timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday')

    time_df = pd.DataFrame(dict(zip(column_labels,time_data)))

    tim = time_df.values.tolist()
    extras.execute_values(cur, time_table_insert, tim)

    # load user table
    user_df = df.loc[:, ["userId", "firstName", "lastName", "gender", "level","ts"]]
    user_df = user_df.drop_duplicates()
    user_df['FILE_ID'] = Next_File_Id
    user_df["rank"] = user_df.groupby("userId")['ts'].rank(ascending=False)
    user_df = user_df.loc[user_df["rank"] == 1, ["userId", "firstName", "lastName", "gender", "level","FILE_ID"]]

    # insert user records
    users = [tuple(i) for i in user_df.to_numpy()]
    extras.execute_values(cur, user_table_insert, users)

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
        songplay_data = (row.ts,row.userId,row.level,songid,artistid,row.sessionId,row.location,
                         row.userAgent,Next_File_Id)
        cur.execute(songplay_table_insert, songplay_data)
    insert_file_metadata(cur, Next_File_Id, FileName, "LOG")


def process_data(cur, conn, filepath, func):
    """
        Description: This function is a generic one which gets all the files with extintion "json"
        in specific path specified in 'filepath' argument. It loops on this list and call
        function specified in 'func' argument to process this specific file.

        Arguments:
            cur: the cursor object.
            conn: connection to the database.
            filepath: The path for for the json files which will be processed by 'func'
            func: The function which will process each file in the list of the files in 'filepath' argument

        Returns:
            None
    """
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
    #process the song files
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)

    # process the log files
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
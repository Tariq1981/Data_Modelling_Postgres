# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS SONGPLAYS;"
user_table_drop = "DROP TABLE IF EXISTS USERS;"
song_table_drop = "DROP TABLE IF EXISTS SONGS;"
artist_table_drop = "DROP TABLE IF EXISTS ARTISTS;"
time_table_drop = "DROP TABLE IF EXISTS TIME;"
file_table_drop = "DROP TABLE IF EXISTS FILES;"
# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS SONGPLAYS
(
   SONGPLAY_ID SERIAL, 
   START_TIME BIGINT NOT NULL,
   USER_ID INT NOT NULL,
   LEVEL text,
   SONG_ID text,
   ARTIST_ID text,
   SESSION_ID text,
   LOCATION text,
   USER_AGENT text,
   FILE_ID INT,
   PRIMARY KEY(songplay_id)
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS USERS
(
   USER_ID INT NOT NULL,
   FIRST_NAME text,
   LAST_NAME text,
   GENDER CHAR(1),
   LEVEL text,
   FILE_ID INT,
   PRIMARY KEY(USER_ID)
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS SONGS
(
   SONG_ID text NOT NULL,
   TITLE text,
   ARTIST_ID text,
   YEAR INT,
   Duration numeric,
   FILE_ID iNT,
   PRIMARY KEY(SONG_ID)
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS ARTISTS
(
   ARTIST_ID text NOT NULL,
   NAME text,
   LOCATION text,
   LATITUDE numeric,
   LONGITUDE numeric,
   FILE_ID INT,
   PRIMARY KEY(ARTIST_ID)
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS TiME
(
   START_TIME BIGINT NOT NULL,
   HOUR INT NOT NULL,
   DAY INT NOT NULL,
   WEEK INT NOT NULL,
   MONTH INT NOT NULL,
   YEAR INT NOT NULL,
   WEEKDAY INT NOT NULL,
   PRIMARY KEY(START_TIME)
);
""")

file_table_create = ("""
CREATE TABLE IF NOT EXISTS FILES
(
   FILE_ID INT,
   FILE_NAME text,
   FILE_TYPE text,
   INSERT_UPDATE_DTTM timestamp,
   PRIMARY KEY(FILE_ID)
);
""")
# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO SONGPLAYS(START_TIME,USER_ID,LEVEL,SONG_ID,ARTIST_ID,SESSION_ID,LOCATION,USER_AGENT,FILE_ID) 
       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT(SONGPLAY_ID) 
DO NOTHING
""")

user_table_insert = ("""
INSERT INTO USERS(USER_ID,FIRST_NAME,LAST_NAME,GENDER,LEVEL,FILE_ID) VALUES %s
ON CONFLICT(USER_ID) 
DO UPDATE SET
    (LEVEL,FILE_ID) = 
    (EXCLUDED.LEVEL,EXCLUDED.FILE_ID);
""")

song_table_insert = ("""
INSERT INTO SONGS(SONG_ID,TITLE,ARTIST_ID,YEAR,Duration,FILE_ID) VALUES %s
ON CONFLICT(SONG_ID) 
DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO ARTISTS(ARTIST_ID,NAME,LOCATION,LATITUDE,LONGITUDE,FILE_ID) VALUES %s
ON CONFLICT(ARTIST_ID) 
DO UPDATE SET
    (LOCATION,LATITUDE,LONGITUDE,FILE_ID) = 
    (EXCLUDED.LOCATION,EXCLUDED.LATITUDE,EXCLUDED.LONGITUDE,EXCLUDED.FILE_ID);
""")


time_table_insert = ("""
INSERT INTO TIME(START_TIME,HOUR,DAY,WEEK,MONTH,YEAR,WEEKDAY) VALUES %s
ON CONFLICT(START_TIME) 
DO NOTHING;
""")

file_table_insert = ("""
INSERT INTO FILES(FILE_ID,FILE_NAME,FILE_TYPE,INSERT_UPDATE_DTTM) 
VALUES(%s,%s,%s,CURRENT_TIMESTAMP)
ON CONFLICT(FILE_ID)
DO UPDATE SET INSERT_UPDATE_DTTM = CURRENT_TIMESTAMP;
""")

# FilE Loading management
file_loaded_select=("""
SELECT FILE_ID FROM FILES WHERE FILE_NAME = %s;
""")
file_next_Id_select=("""
SELECT  MIN(F_ID) Next_File_Id
FROM
(
	SELECT COALESCE(MAX(FILE_ID),0)+1 F_ID FROM FILES
	UNION ALL
	SELECT FILE_ID 
	FROM FILES
	WHERE FILE_NAME=%s 
)Y;
""")

# FIND SONGS

song_select = ("""
SELECT SONG_ID,A.ARTIST_ID
FROM SONGS S
INNER JOIN ARTISTS A
ON A.ARTIST_ID = S.ARTIST_ID 
WHERE TITLE = %s AND NAME = %s AND Duration = %s;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create,file_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop,file_table_drop]
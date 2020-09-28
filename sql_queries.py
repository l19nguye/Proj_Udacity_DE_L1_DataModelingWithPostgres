# DROP TABLES

songplay_table_drop = "DROP table IF EXISTS songplays"
user_table_drop = "DROP table IF EXISTS users"
song_table_drop = "DROP table IF EXISTS songs"
artist_table_drop = "DROP table IF EXISTS artists"
time_table_drop = "DROP table IF EXISTS time"


# CREATE TABLES

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id SERIAL PRIMARY KEY, 
        start_time timestamp NOT NULL, 
        user_id varchar (18) NOT NULL, 
        level varchar (4), 
        song_id varchar (18), 
        artist_id varchar (18),
        session_id varchar (18), 
        location text, 
        user_agent text,
        UNIQUE(start_time, user_id, song_id, artist_id),
        CONSTRAINT fk_time FOREIGN KEY(start_time) REFERENCES time(start_time) ON DELETE NO ACTION,
        CONSTRAINT fk_user FOREIGN KEY(user_id) REFERENCES users(user_id) ON DELETE CASCADE,
        CONSTRAINT fk_song FOREIGN KEY(song_id) REFERENCES songs(song_id) ON DELETE NO ACTION,
        CONSTRAINT fk_artist FOREIGN KEY(artist_id) REFERENCES artists(artist_id) ON DELETE NO ACTION
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id varchar (18) PRIMARY KEY, 
        first_name text, 
        last_name text, 
        gender varchar (1), 
        level varchar (4)
    ) ;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id varchar (18) PRIMARY KEY, 
        title text NOT NULL, 
        artist_id varchar (18) NOT NULL, 
        year int, 
        duration decimal NOT NULL,
        UNIQUE(song_id, title, artist_id)
    ) ;
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id varchar (18) PRIMARY KEY, 
        name text NOT NULL, 
        location text, 
        latitude decimal, 
        longitude decimal,
        UNIQUE(artist_id, name)
    ) ;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time timestamp PRIMARY KEY, 
        hour int, 
        day int, 
        week int, 
        month int, 
        year int, 
        weekday int
     ) ;
""")


# INSERT RECORDS

songplay_table_insert = ("""
    insert into songplays (start_time, user_id, 
                           level, song_id, artist_id,
                           session_id, location, user_agent)
    values(%s, %s, %s, %s, %s, %s, %s, %s)
    on conflict (start_time, user_id, song_id, artist_id) 
    do 
        update set user_agent = EXCLUDED.user_agent;
""")


user_table_insert = ("""
    insert into users (user_id, first_name, last_name, gender, level)
    values (%s, %s, %s, %s, %s)
    on conflict(user_id)  
    do
        update set (first_name, last_name, gender, level) = (EXCLUDED.first_name, EXCLUDED.last_name, EXCLUDED.gender, EXCLUDED.level);
""")


song_table_insert = ("""
    insert into songs (song_id, title, artist_id, year, duration)
    values (%s, %s, %s, %s, %s)
    on conflict (song_id, title, artist_id)
    do 
        update set (year, duration) = (EXCLUDED.year, EXCLUDED.duration);
""")


artist_table_insert = ("""
    insert into artists (artist_id, name, location, latitude, longitude)
    values (%s, %s, %s, %s, %s)
    on conflict (artist_id, name)
    do 
        update set (location, latitude, longitude) = (EXCLUDED.location, EXCLUDED.latitude, EXCLUDED.longitude);
""")


time_table_insert = ("""
    insert into time (start_time, hour, day, week, month, year, weekday)
    values (%s, %s, %s, %s, %s, %s, %s)
    on conflict (start_time)
    do nothing;
""")

# FIND SONGS
song_select = ("""
    select s.song_id,  a.artist_id
    from songs s join artists a on
    s.artist_id = a.artist_id
    where s.title = %s
    and a.name = %s
    and s.duration = %s
""")

# QUERY LISTS

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
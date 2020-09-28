# Data Modeling with Postgres Project

## 1. Purpose of database

Sparkify has a music streaming app and its data is stored in directories of JSON file. In order to help analytics team perform some analysis, we will build a relational database where all master data as well as transactional data will be stored.

When have a look on JSON files, we can find there are 2 kinds of data:

* Songs data: each JSON file does not only include information of a song but also artist. We can see the song name, title, lenth of its, beside of aritst infor like first name, last name,..

* Logs data: which contains historical data of users when they were using the app. When looking on the logs, we will find something interesting, since it does not only tell us which song an user listened, when this happened.. but also details information of users such as first name, last name, gender,.. included.

As we can see, information included in each data is for several objects/items. We can find both song and artist infor in songs data, the log data has user activies/transactions data and users details information mixed together. With this kind of design, it would cost time for anyone look into the data and want to perform any aggregation or analysis, and my task is to fix this problem in following.

The reason I choose relation database to fix this problem is it will provide analytic team ability to perform aggregations. Instead of data of serveral entities stored at one document, we will have different tables, each one represent for a certain entity, each table link to others by a key or conbination of keys, this will help people easily query data by using SQL commands. I am going to implement Star schema in order to design database where the Fact table (**songplays** table) will be in the centre, and it referencing other dimension tables like **users, songs, artists, time**. 


## 2. Database schema design

Let have a look on the data to see how it is structured, from there I could see which information included, meaning of each one, what data type of each one, each information describes to which entity.

### 2.1. Songs data

![Image](images/sample_song_artist_data.jpg)

With those information, we would split into 2 entities, song and artist, each entity will have some specific information like following.



**SONGS table**

* *song_id:* is the `PRIMARY KEY`, has `varchar` type with length of 18.
* *title:* has `text` type and `NOT NULL`.
* *artist_id:* has `varchar` type with length of 18 and `NOT NULL`.
* *year:* type of int.
* *duration:* type of `decimal` and `NOT NULL`.

Beside of that, i put combination of columns *song_id, title, artist_id* should be `UNIQUE`. 



**ARTIST table**

* *artist_id:* is `PRIMARY KEY`, has type of `varchar` with length of 18.
* *name:* type of `text` and `NOT NULL`.
* *location:* type of `text`. 
* *latitude:* type of `decimal`.
* *longitude:* type of `decimal`.

And combination of `NOT NULL` columns should be `UNIQUE`


### 2.2. Logs data

![Image](images/sample_log_data.jpg)

Log data contains information come from different entities, and I would design each one like following.


**USERS table**

* *user_id:* is PRIMARY KEY which has type of `varchar` with length of 18.
* *first_name:* type of `text`.
* *last_name:* type of `text`.
* *gender:* type of `varchar`, due to possible values would be either 'M' or 'F', so I put max length of this column is 1.
* *level:* has type of `varchar`, max length is 4.

**TIME table**

* *start_time:* is PRIMARY KEY which has type of `timestamp`.
* *hour:* type of `int`.
* *day:* type of `int`.
* *week:* type of `int`.
* *month:* type of `int`.
* *year:* type of `int`.
* *weekday:* type of `int`.


**SONGPLAYS table**

* *songplay_id:* is PRIMARY KEY with type of `SERIAL`. Its values will be generated automatically whenever a new record added.
* *start_time:* is FOREIGN KEY with type of `timestamp` and `NOT NULL`. It is referencing to table **TIME**.
* *user_id:* is FOREIGN KEY has type of `varchar` with length of 18, `NOT NULL` and referencing to table **USERS**.
* *level:* type of `varchar`, possible values would be either 'free' or 'paid', so max length of this column is 4.
* *song_id:* is FOREIGN KEY has type of `varchar` with length of 18, `NOT NULL` and referencing to table **SONGS**.
* *artist_id:* is FOREIGN KEY has type of `varchar` with length of 18, `NOT NULL` and referencing to table **ARTISTS**.
* *session_id:* type of `int`.
* *location:* type of `text`.
* *user_agent:* type of `text`.

The combination of columns *start_time, user_id, song_id, artist_id* would be `UNIQUE`.

Foreign key constraints `fk_time`, `fk_song`, `fk_artist` are referencing to tables with `ON DELETE NO ACTION`, means whenever a record of any referenced table deleted, the rows in table **SONGPLAYS** will not be impacted.

I put `ON DELETE CASCADE` on foreign key `fk_user`, means whenever an user deleted, the referencing row of **SONGPLAYS** will be deleted as well.

![Image](images/star_schema.jpg)
    


## 3. ETL pipeline

Following flow demonstrates how I will perform ETL pipeline.
![Image](images/flow.jpg)

### 3.1. Loading JSON files into list

Purpose of this step is to collect all JSON files from all subfolders, path of all files will be stored in a list as result of this step. We will use following python function to do that.

```
# code block
def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    
    return all_files
```

Since logs and songs data are separated so I will have 2 different list of files for each kind of data.

`song_files = get_files("data/song_data")`

`log_files = get_files("data/log_data")`

### 3.2. Read data of JSON file into dataframe

Purpose of this step is to loading data from JSON file into `pandas datafame` for later using. We will use `read_json()` function from `pandas` to do that.

`df = pd.read_json(filepath, lines=True).astype('object')`

As result of this step, we will have a dataframe with a lot of information, and in next step we will transform it into the form we need for each entity.

### 3.3. Extract only information required for each entity

Purpose of this step is to transform the data we got in previous step to the form which we will pass into `insert` SQL command to add records into relational database.

Since we will create different entities and each one has its own information, so depend on which entity we are working on, the information we need to extract would be varied.

Since **songs** data has only one record and has been imported into dataframe so we just select the first row of its with `.iloc[0]`, then convert into a list before passing into INSERT query in next step.

**3.3.1. SONG** entity will have *song_id, title, artist_id, year, duration* columns.

`song_data = list(df[['song_id', 'title', 'artist_id', 'year', 'duration']].iloc[0])`


**3.3.2. ARTIST** entity will need *artist_id, artist_name, artist_location, artist_latitude, artist_longitude* columns and convert it into list.

`artist_data = list(df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].iloc[0])`

Dataframe of **logs** data has serveral rows so we will need a loop to insert them into database. Before that, for each entity, we need to select some specific columns.


**3.3.3. USERS** entity will have *userId, firstName, lastName, gender, level* columns

`user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]`


**3.3.4. TIME** entity will have *time, hour, day, week, month, year, weekday* columns and those are actually break down timestamps in logs data.

First, we will only focus on datarows has page `NextSong`.

`df_nextsong = df.loc[df['page'] == 'NextSong']`

after that, just select data of column `ts` of dataframe, then convert it into `datetime` type by using function `to_datetime()` of pandas.

```
# code block
t = pd.to_datetime(df_nextsong['ts'], unit='ms')
```

now we start break down timestamp data into units using datetimelike properties

`time_data = (t.dt.time, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)`

we also define a list of column names for those datetime units

`column_labels = ('time', 'hour', 'day', 'week', 'month', 'year', 'weekday')`

then merge `time_data` together with `column_labels` to build a dictionary before parsing it to dataframe.

`time_df = pd.DataFrame.from_dict({ column_labels[i]:time_data[i] for i in range(len(column_labels)) }).astype('object')`


**3.3.5. SONGPLAYS** entity has data come from different sources:

* Data of *songplay_id, start_time, user_id, level, session_id, location, user_agent* columns will comes from **logs**data.
* Data of *song_id, artist_id* will be retrieved from 2 entities: 'songs' and 'artists'.

that means, in order to insert a record, beside of extracting values of columns *songplay_id, start_time, user_id, level, session_id, location, user_agent* from each datarow of dataframe, we also need to perform another `select` query to retrieve *song_id, artist_id* values from 2 entities `songs` and `artists`, then we will have a list of values before passing them into `insert` SQL command.

here is `select` query to get *song_id, artist_id*

```
# code block
song_select = ("""
    select s.song_id,  a.artist_id
    from songs s join artists a on
    s.artist_id = a.artist_id
    where s.title = %s
    and a.name = %s
    and s.duration = %s
""")
```

for each row in dataframe `df_nextsong`, we will perform that query with song name, artist name and length of song in order to retrieve *song_id* and *artist_id*.

```
# code block
for index, row in df.iterrows():        
        # get songid and artistid from song and artist tables
        results = cur.execute(song_select, (row.song, row.artist, row.length))
        songid, artistid = results if results else None, None
```

then we merge *song_id, artist_id* with other values of datarow to build data of a `songplays` data before passing it into `insert` SQL command.

`songplay_data = (index, pd.to_datetime(row['ts'], unit='ms'), row['userId'], row['level'], songid, artistid, row['sessionId'], row['location'], row['userAgent'])`

## 4. Project files

#### `sql_queries.py`

All related `SQL` commands will be defined here, such as: `create table, drop table, select, insert`

* `create table` queries: to create **songplays, songs, artist, users, time** tables.
* `drop table` queries: to drop **songplays, songs, artist, users, time** tables.
* `select` query: to retrieve *song_id, artist_id*
* `insert` queries: to add new records into all above tables. For each table, I also add option to handle CONFLICT if adding data already existing.

This file actually does not perform any task, it is just exports those `SQL` queries that will be imported and used by `etl.py` or `etl.ipynb, create_tables.py`

#### `create_tables.py`

When execute this file, it will establish the connection to `studentdb` database, then will try to create database `sparkifydb`. If that database is existing, it will be drop first.

After that, the current connection will be closd before another one will be established to database `sparkifydb`.

Then it will execute `drop table` and `create table` queries which imported from `sql_queries.py` to create **songplays, songs, artist, users, time** tables. (if any of them existing, it will be dropped first)

#### `etl.ipynb`

This is iPython Notebook file where we will just work with a single JSON file of **songs** as well as **logs** data to demonstrate how ETL process will run. The steps should be:

* collect JSON files from subfolders into lists.
* reading data from JSON to dataframe using pandas library.
* build another dataframe or list for each entity by selecting columns from original dataframe, data type conversion will be performed as well.
* pass values of list or dataframe in previous step into SQL queries to retrieve or insert data from/into entities. When execute a query, we put that code in `try...catch` to catch any exception might occurr. In that case, we will print the exception as well as rollback transaction, then program will continue with other queries.


In iPython Notebook file, we will be able to execute every single cell of command and observe the result. It is helpful since you will be able to test every single line of code before merge them together to build a complicated program.

#### `etl.py`

This is executable version of `.ipynb` file and it would be executed in `command prompt`. Whenever it is executed, the `main()` function will be invoked first.

The code in `.py` file is little bit different from `.ipynb` since it has been restructured, but the logic flow to perform ETL processess is same.

In `.py` file, we define separated functions to process all JSON files of **logs** and **songs** data.

* `process_song_file()` will process **songs** data, transform them before adding data into `songs` and `artists` entities.
* `process_log_file()` will process **logs** data, will invoke other functions to adding data into `users, time` and `songplays` entities.


#### `test.ipynb`

Another iPython Notebook file where we perform `select` SQL queries to verify the records in entities that we processed in previous steps. 

#### Images folder

This is where all image files using in `README.md` located. Without this, no image will be displayed.

## 5. How to run

* First, run file `create_tables.py` from `command prompt`. This will connect to default database, check whether database `sparkifydb` existing. If yes, drop it, then create it again. Then the code will establish the connection to database <code>sparkifydb</code> which just been created. After that, the code will try to drop tables (ig they are existing) first before create them again. We need to ensure no error occurred before going to the next step.


* Second, run `etl.py` from `command prompt`. This file will connect to database `sparkifydb` created in the first step, that's why we need to ensure the 1st step performed successfully. Then it will perform ETL processes as we already discussed above:
        - collecting JSON file paths into lists.
        - for each file from the list, rewe will read data into a dataframe.
        - select specific values for each entity.
        - passing selected values into `insert` SQL command to adding data into entities.
     
* Third, open `test.ipynb` file with iPython Notebook, execute every single cell then observe the result. If the second step is performed correctly, we will be able to see some records of each entities here.</li>

* If you want to recreate database or tables as well as inserting data into entities again, you need to disconnect from the database by `Restart kernel` in iPython Notebook
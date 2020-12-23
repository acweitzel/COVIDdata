#The purpose of this code is to collect covid data from the web and put it into an SQLite3 database.
#The database will then be used by an other program which will visualize the data
#The covid data being used is historical data on cases and deaths by county in the U.S. found from:
#https://github.com/nytimes/covid-19-data/blob/master/us-counties.csv
#The data is formated with the following columns of information: date, county, state, fips, cases, deaths

#import relavant packages
import csv
import urllib.request, urllib.parse, urllib.error
import sqlite3

#build database in SQLite3
conn = sqlite3.connect('coviddb.sqlite')
cur = conn.cursor()

cur.executescript('''
DROP TABLE IF EXISTS Date;
DROP TABLE IF EXISTS County;
DROP TABLE IF EXISTS State;
DROP TABLE IF EXISTS FIPS;
DROP TABLE IF EXISTS Main;
CREATE TABLE Date (
    id     INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    date   TEXT UNIQUE
);
CREATE TABLE County (
    id     INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    name   TEXT UNIQUE
);
CREATE TABLE State (
    id     INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    name   TEXT UNIQUE
);
CREATE TABLE FIPS (
    id     INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    fips   TEXT UNIQUE
);
CREATE TABLE Main (
    date_id     INTEGER,
    county_id   INTEGER,
    state_id    INTEGER,
    fips_id     INTEGER,
    cases       INTEGER,
    deaths      INTEGER,
    PRIMARY KEY (date_id, county_id, state_id, fips_id)
)
''')

#Pull data from the web
url = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv'
data = urllib.request.urlopen(url)
lines = [l.decode('utf-8') for l in data.readlines()]
covid = csv.reader(lines)

#insert data into tables in database
for entry in covid:

    date = entry[0];
    county = entry[1];
    state = entry[2];
    fips = entry[3];
    cases = entry[4];
    deaths = entry[5];

    cur.execute('''INSERT OR IGNORE INTO Date (date)
        VALUES ( ? )''', ( date, ) )
    cur.execute('SELECT id FROM Date WHERE date = ? ', (date, ))
    date_id = cur.fetchone()[0]

    cur.execute('''INSERT OR IGNORE INTO County (name)
        VALUES ( ? )''', ( county, ) )
    cur.execute('SELECT id FROM County WHERE name = ? ', (county, ))
    county_id = cur.fetchone()[0]

    cur.execute('''INSERT OR IGNORE INTO State (name)
        VALUES ( ? )''', ( state, ) )
    cur.execute('SELECT id FROM State WHERE name = ? ', (state, ))
    state_id = cur.fetchone()[0]

    cur.execute('''INSERT OR IGNORE INTO FIPS (fips)
        VALUES ( ? )''', ( fips, ) )
    cur.execute('SELECT id FROM FIPS WHERE fips = ? ', (fips, ))
    fips_id = cur.fetchone()[0]

    cur.execute('''INSERT OR REPLACE INTO Main
        (date_id, county_id, state_id, fips_id, cases, deaths) VALUES ( ?, ?, ?, ?, ?, ?)''',
        ( date_id, county_id, state_id, fips_id, cases, deaths) )

    conn.commit()

import requests, json
import sqlite3
from sqlite3 import Error


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
        return conn
    except Error as e:
        print(e)
 
    return None



def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)



def apicall(pagenum):
    r = requests.get('http://api.themoviedb.org/3/discover/movie?page=%s'%(pagenum)+'&with_original_language=en&vote_count.gte=5&primary_release_date.lte=2015-12-31&primary_release_date.gte=2005-01-01&certification_country=US&language=en-US&api_key=82e6187aeb88de69d9c21134f71a9fdb')
    datajson = r.json()

    itemlst = []

    for index, val in enumerate(datajson):
        itemlst.append((datajson['results'][index]['title'], datajson['results'][index]['release_date'], datajson['results'][index]['popularity']))

    return itemlst


def main():
    database = "tmdb.db"
 
    sql_create_movie_table = """ CREATE TABLE IF NOT EXISTS movie (
                                        id INTEGER PRIMARY KEY   AUTOINCREMENT,
                                        title text NOT NULL,
                                        release_date text,
                                        rating text
                                    ); """
 
 
    # create a database connection
    conn = create_connection(database)
    if conn is not None:
        # create projects table
        create_table(conn, sql_create_movie_table)

    else:
        print("Error! cannot create the database connection.")


    # Get page range not to overload database
    # 50 at a time is a good time
    # Let's say you can to dump 50 pages record than simply do 1: 50 
    # In next itereation you can to start 51: 100

    startpage = input("Enter startpage: ")
    endpage = input("Enter startpage: ")

    for i in range(startpage, endpage+1):
        print "starting to dump data from page %s" %(i)

        rows = apicall(i)
        print "Data from page %s" %(rows)

        conn.executemany("insert into movie(title, release_date, rating) values (?,?,?)", rows)
        conn.commit()
    
        

if __name__ == '__main__':
    main()

#! /usr/bin/python2.7
import psycopg2
import sys


class NewsDB():
    # This class creates a connection to the database and methods for querying
    # the database and closing the connection

    def __init__(self, db="news"):
        try:
            self.conn = psycopg2.connect(dbname=db)
            self.cur = self.conn.cursor()
        except:
            print "Unable to connect to database"
            sys.exit()

    def query(self, query):
        try:
            self.cur.execute(query)
            return self.cur.fetchall()
        except:
            print "Unable to execute query"

    def close(self):
        self.cur.close()
        self.conn.close()


def top_errors():
    """this function queries the database to find the days on which
        more than 1% of requests resulted in errors and outputs a
        table showing the date, number of page requests, number of
        errors and the error percentage"""
    # connect to the database
    db = NewsDB()
    # build title and header row output
    print "\n"
    print "HIGH ERROR DAYS"
    print "*" * 100
    print "%-25s | %-20s | %-20s | %20s" % ("Date", "Errors", "Views",
                                            "Error Percent")
    print "_" * 100
    # build query string
    query = """
        WITH Errcnt AS (
            SELECT COUNT(id)::float AS errors, DATE(time) AS day
            FROM log
            WHERE status <> '200 OK'
            GROUP BY day),
            Totcnt AS (
            SELECT COUNT(id)::float AS hits, DATE(time) AS day
            FROM log
            GROUP BY day)
        SELECT e.day, e.errors, t.hits, (e.errors/t.hits) AS errpct
        FROM Errcnt e
        JOIN Totcnt t ON e.day = t.day
        WHERE (e.errors/t.hits)*100 > 1
        ORDER BY errpct DESC;"""

    # execute the query
    result = db.query(query)
    # if results are returned, loop through to print each row
    if result:
        for r in result:
            print " %-25s | %-20s | %-20s | %20s%%" % (r[0], int(r[1]),
                                                       int(r[2]),
                                                       round(r[3] * 100, 2))
        print "\n"
    else:
        print "no data to report\n"
    db.close()


def top_articles():
    """this function queries the database to find how many views each
        article has, and return the three with the most views"""
    # connect to the database
    db = NewsDB()
    # build the title and header row output
    print "\n"
    print "THREE TOP ARTICLES (ranked by views)"
    print "*" * 100
    print "%-50s | %40s" % ("Title", "Views")
    print "_" * 100
    # build the query string
    query = """
        SELECT a.title, count(l.path) AS views
        FROM articles AS a
        JOIN log as l ON a.slug LIKE (replace(l.path, '/article/','')) || '%'
        GROUP BY a.title
        ORDER BY views DESC LIMIT 3;"""
    # execute the query
    result = db.query(query)
    # if results are returned, loop through to print each row
    if result:
        for r in result:
            print "%-50s | %40s" % (r[0], r[1])
        print "\n"
    else:
        print "no data to report\n"
    db.close()


def top_authors():
    """this function queries the database to find how many views each author
        has for all the articles he or she has written and presents the list
        of authors and views in descending order"""
    # connect to the database
    db = NewsDB()
    # build the title and header row
    print "\n"
    print "POPULAR AUTHORS (ranked by views)"
    print "*" * 100
    print "%-50s | %40s" % ("Author Name", "Views")
    print "_" * 100
    # build the query string
    query = """
        SELECT au.name, COUNT(l.path) AS views
        FROM articles AS a
        JOIN log AS l ON a.slug LIKE (replace(l.path, '/article/','')) || '%'
        JOIN authors AS au ON a.author = au.id
        GROUP BY au.name
        ORDER BY views DESC;"""
    # execute the query
    result = db.query(query)
    # if results are returned, loop through and print each row
    if result:
        for r in result:
            print "%-50s | %40s" % (r[0], r[1])
        print "\n"
    else:
        print "no data to report\n"
    db.close()


top_errors()
top_articles()
top_authors()

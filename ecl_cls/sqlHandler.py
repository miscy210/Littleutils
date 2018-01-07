# -*- coding: utf8 -*-
'''
Created on 2017-04-14

@author: miscy210 <miscy210@163.com>
'''
try:
  import pymysql as sqlhandle
except:
  import MySQLdb as sqlhandle

class SQLClient():
    def __init__(self, sqlserver, charset='utf8'):
        self.con = sqlhandle.connect(host=sqlserver['host'],
                                   port=sqlserver['port'],
                                   user=sqlserver['user'],
                                   passwd=sqlserver['passwd'],
                                   db=sqlserver['db'],
                                   charset=charset)
        self.cur = self.con.cursor()

    def _exec(self, execsql, result=None):
        if result:
            self.cur.execute(execsql, result)
        else:
            self.cur.execute(execsql)
        results = self.cur.fetchall()
        return results

    def _execmany(self, execsql, results):
        self.cur.executemany(execsql, results)

    def _commit(self):
        self.con.commit()

    def close(self):
        self.con.commit()
        self.con.close()
        self.cur.close()
#!/usr/local/python3/bin/python3
# coding: utf-8
"""
    Author: Danilo F. Chilene
    Modified: Bao Yang 
"""

import argparse
import cx_Oracle
import inspect
import json

class Checks(object):

    # check instance status

    def check_active(self):
        """Check Intance is active and open"""
        sql = "select count(*) retvalue from v$instance where status = 'OPEN' and logins = 'ALLOWED' \
              and database_status = 'ACTIVE'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print (i[0])   
            
    # check size & space
    
    def dbfilesize(self):
        """Total size of all datafiles (without temp)"""
        sql = "select to_char(sum(bytes), 'FM9999999999999990') retvalue from dba_data_files"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print (i[0])
            
    def dbsize(self):
        """Total size of all datafiles have been used (without temp)"""
        sql = "select to_char(sum(a.bytes - f.bytes), 'FM9999999999999990') retvalue from \
              (select tablespace_name, sum(bytes) bytes from dba_data_files group by tablespace_name) a, \
              (select tablespace_name, sum(bytes) bytes from dba_free_space group by tablespace_name) f \
              where a.tablespace_name = f.tablespace_name"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print (i[0])

    def show_tablespaces(self):
        """List tablespace names in json format for zabbix auto discover"""
        sql = "select tablespace_name from dba_tablespaces where contents <> 'TEMPORARY'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        key = ['{#TABLESPACE}']
        lst = []
        for i in res:
            d = dict(zip(key, i))
            lst.append(d)
        print (json.dumps({'data': lst}))            
            
    def tablespace_used(self, name):
        """Get tablespace size has been used"""
        sql = "select to_char(sum(a.bytes - f.bytes), 'FM9999999999999990') retvalue from \
              (select tablespace_name, sum(bytes) bytes from dba_data_files group by tablespace_name) a, \
              (select tablespace_name, sum(bytes) bytes from dba_free_space group by tablespace_name) f \
              where a.tablespace_name = f.tablespace_name and a.tablespace_name = '{0}'".format(name)
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print (i[0])            
            
    def tablespace(self, name):
        """Get tablespace usage"""
        sql = "select to_char((a.bytes_alloc - f.bytes_free)*100/a.bytes_total, 'FM9990') retvalue from \
              (select tablespace_name, sum(bytes) bytes_alloc, sum(greatest(bytes,maxbytes)) bytes_total \
              from dba_data_files group by tablespace_name) a, \
              (select tablespace_name, sum(bytes) bytes_free from dba_free_space group by tablespace_name) f \
              where a.tablespace_name = f.tablespace_name and a.tablespace_name = '{0}'".format(name)   
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print (i[0])            
            
    def show_tablespaces_temp(self):
        """List temporary tablespace names in json format for zabbix auto discover"""
        sql = "select tablespace_name from dba_tablespaces where contents = 'TEMPORARY'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        key = ['{#TABLESPACE_TEMP}']
        lst = []
        for i in res:
            d = dict(zip(key, i))
            lst.append(d)
        print (json.dumps({'data': lst}))
                
    def tablespace_temp(self, name):
        """Get temporary tablespace usage"""
        sql = "select nvl(to_char(used*100/total, 'FM9990'), '0') retvalue from \
              (select tablespace_name, sum(greatest(bytes, maxbytes)) total from dba_temp_files \
              group by tablespace_name) a, \
              (select tablespace, sum(blocks*8192) used from v$tempseg_usage group by tablespace) f \
              where a.tablespace_name = f.tablespace(+) and a.tablespace_name = '{0}'".format(name)
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print (i[0])        
     
    def show_asm_volumes(self):
        """List asm volumes in json format for zabbix auto discover"""
        sql = "select name from v$asm_diskgroup"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        key = ['{#ASMVOLUME}']
        lst = []
        for i in res:
            d = dict(zip(key, i))
            lst.append(d)
        print (json.dumps({'data': lst}))

    def asm_volume_use(self, name):
        """Get asm volume usage"""
        sql = "select to_char(100 * (1 - usable_file_mb*(decode(type,'EXTERN',1,'NORMAL',3,'HIGH',5))/total_mb), 'FM9990') \
              retvalue from v$asm_diskgroup where name = '{0}'".format(name)
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print (i[0])

    def fra_use(self):
        """Query the fast recovery area usage"""
        sql = "select nvl((select to_char(space_used*100/space_limit, 'FM9990') retvalue from v$recovery_file_dest), 0) from dual"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print (i[0])

    # check user    
                              
    def show_users(self):
        """List open user in json format for zabbix auto discover"""
        sql = "select username from dba_users where account_status = 'OPEN'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        key = ['{#DBUSER}']
        lst = []
        for i in res:
            d = dict(zip(key, i))
            lst.append(d)
        print (json.dumps({'data': lst}))

    def user_status(self, dbuser):
        """If user status not change, then 1, otherwise 0"""
        sql = "select decode(account_status, 'OPEN', '1', '0') retvalue from dba_users \
              where username = '{0}'".format(dbuser)
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print (i[0])            
                                           
    # session & lock
    
    def query_sessions(self):
        """Query active sessions"""
        sql = "select count(*) retvalue from v$session where status = 'ACTIVE' and type <> 'BACKGROUND'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print (i[0])     

    def query_processes(self):
        """Query max processes"""
        sql = "select to_char(max_utilization*100/limit_value, 'FM9990') retvalue \
               from v$resource_limit where resource_name = 'processes'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print (i[0])                
                        
    def query_lock(self):
        """Query max lock time"""
        sql = "select nvl(retvalue,0) retvalue from (select max(ctime) retvalue from v$lock vk, dba_objects ob where vk.type in('TM') and vk.id1=ob.object_id and ob.temporary<>'Y')"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print (i[0])
            
    # performance            
            
    def deadlocks(self):
        """Deadlocks"""
        sql = "select to_char(value, 'FM99999999999999990') retvalue from v$sysstat \
        where name = 'enqueue deadlocks'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print (i[0])     
                
    def redowrites(self):
        """Redo Writes"""
        sql = "select to_char(value, 'FM99999999999999990') retvalue from v$sysstat \
              where name = 'redo writes'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print (i[0])
                       
    def commits(self):
        """User Commits"""
        sql = "select to_char(value, 'FM99999999999999990') retvalue from v$sysstat \
              where name = 'user commits'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print (i[0])

    def rollbacks(self):
        """User Rollbacks"""
        sql = "select to_char(value, 'FM99999999999999990') retvalue from v$sysstat \
              where name = 'user rollbacks'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print (i[0])            

    def hparsratio(self):
        """Hard parse ratio"""
        sql = "select to_char(h.value/t.value*100,'FM9990') retvalue from v$sysstat h, v$sysstat t \
              where h.name = 'parse count (hard)' and t.name = 'parse count (total)'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print (i[0])
            
    def rcachehit(self):
        """Read Cache hit ratio"""
        sql = "select to_char((1 - (phy.value - lob.value - dir.value)/ses.value) * 100, 'FM9990') retvalue \
              from v$sysstat ses, v$sysstat dir, v$sysstat lob, v$sysstat phy \
              where ses.name = 'session logical reads' \
              and dir.name = 'physical reads direct' \
              and lob.name = 'physical reads direct (lob)' \
              and phy.name = 'physical reads'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print (i[0])

    def dsksortratio(self):
        """Disk sorts ratio"""
        sql = "select to_char(d.value/(d.value + m.value) * 100, 'FM9990.99') retvalue \
              from v$sysstat m, v$sysstat d \
              where m.name = 'sorts (memory)' and d.name = 'sorts (disk)'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print (i[0])

    def tblscans(self):
        """Table scans (long tables)"""
        sql = "select to_char(value, 'FM99999999999999990') retvalue from v$sysstat \
              where name = 'table scans (long tables)'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print (i[0])

    def indexffs(self):
        """Index fast full scans (full)"""
        sql = "select to_char(value, 'FM99999999999999990') retvalue from v$sysstat \
              where name = 'index fast full scans (full)'" 
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print (i[0])

    def netsent(self):
        """Bytes sent via SQL*Net to client"""
        sql = "select to_char(value, 'FM99999999999999999999990') retvalue from v$sysstat \
              where name = 'bytes sent via SQL*Net to client'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print (i[0])

    def netresv(self):
        """Bytes received via SQL*Net from client"""
        sql = "select to_char(value/1024/1024, 'FM99999999999999999999990') retvalue from v$sysstat \
              where name = 'bytes received via SQL*Net from client'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print (i[0])
    
    # query cpu time model
    
    def query_sysmetrics(self, name):
        """Query v$sysmetric parameters"""
        sql = "select value from v$sysmetric where metric_name = '{0}' and \
               intsize_csec > 3000".format(name.replace('_', ' '))
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print (i[0])    
      
    # event statistics  

    def maxwaitevents(self):
        """Get session event name which more than 20"""
        sql = "select event from (select wait_class, event, count(*) from v$session \
              where wait_class <> 'Idle' group by wait_class, event having count(*) > 20 \
              order by count(*) desc) where rownum <= 1"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print (i[0])
            
    def freebufwaits(self):
        """Free buffer waits"""
        sql = "select nvl(to_char(time_waited, 'FM99999999999999990'), '0') retvalue \
              from v$system_event se, v$event_name en \
              where se.event(+) = en.name and en.name = 'free buffer waits'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print (i[0])

    def bufbusywaits(self):
        """Buffer busy waits"""
        sql = "select nvl(to_char(time_waited, 'FM99999999999999990'), '0') retvalue \
              from v$system_event se, v$event_name en \
              where se.event(+) = en.name and en.name = 'buffer busy waits'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print (i[0])

    def logswcompletion(self):
        """Log file switch completion"""
        sql = "select nvl(to_char(time_waited, 'FM99999999999999990'), '0') retvalue \
              from v$system_event se, v$event_name en \
              where se.event(+) = en.name and en.name = 'log file switch completion'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print (i[0])

    def logfilesync(self):
        """Log file sync"""
        sql = "select nvl(to_char(time_waited, 'FM99999999999999990'), '0') retvalue \
              from v$system_event se, v$event_name en \
              where se.event(+) = en.name and en.name = 'log file sync'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print (i[0])

    def logprllwrite(self):
        """Log file parallel write"""
        sql = "select nvl(to_char(time_waited, 'FM99999999999999990'), '0') retvalue \
              from v$system_event se, v$event_name en \
              where se.event(+) = en.name and en.name = 'log file parallel write'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print (i[0])

    def dbseqread(self):
        """DB file sequential read"""
        sql = "select nvl(to_char(time_waited, 'FM99999999999999990'), '0') retvalue \
              from v$system_event se, v$event_name en \
              where se.event(+) = en.name and en.name = 'db file sequential read'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print (i[0])

    def dbscattread(self):
        """DB file scattered read"""
        sql = "select nvl(to_char(time_waited, 'FM99999999999999990'), '0') retvalue \
              from v$system_event se, v$event_name en \
              where se.event(+) = en.name and en.name = 'db file scattered read'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print (i[0])

    def dbsnglwrite(self):
        """DB file single write"""
        sql = "select nvl(to_char(time_waited, 'FM99999999999999990'), '0') retvalue \
              from v$system_event se, v$event_name en \
              where se.event(+) = en.name and en.name = 'db file single write'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print (i[0])

    def dbprllwrite(self):
        """DB file parallel write"""
        sql = "select nvl(to_char(time_waited, 'FM99999999999999990'), '0') retvalue \
              from v$system_event se, v$event_name en \
              where se.event(+) = en.name and en.name = 'db file parallel write'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print (i[0])

    def directread(self):
        """Direct path read"""
        sql = "select nvl(to_char(time_waited, 'FM99999999999999990'), '0') retvalue \
              from v$system_event se, v$event_name en \
              where se.event(+) = en.name and en.name = 'direct path read'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print (i[0])

    def directwrite(self):
        """Direct path write"""
        sql = "select nvl(to_char(time_waited, 'FM99999999999999990'), '0') retvalue \
              from v$system_event se, v$event_name en \
              where se.event(+) = en.name and en.name = 'direct path write'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print (i[0])

    def latchfree(self):
        """latch free"""
        sql = "select nvl(to_char(time_waited, 'FM99999999999999990'), '0') retvalue \
              from v$system_event se, v$event_name en \
              where se.event(+) = en.name and en.name = 'latch free'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print (i[0])

    # check dataguard status
    def check_dg_lag(self, name):
        """Check dataguard lag, if database is primary, then return 0, if database is dataguard, return lag time(s)"""
        sql = "select database_role from v$database"
        self.cur.execute(sql)
        res = self.cur.fetchone()[0]
        if res == 'PRIMARY':
            print (0)
        else:
            sql = """select decode(value, null, -1000, value) lag from(
                  select substr(value, 2, 2) * 86400 + substr(value, 5, 2) * 3600 
                  + substr(value, 8, 2) * 60 + substr(value, 11, 2) value
                  from v$dataguard_stats where name = '{0} lag')""".format(name)
            self.cur.execute(sql)
            res = self.cur.fetchall()
            for i in res:
                print (i[0]) 

            
class Main(Checks):
    def __init__(self):
        parser = argparse.ArgumentParser()            
        parser.add_argument('--username')             
        parser.add_argument('--password')
        parser.add_argument('--address')
        parser.add_argument('--database')
        parser.add_argument('--port')

        subparsers = parser.add_subparsers()          

        for name in dir(self):
            if not name.startswith("_"):
                p = subparsers.add_parser(name)        
                method = getattr(self, name)
                argnames = inspect.getargspec(method).args[1:]    
                for argname in argnames:
                    p.add_argument(argname)
                p.set_defaults(func=method, argnames=argnames)    
        self.args = parser.parse_args()               

    def db_connect(self):
        a = self.args
        username = a.username
        password = a.password
        address = a.address
        database = a.database
        port = a.port if a.port else 1521
        self.db = cx_Oracle.connect("{0}/{1}@{2}:{3}/{4}".format(
            username, password, address, port, database))
        self.cur = self.db.cursor()

    def db_close(self):
        self.cur.close()
        self.db.close()

    def __call__(self):
        try:
            a = self.args
            callargs = [getattr(a, name) for name in a.argnames]    
            self.db_connect()
            try:
                return self.args.func(*callargs)
            finally:
                self.db_close()
        except Exception as err:
            print (err)


if __name__ == "__main__":
    main = Main()
    main()


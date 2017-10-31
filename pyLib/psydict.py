# psydict

import psycopg2, psycopg2.extras
from psycopg2.extensions import connection
import datetime, re, copy, sys
from json import loads, dumps
from time import sleep, gmtime, time
from datetime import timedelta
from threading import Lock

PSYPOOL_LOCK = Lock()
PSYPOOL_MAXIMUM = 2

class Psyconnection(connection) :
    url = None
    pool = True
    def close(self, force=False) :
        if self.closed :
            return True
        if self.pool and not force :
            with PSYPOOL_LOCK :
                if len(PSYPOOL.get(self.url, [])) >= PSYPOOL_MAXIMUM :
                    return super(Psyconnection, self).close()
                else :
                    if not self.closed :
                        self.rollback()
                    if self.url not in PSYPOOL :
                        PSYPOOL[self.url] = []
                    PSYPOOL[self.url].append(self)
                    return True
        else :
            from psycopg2.extensions import connection
            return connection.close(self)

    def __init__(self, dsn, *args) :
        if dsn[:8] == 'psypool+' :
            self.pool = True
            self.url = dsn[8:]
            super(self.__class__, self).__init__(dsn[8:])
            return
        super(self.__class__, self).__init__(dsn)
    def __del__(self) :
        if not self.closed :
            self.close()

class Psypools(dict) :
    def connect(self, url, pool=True) :
        if pool :
            with PSYPOOL_LOCK :
                if len(self.get(url, [])) > 0 :
                    conn = self[url].pop(0)
                    try :
                        conn.cursor().execute('SELECT 1')
                        return conn
                    except psycopg2.OperationalError as e :
                        conn.close(force=True)
                        return psycopg2.connect(connection_factory=Psyconnection, dsn='psypool+' + url)
                    except psycopg2.InterfaceError as e :
                        conn.close(force=True)
                        return psycopg2.connect(connection_factory=Psyconnection, dsn='psypool+' + url)
                else :
                    return psycopg2.connect(connection_factory=Psyconnection, dsn='psypool+' + url)
        else :
            return psycopg2.connect(connection_factory=Psyconnection, dsn=url)
    def clean(self, url=None) :
        if url :
            for c in self.get(url, []) :
                c.close(force=True)
        else :
            for key, conn in self.iteritems() :
                for c in conn :
                    c.close(force=True)

    def __del__(self) :
            self.clean()
PSYPOOL = Psypools()

class Column(object) :
    metadata = ''
    def __init__ (self, metadata) :
        self.metadata = metadata

class BaseTable(object) :
    __tablename__ = None
    __table_procedure__ = {}
    @classmethod
    def columns(cls) :
        cols = {}
        for name in [key for key in dir(cls) if not callable(getattr(cls, key)) and key[:2] != '__'] :
            value = getattr(cls, name)
            if isinstance(value, Column) :
                cols[name] = value.metadata
        return cols
    @classmethod
    def tablename(cls) :
        return cls.__tablename__
    @classmethod
    def tableprocedures(cls) :
        return [x[1] for x in sorted(cls.__table_procedure__.items())]
    @classmethod
    def all_tables(cls) :
        return cls.__subclasses__() + [g for s in cls.__subclasses__() for g in s.all_tables()]

class Psydict :
    __conn__ = None
    __curr__ = None
    __database__ = None
    __table__ = None
    __metadata__ = {}
    __autofill__ = {}
    __primary__ = []
    __readonly__ = False

    def __init__(self, url=None, clone=None, fork=None):
        if fork != None :
            self.__dict__ = copy.deepcopy({x:y for x,y in fork.__dict__.iteritems() if x not in ('__conn__', '__curr__')})
            self.__conn__ = fork.__conn__
            self.dbconnect()
        elif clone != None :
            self.__dict__ = copy.deepcopy({x:y for x,y in clone.__dict__.iteritems() if x not in ('__conn__', '__curr__')})
            self.dbconnect()
        else :
            self.dbconnect(url=url)
            if self.__table__ != None :
                self.load_table(self.__table__)
    def __enter__ (self) :
        return self
    def __exit__(self, *argv) :
        self.close()
    def __del__(self) :
        self.close()
    def close(self, force=False) :
        if self.__conn__ != None:
            try:
                self.__conn__.close(force)
                self.__conn__ = None
            except:
                pass
    def dbconnect(self, url=None, reconnect=False, retry=True) :
        load_table = False
        if reconnect :
            url = None
            try:
                self.__conn__.close()
            except :
                pass
            self.__conn__ = None
        if url != None :
            if self.__table__ != None :
                self.clean_fieldname(detach=True)
            res = re.findall(r'(^.+://[^/]+/[^/]+)/(.+)$', url)
            if len(res) > 0 :
                self.__database__, self.__table__ = res[0]
                load_table = True
            else :
                self.__database__, self.__table__ = url, None
            if self.__conn__ != None :
                self.close()
        if self.__conn__ == None or self.__conn__.closed :
            if self.__database__ != None :
                for id in xrange(5) :
                    try:
                        self.__conn__ = PSYPOOL.connect(self.__database__)
                        cur = self.__conn__.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
                        cur.execute('SET SESSION transform_null_equals TO ON;')
                        self.__conn__.commit()
                        break
                    except psycopg2.Error as e :
                        if id >= 4 :
                            raise e
                        else :
                            print 'retry {0}'.format(id+1)
                            sleep(3)
                            pass
            else :
                return None
        try:
            self.__curr__ = self.__conn__.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
            if load_table :
                self.load_table()
            return self.__curr__
        except psycopg2.Error as e:
            raise e

    def list_databases(self, selfown_only = False) :
        if selfown_only :
            sql = "SELECT datname FROM pg_catalog.pg_database WHERE pg_catalog.pg_get_userbyid(datdba) = current_user AND datistemplate = false;"
        else :
            sql = "SELECT datname FROM pg_database WHERE datistemplate = false;"

        return [ res['datname'] for res in self.execute(sql, fetchdata=2) ]

    def list_tables(self, schema= 'public') :
        return [res['table_name'] for res in \
                self.execute("SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema = %s ORDER BY table_name;", param=['public'], fetchdata=2) ]

    def clean_fieldname(self, detach=False) :
        if detach :
            self.__metadata__ = ''
        for fieldname in self.__dict__.keys() :
            if fieldname[:2] != '__' and fieldname not in self.__metadata__ :
                self.__dict__.pop(fieldname, None)
            self.__dict__.update({fld:None for fld in self.__metadata__})

    def create_database(self, dbname, owner=None) :
        if owner == None :
            owner = self.execute("SELECT current_user", fetchdata=1)['current_user']
        return self.execute_autocommit("CREATE DATABASE \"{0}\" WITH OWNER={1} ENCODING = 'UTF8' TABLESPACE = pg_default CONNECTION LIMIT = -1;".format(dbname, owner))

    def drop_database(self, dbname) :
        self.execute("select pg_terminate_backend(pid) from pg_stat_activity where datname=%s;", (dbname,), commit=True)
        return self.execute_autocommit("DROP DATABASE \"{0}\";".format(dbname))

    def lock_table(self, mode = 'ACCESS EXCLUSIVE') :
        self.execute('LOCK TABLE {0} IN {1} MODE;'.format(self.__table__, mode))
    def lock_advisory(self, number, transaction_level=False) :
        if transaction_level :
            return self.execute('SELECT pg_advisory_xact_lock(%s)', [number], commit=False, fetchdata=1)
        else :
            return self.execute('SELECT pg_advisory_lock(%s)', [number], commit=False, fetchdata=1)
    def unlock_advisory(self, number, transaction_level=False) :
        if transaction_level :
            return self.commit()
        elif number is not None :
            return self.execute('SELECT pg_advisory_unlock(%s)', [number], commit=False)
    def load_view(self, viewname, join=[], filter=None, fields=None, **argv) :
        '''
        view will be readonly and can co-related multiple tables/views with joins.
        join can be :
        viewname = ['table1'], join = ['table2', {'col2 in table2', 'col1 in table1'}]
        or
        viewname = ['table1'], join = ['table2', 'table1.col1 = table2.col2']

        Alternatively, for multiple joins:
        viewname = ['table1'], join = [['table2', {'col2 in table2', 'col1 in table1'}], ['table3', {'col3 in table3', 'col1 in table1'}]]
        or
        viewname = ['table1'], join = [['table2', 'table1.col1 = table2.col2'], [table3, 'table1.col1 = table3.col3']]
        '''
        tablename = viewname
        if len(join) > 0 :
            if isinstance(join[0], list) or isinstance(join[0], tuple) :
                if isinstance(join[0][1], dict) :
                    tablename += ' ' + ' '.join(['JOIN {0} ON ({0}.{1} = {2})'.format(tbl, *rel.items()[0]) for tbl, rel in join])
                else :
                    tablename += ' ' + ' '.join(['JOIN {0} ON ({1})'.format(tbl, rel) for tbl, rel in join])
            else :
                if isinstance(join[1], dict) :
                    tablename += ' JOIN {0} ON ({0}.{1} = {2})'.format(join[0], *join[1].items()[0])
                else :
                    tablename += ' JOIN {0} ON ({1})'.format(*join)
        if filter is not None :
            tablename += ' WHERE {0}'.format(filter)
        if isinstance(fields, list) or isinstance(fields, tuple) :
            fields = ', '.join(fields)
        if len(argv) > 0 :
            self.__metadata__ = fields +', ' if fields != None else ''
            self.__metadata__ += ', '.join(['{1} AS {0}'.format(k,v) for k,v in argv.iteritems()])
        else :
            self.__metadata__ = fields if fields != None else '*'
        self.__table__ = tablename
        self.__readonly__ = True
        return True
    def load_table(self, tablename=None, report_error=True, manual_fields=[]) :
        self.__readonly__ = False
        if tablename != None :
            self.__table__ = tablename
        if self.__table__ == None or self.__table__ == '' :
            raise 'No tablename given'

        columns = self.execute("SELECT column_name, data_type, column_default FROM information_schema.columns WHERE table_name = %s", [self.__table__], fetchdata=2)
        self.__metadata__ = {col['column_name']:col['data_type'] for col in columns}
        self.__autofill__ = {col['column_name']:col['column_default'] for col in columns if col['column_default'] != None and col['column_name'] not in manual_fields}

        if len(columns) == 0 :
            if self.__table__ not in self.list_tables() :
                if report_error :
                    raise ValueError("Table '{0}' is unaccessible in database '{1}'".format(self.__table__, self.__database__))
                else :
                    return None
        else :
            self.__primary__ = self.execute("SELECT ARRAY(SELECT pg_get_indexdef(indexrelid, k + 1, TRUE) FROM generate_subscripts(indkey, 1) AS k ORDER BY k ) as keys FROM pg_index where indisprimary = 't' and indrelid = '{0}'::regclass;".format(self.__table__), fetchdata=1).get('keys', [])
            if len(self.__primary__) == 0 :
                raise 'Table has no primary key'
            self.clean_fieldname()
            for col in columns :
                if col in self.__primary__ :
                    col['primary_key'] = True
        return columns
    def format_to_psql(self, data, dump_json=True, retain_unknown=False) :
        if self.__readonly__ :
            return data
        if data == None :
            return None
        if isinstance(data, dict) :
            new_data = {}
            #ot = time()
            for fld in data :
            #    print fld, time() - ot
                if fld in self.__metadata__ :
                    if self.__metadata__[fld][:4].upper() == 'TIME' and data[fld] != None and not isinstance(data[fld], datetime.datetime) and data[fld].upper() != 'NOW' :
                        try :
                            new_data[fld] = datetime.datetime.strptime(data[fld], '%Y-%m-%d %H:%M:%S.%f')
                        except :
                            try:
                                new_data[fld] = datetime.datetime.strptime(data[fld], '%Y-%m-%d %H:%M:%S')
                            except :
                                try:
                                    new_data[fld] = datetime.datetime.strptime(data[fld], '%Y-%m-%dT%H:%M:%S.%f')
                                except :
                                    new_data[fld] = datetime.datetime.strptime(data[fld], '%Y-%m-%dT%H:%M:%S')
                    elif dump_json and data.get(fld, None) is not None and ( isinstance(data[fld], list) or isinstance(data[fld], dict) or self.__metadata__[fld][:4].upper() in ('DICT', 'JSON', 'LIST') ) :
                        try :
                            loads(data[fld])
                            new_data[fld] = data[fld]
                        except :
                            new_data[fld] = dumps(data[fld])
                    else :
                        new_data[fld] = data[fld]
                elif retain_unknown :
                    new_data[fld] = data[fld]
            return new_data
        else :
            new_data = [self.format_to_psql(d, dump_json) for d in data]
            return new_data
    def export_dict(self, data=None, detailed=False) :
        if data == None :
            data = self.__dict__
        if isinstance(data, dict) :
            new_data = {}
            for fld in data :
                if not detailed and fld[0] == '_' :
                    continue
                if isinstance(data[fld], datetime.datetime) :
                    new_data[fld] = str(data[fld])
                elif fld in self.__metadata__ and self.__readonly__ == False and self.__metadata__[fld][:4].upper() in ('JSON', 'LIST', 'DICT') :
                    try:
                        new_data[fld] = loads(data[fld])
                    except :
                        new_data[fld] = data[fld]
                else :
                    new_data[fld] = data[fld]
            return new_data
        else :
            new_data = [self.export_dict(d, detailed) for d in data]
            return new_data
    def add_field(self, fieldname, fieldtype, default='NULL', commit=True) :
        if self.__table__ == None :
            raise 'No table information'
        elif self.__readonly__ :
            raise ValueError('Current table is read only.')
        curr = self.dbconnect()
        if fieldname in self.__dict__ :
            sqlcmd = 'alter table {0} alter column {1} type {2} using {1}::{2}; ALTER TABLE {0} ALTER COLUMN {1} SET DEFAULT {3};'.format(self.__table__, fieldname, fieldtype, default)
        else :
            sqlcmd = 'ALTER TABLE {0} ADD COLUMN {1} {2} DEFAULT {3}'.format(self.__table__, fieldname, fieldtype, default)
        self.execute(sqlcmd, commit=commit)
        self.load_table()
        return True

    def delete_field(self, fieldname, cascade=False, commit=True) :
        if self.__table__ == None :
            raise ValueError('No table information')
        if cascade :
            self.execute('ALTER TABLE {0} DROP COLUMN {1} CASCADE'.format(self.__table__, fieldname), commit=commit)
        else :
            self.execute('ALTER TABLE {0} DROP COLUMN {1}'.format(self.__table__, fieldname), commit=commit)
        self.load_table()
        return True

    def add_table(self, tablename=None, columns={}, rebuild=False, modify=False) :
        if not isinstance(columns, dict) and BaseTable in columns.__bases__ :
            if tablename == None :
                tablename = columns.tablename()
            tableprocedures = columns.tableprocedures()
            columns = columns.columns()
        else :
            tableprocedures = []

        if tablename == None :
            raise ValueError('Tablename is not given')
        elif tablename.upper()[:2] == '__' :
            for procedure in tableprocedures :
                self.execute(procedure, commit=False)
            self.commit()
            return 'Done'

        if rebuild:
            self.delete_table(tablename)
        if tablename in self.list_tables() :
            self.load_table(tablename=tablename)
            co_group = {'BIGINT':1, 'BIGSERIAL':1, 'INTEGER':2, 'SERIAL':2, 'INT':2, 'FLOAT':3, 'DOUBLE':3}

            for col, dat in self.__metadata__.iteritems() :
                if col not in columns :
                    if not modify :
                        raise ValueError('Table exists and has an extra column "{0}" than given data'.format(col))
                    else :
                        self.delete_field(col)
                        
                elif columns[col].split(' ', 1)[0].upper() in ('LIST', 'DICT') :
                    columns[col] = ' '.join(['json', columns[col].split(' ', 1)[1] if len(columns[col].split(' ', 1)) > 1 else ''])

                elif dat.split(' ', 1)[0].upper() != columns[col].split(' ', 1)[0].upper() :
                    if co_group.get(dat.split(' ', 1)[0].upper(), -1) == co_group.get(columns[col].split(' ', 1)[0].upper(), -2) :
                        continue
                    if not modify :
                        raise ValueError('Table exists and column {0} has a different data type with given definition'.format(col))
                    else :
                        self.add_field(col, columns[col].split(' ', 1)[0])
            for col, dat in columns.iteritems():
                if columns[col].split(' ', 1)[0].upper() in ('LIST', 'DICT') :
                    columns[col] = ' '.join(['json', columns[col].split(' ', 1)[1] if len(columns[col].split(' ', 1)) > 1 else ''])
                if col not in self.__metadata__ and col[:2] != '__' :
                    if not modify :
                        raise ValueError('Table exists and has less column {0} than the given set'.format(col))
                    else :
                        self.add_field(col, columns[col].split(' ', 1)[0])
        else :
            if isinstance(columns, dict) :
                column_string = ', '.join(['{0} {1}'.format(*col) for col in sorted(columns.iteritems())])
            else :
                column_string = ', '.join(['{0} {1}'.format(*col) for col in columns])
            if len(column_string) > 0 and column_string.upper().find('PRIMARY') == -1:
                raise ValueError('A primary key is required.')
            self.execute( 'CREATE TABLE {0} ({1});'.format(tablename, column_string), commit=True )
        if len(tableprocedures) > 0 :
            for procedure in tableprocedures :
                self.execute(procedure, commit=False)
            self.commit()
        self.load_table(tablename)
        return 'Done'

    def delete_table(self, tablename=None, casade=False) :
        if self.__readonly__ :
            raise ValueError('Current psydict object is read only')

        try:
            if tablename != None :
                if casade :
                    self.execute('DROP TABLE {0} CASADE;'.format(tablename))
                else :
                    self.execute('DROP TABLE {0}'.format(tablename))
            else :
                if casade :
                    self.execute('DROP TABLE {0} CASADE;'.format(self.__table__))
                else :
                    self.execute('DROP TABLE {0}'.format(self.__table__))
                self.__table__ = None
                self.__metadata__ = {}
                self.clean_fieldname()
            return True
        except :
            return False
    def add_index(self, fieldname, indexname=None, method='btree', force=True) :
        if self.__table__ == None :
            raise 'No table information'
        if indexname != None:
            if force :
                self.execute( 'DROP INDEX IF EXISTS {0}'.format(indexname) )
            self.execute( 'CREATE INDEX {0} ON {1} using {3} ({2});'.format(indexname, self.__table__, fieldname, method) )
        else :
            self.execute( 'CREATE INDEX ON {0} using {2} (({1}));'.format(self.__table__, fieldname, method) )
        return True
    def delete_index(self, indexname) :
        self.execute( 'DROP INDEX IF EXISTS {0}'.format(indexname) )

    def execute(self, sqlcmd, param=None, fetchdata=0, commit=False) :
        for i in xrange(300):
            try:
                self.__curr__.execute(sqlcmd, param)
                if commit:
                    self.__conn__.commit()
                if fetchdata <= 0 :
                    return True
                elif fetchdata == 1 :
                    try :
                        return self.__curr__.fetchone()
                    except :
                        return None
                else :
                    try:
                        return self.__curr__.fetchall()
                    except :
                        return []
                break
            except psycopg2.OperationalError as e:
                if i < 300:
                    sys.stderr.write( str(e) + '\n' )
                    sys.stderr.write( 'Reconnecting. \n' )
                    sleep(3)
                    self.dbconnect(reconnect=True)
                else :
                    raise e
            except psycopg2.InterfaceError as e :
                if str(e) != 'cursor already closed' :
                    self.__conn__.rollback()
                    sys.stderr.write( 'Exception: ' + self.__curr__.mogrify(sqlcmd, param) +'\n')
                    raise e
                if i < 300:
                    sys.stderr.write( str(e) + '\n' )
                    sys.stderr.write( 'Reconnecting. \n' )
                    sleep(3)
                    self.dbconnect(reconnect=True)
                else :
                    raise e
            except psycopg2.DatabaseError as e:
                if str(type(e)) != "<class 'psycopg2.DatabaseError'>" :
                    self.__conn__.rollback()
                    sys.stderr.write( 'Exception: ' + self.__curr__.mogrify(sqlcmd, param) +'\n')
                    raise e
                if i < 300:
                    sys.stderr.write( str(e) + '\n' )
                    sys.stderr.write( 'Reconnecting. \n' )
                    sleep(3)
                    self.dbconnect(reconnect=True)
                else :
                    raise e
            except psycopg2.Error as e:
                try :
                    self.__conn__.rollback()
                except :
                    pass
                sys.stderr.write( 'Exception: ' + self.__curr__.mogrify(sqlcmd, param) +'\n')
                raise e
    def commit(self) :
        try:
            return self.__conn__.commit()
        except psycopg2.OperationalError as e:
            if i < 300:
                sys.stderr.write( str(e) + '\n' )
                sys.stderr.write( 'Reconnecting. \n' )
                sleep(3)
                self.dbconnect(dbname=self.__database__)
            else :
                raise e
    def rollback(self) :
        try:
            return self.__conn__.rollback()
        except psycopg2.OperationalError as e:
            if i < 300:
                sys.stderr.write( str(e) + '\n' )
                sys.stderr.write( 'Reconnecting. \n' )
                sleep(3)
                self.dbconnect(dbname=self.__database__)
            else :
                raise e
    def execute_autocommit(self, sqlcmd, param=None, fetchdata=0) :
        self.commit()
        self.__conn__.autocommit = True
        res = self.execute(sqlcmd, param, fetchdata, commit=False)
        self.__conn__.autocommit = False
        return res
    def get_tablename(self) :
        return self.__table__
    def get_dbname(self) :
        return self.__database__
    def set_default(self, fieldname, default) :
        if self.__table__ == None :
            raise 'No table information'
        if default == None :
            default = 'NULL'
        self.execute('ALTER TABLE {0} ALTER COLUMN "{1}" SET DEFAULT {2};'.format(self.__table__, fieldname, default))
        return self.load_table()
    def set_primarykey(self, fieldname) :
        constraint_name = self.execute('SELECT constraint_name from information_schema.table_constraints WHERE table_name = %s AND constraint_type = %s', [self.__table__, 'PRIMARY KEY'], fetchdata=2)
        for cn in constraint_name :
            self.execute('ALTER TABLE {0} DROP CONSTRAINT {1}'.format(self.__table__, cn['constraint_name']))
        self.execute('ALTER TABLE {0} ADD PRIMARY KEY ({1})'.format(self.__table__, fieldname), commit=True)
        return self.load_table()
    def set_foreignkey(self, fieldname, source_table, source_field) :
        self.drop_foreignkey(fieldname)
        self.execute('ALTER TABLE {0} ADD CONSTRAINT FOREIGN KEY ({1}) REFERENCES {2} ({3}) MATCH FULL'.format(self.__table__, fieldname, source_table, source_field), commit=True)
        return True
    def drop_foreignkey(self, fieldname) :
        fkeys = self.execute("SELECT tc.constraint_name FROM information_schema.table_constraints AS tc JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name WHERE constraint_type = 'FOREIGN KEY' AND column_name = %s AND table_name = %s", [fieldname, self.__table__], fetchdata=2);
        for fkey in fkeys :
            self.execute('ALTER TABLE {0} DROP CONSTRAINT {1}'.format(self.__table__, fkey['constraint_name']))
        self.commit()

    def upsert_record(self, data=None, insert_filter_field=None, insert_filter_clause=None, insert_filter_param=None, update_filter_field=None, update_filter_clause=None, update_filter_param=None, fields_to_upsert=None, autofill=True, return_fields=['*'], lock='EXCLUSIVE', rewrite=True, commit=True) :
        if self.__readonly__ :
            raise ValueError('Current table is read only')
        fetchdata = 1 if return_fields != None and len(return_fields) > 0 else 0

        if data == None or isinstance(data, dict) :
            if isinstance(lock, int) :
                self.lock_advisory(lock, transaction_level=True)
            elif lock is None :
                pass
            else :
                self.lock_table(mode='EXCLUSIVE')

            if data == None :
                new_data = self.format_to_psql(self.__dict__)
            else :
                new_data = self.format_to_psql(data)
            if insert_filter_clause == None :
                if insert_filter_field == None :
                    insert_filter_field = sorted(self.__primary__)
                elif isinstance(insert_filter_field, basestring) :
                    insert_filter_field = [insert_filter_field]
                else :
                    insert_filter_field = sorted(insert_filter_field)
                insert_filter_clause = ' AND '.join(['{0} = %s'.format(fld) for fld in insert_filter_field])
                insert_filter_param = [new_data[fld] for fld in insert_filter_field if new_data[fld] != None]
                if len(insert_filter_param) != len(insert_filter_field) :
                    self.rollback()
                    raise ValueError('Some key fields are NULL')

            keys = self.execute('SELECT * FROM {0} WHERE {1}'.format(self.__table__, insert_filter_clause), insert_filter_param, fetchdata=2)

            if len(keys) == 0 :
                res = self.insert_record(new_data, fields_to_upsert, autofill, commit, return_fields, rewrite)
                return res
            elif len(keys) == 1 :
                if update_filter_clause == None :
                    if update_filter_field == None :
                        update_filter_clause = 'ALWAYS'
                    else :
                        if isinstance(update_filter_field, basestring) :
                            update_filter_field = [update_filter_field]
                        else :
                            update_filter_field = sorted(update_filter_field)
                        update_filter_clause = ' AND '.join(['{0} = %s'.format(fld) for fld in update_filter_field])
                        update_filter_param = [new_data.get(fld, None) for fld in update_filter_field ] ## if new_data[fld] != None]
                        if len(update_filter_param) != len(update_filter_field) :
                            raise ValueError('Some key fields are NULL')
                if update_filter_param is None :
                    update_filter_param = insert_filter_param
                elif insert_filter_param is not None :
                    update_filter_param = insert_filter_param + update_filter_param

                if update_filter_clause.upper() != 'NEVER' and (update_filter_clause.upper() == 'ALWAYS' or len(self.execute('SELECT 1 FROM (SELECT * FROM {0} WHERE {1}) res WHERE {2}'.format(self.__table__, insert_filter_clause, update_filter_clause), update_filter_param, fetchdata=2)) == 0) :
                    keys[0].update({k:v for k, v in new_data.iteritems() if v is not None})
                    result = self.update_record(keys[0], fields_to_upsert, autofill, commit, return_fields, rewrite)
                    return result
                else :
                    if commit :
                        self.commit()
                    if rewrite :
                        self.import_dict(keys[0], self_clean=True)
                    if fetchdata :
                        return self.export_dict(keys[0], detailed=True)
                    else :
                        return False
            else :
                self.commit()
                raise ValueError('Record is not unique')
        else :
            res = []
            for id in xrange(0, len(data), 200) :
                if isinstance(lock, int) :
                    self.lock_advisory(lock, transaction_level=True)
                elif lock is None :
                    pass
                else :
                    self.lock_table(mode='EXCLUSIVE')
                
                res.extend([self.upsert_record(d, insert_filter_field, insert_filter_clause, insert_filter_param, update_filter_field, update_filter_clause, update_filter_param, fields_to_upsert, autofill, return_fields, None, rewrite, False) for d in data[id:(id+200)]])
                self.commit()
            return res if fetchdata else True

    def upsert_record_dictParam(self, data=None, insert_filter_field=None, insert_filter_clause=None, update_filter_field=None, update_filter_clause=None, fields_to_upsert=None, autofill=True, return_fields=['*'], lock='EXCLUSIVE', rewrite=True, commit=True) :
        if self.__readonly__ :
            raise ValueError('Current table is read only')
        fetchdata = 1 if return_fields != None and len(return_fields) > 0 else 0

        if insert_filter_clause == None :
            if insert_filter_field == None :
                insert_filter_field = sorted(self.__primary__)
            elif isinstance(insert_filter_field, basestring) :
                insert_filter_field = [insert_filter_field]
            else :
                insert_filter_field = sorted(insert_filter_field)
            insert_filter_clause = ' AND '.join(['{0} = %({0})s'.format(fld) for fld in insert_filter_field])

        if update_filter_clause == None :
            if update_filter_field == None :
                update_filter_clause = 'ALWAYS'
            else :
                if isinstance(update_filter_field, basestring) :
                    update_filter_field = [update_filter_field]
                else :
                    update_filter_field = sorted(update_filter_field)
                update_filter_clause = ' AND '.join(['{0} = %({0})s'.format(fld) for fld in update_filter_field])

        if data is None :
            data = self.__dict__
            
        if isinstance(data, dict) :
            if isinstance(lock, int) :
                self.lock_advisory(lock, transaction_level=True)
            elif lock is not None :
                self.lock_table(mode='EXCLUSIVE')
                
            new_data = self.format_to_psql(data)

            keys = self.execute('SELECT * FROM {0} WHERE {1}'.format(self.__table__, insert_filter_clause), new_data, fetchdata=2)

            if len(keys) == 0 :
                return self.insert_record(new_data, fields_to_upsert, autofill, commit, return_fields, rewrite)
            elif len(keys) == 1 :
                if update_filter_clause.upper() != 'NEVER' and (update_filter_clause.upper() == 'ALWAYS' or len(self.execute('SELECT 1 FROM (SELECT * FROM {0} WHERE {1}) res WHERE {2}'.format(self.__table__, insert_filter_clause, update_filter_clause), new_data, fetchdata=2)) == 0) :
                    keys[0].update({k:v for k, v in new_data.iteritems() if v is not None})
                    result = self.update_record(keys[0], fields_to_upsert, autofill, commit, return_fields, rewrite)
                    return result
                else :
                    if commit :
                        self.commit()
                    if rewrite :
                        self.import_dict(keys[0], self_clean=True)
                    return self.export_dict(keys[0], detailed=True) if fetchdata else False
            else :
                self.commit()
                raise ValueError('Record is not unique')
        else :
            res = []
            for id in xrange(0, len(data), 200) :
                if isinstance(lock, int) :
                    self.lock_advisory(lock, transaction_level=True)
                elif lock is not None :
                    self.lock_table(mode='EXCLUSIVE')
            
                res.extend([self.upsert_record_dictParam(d, None, insert_filter_clause, None, update_filter_clause, fields_to_upsert, autofill, return_fields, None, rewrite, False) for d in data[id:(id+200)]])
                self.commit()
            return res if fetchdata else True

    def insert_record(self, data=None, fields_to_insert=None, autofill=True, commit=True, return_fields=['*'], rewrite=True) :
        if self.__readonly__ :
            raise ValueError('Current table is read only')
        fetchdata = 1 if return_fields != None and len(return_fields) > 0 else 0

        if fields_to_insert == None :
            if autofill == False :
                fields = [field for field in self.__metadata__.keys() if field[:2] != '__' ]
            else :
                fields = [field for field in self.__metadata__.keys() if field[:2] != '__' and field not in self.__autofill__]
        else :
            fields = [field for field in self.__metadata__.keys() if field[:2] != '__' and (field not in self.__autofill__ or field in fields_to_insert)]

        if data == None :
            new_data = self.format_to_psql(self.__dict__)
        else :
            new_data = self.format_to_psql(data)
        if fetchdata :
            returns = 'RETURNING {0}'.format(', '.join(return_fields))
        else :
            returns = ''
        if isinstance(new_data, dict) :
            res = self.execute('INSERT INTO {0} ({1}) VALUES ({2}) {3}'.format(self.__table__, ', '.join(fields), ', '.join(['%s'] * len(fields)), returns), [new_data.get(fld, None) for fld in fields], commit=commit, fetchdata=int(returns != ''))
            if isinstance(res, dict) :
                new_data.update(res)
        else :
            res = []
            for id in xrange(0, len(new_data), 1000) :
                ins_clause = []
                ins_param = []
                for nd in new_data[id:(id+1000)]:
                    ins_param.extend( [nd.get(fld, None) for fld in fields] )
                    ins_clause.append('({0})'.format(', '.join(['%s'] * len(fields))))
                res.extend( self.execute('INSERT INTO {0} ({1}) VALUES {2} {3}'.format(self.__table__, ', '.join(fields), ', '.join(ins_clause), returns), ins_param, commit=commit, fetchdata=int(returns != '')) )
            if isinstance(res, list) and len(res) > 0 and isinstance(res[0], dict) :
                for id, nd in enumerate(new_data) :
                    nd.update(res[id])
            if commit :
                self.commit()
        if rewrite:
            self.import_dict(new_data, self_clean=True)
        if fetchdata :
            return self.export_dict(new_data, detailed=True)
        else :
            return True

    def update_record(self, data=None, fields_to_update=None, autofill=True, commit=True, return_fields=['*'], rewrite=True) :
        if self.__readonly__ :
            raise ValueError('Current table is read only')
        fetchdata = 1 if return_fields != None and len(return_fields) > 0 else 0
        if fields_to_update == None :
            if autofill == False :
                fields = [field for field in self.__metadata__.keys() if field[:2] != '__' and field not in self.__primary__]
            else :
                fields = [field for field in self.__metadata__.keys() if field[:2] != '__' and field not in self.__primary__ and field not in self.__autofill__]
        else :
            fields = [field for field in self.__metadata__.keys() if field[:2] != '__' and field in fields_to_update and field not in self.__primary__]
        if data == None :
            new_data = self.format_to_psql(self.__dict__)
        else :
            new_data = self.format_to_psql(data)
        if fetchdata:
            returns = 'RETURNING {0}'.format(', '.join(return_fields))
        else :
            returns = ''
        if isinstance(new_data, dict) :
            res = self.execute('UPDATE {0} SET {1} WHERE {2} {3}'.format(\
                self.__table__, \
                ', '.join(['{0}=%({0})s'.format(fld) for fld in fields]), \
                ' AND '.join(['{0}=%({0})s'.format(fld) for fld in self.__primary__]), \
                returns ), new_data, commit=commit, fetchdata=int(returns != ''))
            if isinstance(res, dict) :
                new_data.update( res )
        else :
            for nd in new_data:
                res = self.execute('UPDATE {0} SET {1} WHERE {2} {3}'.format(\
                    self.__table__, \
                    ', '.join(['{0}=%({0})s'.format(fld) for fld in fields]), \
                    ' AND '.join(['{0}=%({0})s'.format(fld) for fld in self.__primary__]), \
                    returns ), nd, commit=False, fetchdata=int(returns != ''))
                if isinstance(res, dict) :
                    nd.update(res)
            if commit :
                self.commit()
        if rewrite:
            self.import_dict(new_data, self_clean=True)
        if fetchdata :
            return self.export_dict(new_data, detailed=True)
        else :
            return True
    def load_record_batch(self, filter_source, filter_statement=None, fieldnames='*', limit=None,batch_size=1000) :
        if (isinstance(filter_source, list) or isinstance(filter_source, tuple) ) and isinstance(filter_source[0], dict):
            fields = sorted(filter_source[0].keys())
            if filter_statement is None :
                filter_statement = '({0})'.format(' AND '.join(['{0} = %({0})s'.format(fld) for fld in fields]))
            else :
                filter_statement = '({0})'.format(filter_statement)
            results = []
            for pos in xrange(0, len(filter_source), batch_size) :
                fs = filter_source[pos:(pos+batch_size)]
                statements = {}
                for id, flt in enumerate(fs) :
                    key = self.__curr__.mogrify(filter_statement, self.format_to_psql(flt))
                    if key not in statements :
                        statements [key] = [id]
                    else :
                        statements[key].append(id)
                res = [[] for state in fs]
                filter = ' OR '.join(statements)
                if not isinstance(fieldnames, basestring) and fieldnames is not None :
                    fieldnames = ', '.join(fieldnames)
                if limit != None :
                    filter += 'LIMIT {0}'.format(int(limit))
                if self.__readonly__ :
                    data = self.execute('SELECT {0} FROM (SELECT {3} FROM {1}) view_result WHERE {2}'.format(fieldnames, self.__table__, filter, self.__metadata__), fetchdata=2)
                else :
                    data = self.execute('SELECT {0} FROM {1} WHERE {2}'.format(fieldnames, self.__table__, filter), fetchdata=2)
                for d in data :
                    key = self.__curr__.mogrify(filter_statement, self.format_to_psql(d))
                    if key in statements :
                        for m in statements[key] :
                            res[m] = self.export_dict(d, detailed=True)
                results.extend(res)
            return results
        else :
            raise ValueError('filter_source is not in correct format')
    def load_record(self, filter=None, filter_param=None, fieldnames=None, sort_by=None, limit=None, rewrite=True, **kwarg) :
        #ot = time()
        if filter_param == None : filter_param = []
        if filter == None or filter.upper() not in ('ALL', 'SELF') :
            filter = [] if filter == None else [filter.split(';', 1)[0]]
            for fld, value in self.format_to_psql(kwarg).iteritems():
                if fld in self.__dict__ or self.__readonly__ :
                    filter.append('{0}=%s'.format(fld))
                    filter_param.append(value)
            filter = ' WHERE ' + ' AND '.join(filter)
        elif filter.upper() == 'ALL' :
            filter = ''
            filter_param = []
        elif filter.upper() == 'SELF' :
            if self.__readonly__ :
                raise ValueError('read only table cannot use a SELF load')
            filter = ' WHERE ' + ' AND '.join(['{0}=%s'.format(fld) for fld in self.__primary__])
            filter_param = [self.__dict__[fld] for fld in self.__primary__]
        if sort_by != None :
            filter += ' ORDER BY {0}'.format(str(sort_by).split(';', 1)[0])
        if limit != None :
            filter += ' LIMIT {0}'.format(str(limit).split(';', 1)[0])
        if fieldnames is None :
            fieldnames = '*'
        elif not isinstance(fieldnames, basestring) :
            fieldnames = ', '.join(fieldnames)
        #print 'A', time() - ot
        if self.__readonly__ :
            data = self.execute('SELECT {0} FROM (SELECT {3} FROM {1}) view_result {2}'.format(fieldnames, self.__table__, filter, self.__metadata__), filter_param, fetchdata=2)
            if rewrite and len(data) > 0 :
                self.import_dict(data[0])
        else :
            data = self.execute('SELECT {0} FROM {1} {2}'.format(fieldnames, self.__table__, filter), filter_param, fetchdata=2)
            if rewrite:
                if len(data) > 0 :
                    self.import_dict(data[0], self_clean=True)
                else :
                    self.clean_fieldname()
        #print 'C', time() -ot, filter
        return self.export_dict(data, detailed=True)
    def delete_record(self, filter=None, filter_param=None, commit=True, returning=False, **kwarg) :
        if filter_param == None : filter_param = []
        if self.__readonly__ :
            raise ValueError('the table is read only.')
        if filter == None or filter.upper() not in ('ALL', 'SELF') :
            filter = [] if filter == None else [filter.split(';', 1)[0]]
            for fld, value in self.format_to_psql(kwarg).iteritems():
                if fld in self.__dict__ :
                    filter.append('{0}=%s'.format(fld))
                    filter_param.append(value)
            filter = ' WHERE ' + ' AND '.join(filter)
        elif filter.upper() == 'ALL' :
            filter = ''
        elif filter.upper() == 'SELF' :
            filter = ' WHERE ' + ' AND '.join(['{0}=%s'.format(fld) for fld in self.__primary__])
            filter_param = [self.__dict__[fld] for fld in self.__primary__]
        if not returning :
            self.execute('DELETE FROM {0} {1}'.format(self.__table__, filter), filter_param, commit=commit)
            return True
        else :
            return self.execute('DELETE FROM {0} {1} RETURNING *'.format(self.__table__, filter), filter_param, commit=commit, fetchdata=2)

    def import_dict(self, data, self_clean=False) :
        if self_clean :
            self.clean_fieldname()
        if not self.__readonly__ :
            if isinstance(data, dict) :
                self.__dict__.update({k:v for k,v in self.export_dict(data, detailed=True).iteritems() if k in self.__metadata__})
            elif isinstance(data, list) :
                self.__dict__.update({k:v for k,v in self.export_dict(data[-1] if len(data) else {}, detailed=True).iteritems() 
                                      if k in self.__metadata__})
            else :
                return False
        else :
            if isinstance(data, dict) :
                self.__dict__.update(self.export_dict(data, detailed=True))
            elif isinstance(data, list) :
                self.__dict__.update(self.export_dict(data[-1], detailed=True))
            else :
                return False
        return True

if __name__ == '__main__' :
    #sys.exit(1)
    db = Psydict()
    db.upsert_record_dictParam(data=[dict(sequence='1', info={'a':1}), dict(sequence='3', info=None)], insert_filter_field='sequence', update_filter_clause='info = %(info)s')

import json
import re
import glob
import logging
from pathlib import Path
from sqlalchemy import MetaData, Table, Column, Index, text
from sqlalchemy.dialects import mysql, postgresql, sqlite
import sqlalchemy.types as satypes

log = logging.getLogger()

class ColumnSpec(object):
    TYPES = {
        'string': True,
        'binary': True,
        'integer': True,
        'float': True,
        'currency': True,
        'boolean': True,
        'record': True,
        'date': True,
        'datetime': True,
        'json': True,
        'uuid': True,
    }
    
    # spec fields:
    #        name
    #        type
    #        length
    #        length_specify
    #        nullable
    #        default
    #        default_value
    def __init__(self, spec):
        for k in spec.keys():
            setattr(self, k, spec[k])
        if self.type not in self.TYPES:
            raise Exception('Invalid type: {}'.format(self.type))

    def build_column(self, engine):
        coltype = self.type
        sqltype = None
        if coltype == 'string':
            if self.length == 'default':
                sqltype = satypes.Text
            elif self.length == 'long':
                if engine == 'mysql':
                    sqltype = mysql.LONGTEXT
                else:
                    sqltype = satypes.Text
            elif self.length == 'specify':
                sqltype = satypes.VARCHAR(int(self.length_specify))
        elif coltype == 'binary':
            if self.length == 'default':
                if engine == 'postgresql':
                    sqltype = postgresql.BYTEA
                else:
                    sqltype = satypes.BLOB
            elif self.length == 'long':
                if engine == 'mysql':
                    sqltype = mysql.LONGBLOB
                elif engine == 'postgresql':
                    sqltype = postgresql.BYTEA
                elif engine == 'sqlite':
                    sqltype = satypes.BLOB
            elif self.length == 'specify':
                if engine == 'postgresql':
                    sqltype = postgresql.BYTEA
                else:
                    sqltype = satypes.VARBINARY(int(self.length_specify))
        elif coltype == 'integer':
            if engine == 'sqlite':
                sqltype = satypes.Integer
            else:
                sqltype = satypes.BigInteger
        elif coltype == 'float':
            sqltype = satypes.Float
        elif coltype == 'currency':
            sqltype = satypes.Numeric(20, 2)
        elif coltype == 'boolean':
            sqltype = satypes.Boolean
        elif coltype == 'record':
            if engine == 'sqlite':
                sqltype = satypes.Integer
            else:
                sqltype = satypes.BigInteger
        elif coltype == 'date':
            sqltype = satypes.Date
        elif coltype == 'datetime':
            sqltype = satypes.DateTime
        elif coltype == 'json':
            sqltype = satypes.JSON
        elif coltype == 'uuid':
            if engine == 'postgresql':
                sqltype = postgresql.UUID
            else:
                sqltype = satypes.VARBINARY(16)
        else:
            raise Exception('Invalid type: {}'.format(coltype))
        if sqltype is None:
            raise Exception('Type not found: {}'.format(coltype))
        args = {
            'nullable': self.nullable
        }
        if self.default == 'null':
            args['server_default'] = text('NULL')
        elif self.default == 'specify':
            args['server_default'] = self.default_value
        return Column(self.name, sqltype, **args)

class IndexSpec(object):
    # spec fields:
    #   name
    #   type: 'index' | 'unique'
    #   columns
    def __init__(self, spec):
        for k in spec.keys():
            setattr(self, k, spec[k])
        if not(self.type == 'index' or self.type == 'unique'):
            raise Exception('Invalid type: {}'.format(self.type))

    def build_index(self, engine):
        uniq = False
        if self.type == 'unique':
            uniq = True
        cols = []
        for c in self.columns:
            ctxt = c['column']
            if 'size' in c:
                ctxt = '{}({})'.format(ctxt, c['size'])
            cols.append(ctxt)
        return Index(self.name, *cols, unique=uniq)

class TableSpec(object):
    def __init__(self, table):
        self.table = table
        self.column = []
        self.column_key = {}
        self.index = []
        self.index_key = {}

    @classmethod
    def read_file(cls, filepath):
        with open(filepath, 'r') as fh:
            spec = json.load(fh)
            obj = cls(spec['table'])
            obj.from_json(spec)
            return obj

    def write_file(self, filepath):
        with open(filepath, 'w') as fh:
            spec = self.to_json()
            json.dump(spec, fh, sort_keys=True, indent=4)

    def to_json(self):
        columns = []
        indexes = []
        for cl in self.column:
           columns.append(cl.copy())
        for ix in self.index:
           indexes.append(ix.copy())
        return {
            'table': self.table,
            'column': columns,
            'index': indexes,
        }

    def from_json(self, spec):
        # TODO: verify table name
        # Columns
        for cl in spec['column']:
            # TODO: verify this data!
            self.column_key[cl['name']] = cl
        self.column = []
        for k in sorted(self.column_key.keys()):
            self.column.append(self.column_key[k])
        # Indexes
        for ix in spec['index']:
            # TODO: verify this data!
            self.index_key[ix['name']] = ix
        self.index = []
        for k in sorted(self.index_key.keys()):
            self.index.append(self.index_key[k])

    def check_column(self, colname):
        if not re.search(r'^[a-zA-Z_][a-zA-Z0-9_]*$', colname):
            raise Exception('Invalid column name: {}'.format(colname))
        return True

    def add_column(self, colspec):
        name = colspec['name']
        if name in self.column_key:
            raise Exception('Duplicate column: {}'.format(name))
        self.column_key[name] = colspec
        self.column = []
        for k in sorted(self.column_key.keys()):
            self.column.append(self.column_key[k])

    def remove_column(self, key):
        if key not in self.column_key:
            raise Exception('Unknown column: {}'.format(key))
        if 'indexes' in self.column_key[key]:
            for idx in self.column_key[key]['indexes']:
                self.remove_index(idx)
        del self.column_key[key]
        self.column = []
        for k in sorted(self.column_key.keys()):
            self.column.append(self.column_key[k])

    def get_columns(self):
        cols = []
        for c in self.column:
            cols.append(ColumnSpec(c))
        return cols

    def get_column(self, name):
        if name not in self.column_key:
            raise Exception('Unknown column: {}'.format(name))
        return ColumnSpec(self.column_key[name])

    def check_index_columns(self, columns):
        pts = columns.split(',')
        cols = []
        for col in pts:
            col = col.strip()
            m = re.search(r'^([a-zA-Z_][a-zA-Z0-9_]*)$', col)
            if m:
                colname = m.group(1)
                size = None
            else:
                m = re.search(r'^([a-zA-Z_][a-zA-Z0-9_]*)\((\d+)\)$', col)
                if m:
                    colname = m.group(1)
                    size = m.group(2)
                else:
                    raise Exception('Invalid index column specification: {}'.format(col))
            if colname not in self.column_key:
                raise Exception('Index column not found: {}'.format(colname))
            rc = {'column': colname}
            if size is not None:
                rc['size'] = size
            cols.append(rc)
        return cols

    def add_index(self, idxspec):
        name = idxspec['name']
        if name in self.index_key:
            raise Exception('Duplicate index: {}'.format(name))
        self.index_key['name'] = idxspec
        for cs in idxspec['columns']:
            coldata = self.column_key[cs['column']]
            coldata.setdefault('indexes', [])
            coldata['indexes'].append(name)
            coldata['indexes'] = sorted(coldata['indexes'])
        self.index = []
        for k in sorted(self.index_key.keys()):
            self.index.append(self.index_key[k])

    def remove_index(self, key):
        if key not in self.index_key:
            raise Exception('Unknown index: {}'.format(key))
        cols = self.index_key[key]['columns']
        for ix in cols:
            cs = self.column_key[ix['column']]
            cs['indexes'] = list(filter(key.__ne__, cs['indexes']))
            if len(cs['indexes']) == 0:
                del cs['indexes']
        del self.index_key[key]
        self.index = []
        for k in sorted(self.index_key.keys()):
            self.index.append(self.index_key[k])

    def get_indexes(self):
        idxs = []
        for i in self.index:
            idxs.append(IndexSpec(i))
        return idxs

    def get_index(self, name):
        if name not in self.index_key:
            raise Exception('Unknown index: {}'.format(name))
        return IndexSpec(self.index_key[name])

class TableBuilder(object):
    def __init__(self, **param):
        self.metadata = param.get('metadata', MetaData())

    def build_sqlalchemy_table(self, tblspec, engine='mysql'): 
        if engine == 'sqlite':
            ptype = satypes.Integer
        else:
            ptype = satypes.BigInteger
        cols = []
        cols.append(Column('id', ptype, nullable=False, primary_key=True))
        for col in tblspec.get_columns():
            cols.append(col.build_column(engine))
        for idx in tblspec.get_indexes():
            cols.append(idx.build_index(engine))
        extra = {}
        if engine == 'sqlite':
            extra['sqlite_autoincrement'] = True
        table = Table(tblspec.table, self.metadata, *cols, **extra)
        return table

    def build_sqlalchemy_schema(self, path, engine='mysql'):
        pattern = str(Path(path) / '*.js')
        tables = sorted(glob.glob(pattern))
        log.error('Path: {} Tables: {}'.format(path, tables))
        res = []
        for table in tables:
            ts = TableSpec.read_file(table)
            t = self.build_sqlalchemy_table(ts, engine)
            log.info('Table: {} {}'.format(table, t))
            res.append([table, t])
        return res


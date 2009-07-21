import sys, os

from hbase.ttypes import *
from hbase import Hbase

from thrift import Thrift
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol

class HBaseConnection:
    def __init__(self, hostname='localhost', port=9090):
        self.hostname = hostname
        self.port = port
        self.transport = None
        self.protocol = None
        self.client = None
 
    def connect(self):
        if self.client == None or self.transport == None or not self.transport.isOpen():
            # Make socket
            self.transport = TSocket.TSocket(self.hostname, str(self.port))
            # Buffering is critical. Raw sockets are very slow
            self.transport = TTransport.TBufferedTransport(self.transport)
            # Wrap in a protocol
            self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
            # Create a client to use the protocol encoder
            self.client = Hbase.Client(self.protocol)
            print "Opening hbase connection on %s %s" % (self.hostname, self.port)
            self.transport.open()
            print "Opened"
   
    def disconnect(self):
        print "Closing hbase connection"
        if self.transport:
            self.transport.close()
    
    def validate_column_descriptors(self, table_name, column_descriptors):
        hbase_families = self.client.getColumnDescriptors(table_name)
        for col_desc in column_descriptors:
            family, column = col_desc.split(":")
            if not family in hbase_families:
                raise Exception("Invalid column descriptor \"%s\" for hbase table \"%s\"" % (col_desc, table_name))
      
    def validate_table_name(self, table_name):
        if not table_name in self.client.getTableNames():
            raise Exception("hbase table '%s' not found." % (table_name))
        
    def put(self, table, puts):
        batches = []
        for put in puts:
            mutations = []
            columns = put.columns
            for family in columns:
                for qualifier, value in columns[family].iteritems():
                    column = "%s:%s" % (family, qualifier)
                    mutations.append(Mutation(column=column, value=value))
            if len(mutations) > 0:
                batches.append(BatchMutation(row=put.row, mutations=mutations))
        if len(batches) > 0:
            try:
                self.client.mutateRows(table, batches)
            except IOError, e:
                raise Exception(e.message)
        
    def delete(self, table, deletes):
        print "Bug in thrift server, we can't delete at the moment sorry..."
        batches = []
        for delete in deletes:
            mutations = []
            columns = delete.columns
            for family in columns:
                for qualifier in columns[family]:
                    column = "%s:%s" % (family, qualifier)
                    mutations.append(Mutation(column=column, isDelete=True))
            if len(mutations) > 0:
                batches.append(BatchMutation(row=delete.row, mutations=mutations))
        #aborting without doing a delete
        return
        if len(batches) > 0:
            try:
                self.client.mutateRows(table, batches)
            except Exception, e:
                raise Exception(e.message)
 
    def get(self, table, get):
        cols = []
        columns = get.columns
        for family in columns:
            for qualifier in columns[family]:
                col = "%s:%s" % (family, qualifier)
                cols.append(col)
        results = None
        try:
            if len(cols) > 0:
                results = self.client.get(table, get.row, cols)
            else:
                results = self.client.getRow(table, get.row)
        except Exception, e:
            raise Exception(e.message)
        result = None
        if results:
            rowresult = results[0]
            result = Result(rowresult.row)
            columns = rowresult.columns
            for column in columns:
                family, qualifier = column.split(':')
                result.add(family, qualifier, columns[column].value)
        return result
        
class Put(object):
    def __init__(self, row):
        self.row = row
        self.columns = {}
    def add(self, familyQualifier, value):
        family, qualifier = familyQualifier.split(':')
        self.add(family, qualifier, value)
    def add(self, family, qualifier, value):
        f = self.columns.get(family, {})
        f[qualifier] = value
        self.columns[family] = f
        
class Delete(object):
    def __init__(self, row):
        self.row = row
        self.columns = {}
    def add(self, familyQualifier):
        family, qualifier = familyQualifier.split(':')
        self.add(family, qualifier)
    def add(self, family, qualifier):
        f = self.columns.get(family, [])
        f.append(qualifier)
        self.columns[family] = f

class Get(object):
    def __init__(self, row):
        self.row = row
        self.columns = {}
    def add(self, familyQualifier):
        family, qualifier = familyQualifier.split(':')
        self.add(family, qualifier)
    def add(self, family, qualifier):
        f = self.columns.get(family, [])
        f.append(qualifier)
        self.columns[family] = f

class Result(object):
    def __init__(self, row):
        self.row = row
        self.columns = {}
    def add(self, familyQualifier, value):
        family, qualifier = familyQualifier.split(':')
        self.add(family, qualifier, value)
    def add(self, family, qualifier, value):
        f = self.columns.get(family, {})
        f[qualifier] = value
        self.columns[family] = f
    def get(self, family):
        return self.columns.get(family, None)
    def get(self, family, qualifier):
        family = self.columns.get(family, {})
        return family.get(qualifier, None)



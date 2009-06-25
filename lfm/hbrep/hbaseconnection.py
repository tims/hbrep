import sys, os

from hbase.ttypes import *
from hbase import Hbase

from thrift import Thrift
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol

class HBaseConnection:
    def __init__(self, hostname, port):
       self.hostname = hostname
       self.port = port
       self.transport = None
       self.protocol = None
       self.client = None
 
    def connect(self):  
        if self.client == None or self.transport == None or not self.transport.isOpen():
            # Make socket
            self.transport = TSocket.TSocket(self.hostname, self.port)
            # Buffering is critical. Raw sockets are very slow
            self.transport = TTransport.TBufferedTransport(self.transport)
            # Wrap in a protocol
            self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
            # Create a client to use the protocol encoder
            self.client = Hbase.Client(self.protocol)
            print "Opening hbase connection on %s %s" % (self.hostname, self.port)
            self.transport.open()
   
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
        batches = []
        for delete in deletes:
            mutations = []
            columns = delete.columns
            for family in columns:
                for qualifier in columns[family]:
                    column = "%s:%s" % (family, qualifier)
                    mutations.append(Mutation(column=column, isDelete=True))
            if len(mutations) > 0:
                batches.append(BatchMutation(row=put.row, mutations=mutations))
        if len(batches) > 0:
            try:
                self.client.mutateRows(table, batches)
            except Exception, e:
                raise Exception(e.message)
            
class Put(object):
    def __init__(self, row):
        self.row = row
        self.columns = {}
    def add(self, familyQualifier, value):
        family, qualifier = familyQualifier.split(':')
        self.add(family, qualifier, value)
    def add(self, family, qualifier, value):
        value = unicode(value, 'utf-8')
        f = self.columns.get(family, {})
        f[qualifier] = value.encode( "utf-8" )
        self.columns[family] = f
        
class Delete(object):
    def __init__(self, row):
        self.row = row
        self.columns = {}
    def add(self, familyQualifier):
        family, qualifier = familyQualifier.split(':')
        self.add(family, qualifier)
    def add(self, family, qualifier):
        value = unicode(value, 'utf-8')
        f = self.columns.get(family, [])
        f.append(qualifier)
        self.columns[family] = f
    
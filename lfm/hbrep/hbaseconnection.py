import sys, os

from hbase.ttypes import *
from hbase import Hbase

from thrift import Thrift
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol

class HBaseConnection:
    def __init__(self, hostname, port):
        # Make socket
        self.transport = TSocket.TSocket(hostname, port)
        # Buffering is critical. Raw sockets are very slow
        self.transport = TTransport.TBufferedTransport(self.transport)
        # Wrap in a protocol
        self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        # Create a client to use the protocol encoder
        self.client = Hbase.Client(self.protocol)
 
    def connect(self):  
        self.transport.open()
   
    def disconnect(self):
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
        
    def put(self, table, put):
        if type(put) != Put:
            raise Exception("not put type");
        mutations = []
        columns = put.columns
        for family in columns:
            for qualifier, value in columns[family].iteritems():
                column = "%s:%s" % family, qualifier
                mutations.append(Mutation(column=column, value=value))
        self.client.mutateRow(table, row, mutations)
        
    def delete(self, table, delete):
        if type(delete) != Delete:
            raise Exception("not delete type");
        mutations = []
        columns = delete.columns
        for family in columns:
            for qualifier in columns[family]:
                column = "%s:%s" % family, qualifier
                mutations.append(Mutation(column=column, isDelete=True))
        self.client.mutateRow(table, row, mutations)
  
class Put:
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
        
class Delete:
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
    
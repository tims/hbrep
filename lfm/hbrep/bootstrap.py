import sys, os

import pgq, pgq.producer
import skytools, yaml

from hbaseconnection import *
import tablemapping

class HBaseBootstrap(skytools.DBScript):
    """Bootstrapping script for loading columns from a table in postgresql to hbase."""
  
    def __init__(self, service_name, args):
        # This will process any options eg -k -v -d
        skytools.DBScript.__init__(self, service_name, args)
    
        config_file = self.args[0]
        if len(self.args) < 2:
            print "need table names"
            sys.exit(1)
        else:
            self.table_names = self.args[1:]
    
        self.mappingsFile = "mappings.yaml"
        self.mappings = yaml.load(file(self.mappingsFile, 'r'))

        self.max_batch_size = int(self.cf.get("max_batch_size", "1000"))
        self.hbase_hostname = self.cf.get("hbase_hostname", "localhost")
        self.hbase_port = int(self.cf.get("hbase_port", "9090"))
        self.dumpfile = self.cf.get("bootstrap_tmpfile", "tmpdump.dat")
        self.db = self.cf.get("postgresql_db")
  
    def startup(self):
        # make sure the script loops only once.
        self.set_single_loop(1)
        self.log.info("Starting " + self.job_name)
    
    def work(self):
        for t in self.table_names:
            self.bootstrap_table(t)
      
    def bootstrap_table(self, table_name):
        self.log.info("Bootstrapping table %s" % table_name)
        schema, table = table_name.split('.')

        mapping = None
        if schema in self.mappings:
            mapping = self.mappings[schema].get(table, None)
        if not mapping:
            raise Exception("table not specified in mappings file")

        #self.dumpPostgres(table, schema, mapping)
        self.loadHBase(mapping)

    def dumpPostgres(self, table, schema, mapping):
        row = mapping['row']
        columns = mapping['columns']
        self.dumpToFile(schema + "." + table, [row] + columns.keys(), self.dumpfile)
    
    def loadHBase(self, mapping):
        try:
            hbase = HBaseConnection(self.hbase_hostname, self.hbase_port)
            try:
                print mapping
                self.log.debug('Connecting to HBase')
                hbase.connect()
                hbaseTable = mapping['table']
                columns = mapping['columns']
                row = mapping['row']
                hbase.validate_table_name(hbaseTable)
                hbase.validate_column_descriptors(hbaseTable, columns.values())
                # max number of rows to fetch at once
                batchsize = self.max_batch_size
                total_rows = 0L
         
                self.log.debug('Starting puts to hbase with batchsize %s' % batchsize)
                puts = []
                for line in file(self.dumpfile):
                    parts = line.split("\t")
                    row, values = parts[0], parts[1:]
                    put = Put(row)
                    for column, value in zip(columns.values(), values):
                        family, qualifier = column.split(':')
                        put.add(family, qualifier, value)
                    puts.append(put)
                    if len(puts) > batchsize:
                        hbase.put(hbaseTable, puts)
                if len(puts) > 0:
                    hbase.put(hbaseTable, puts)
                self.log.info("total rows put = %d" % (total_rows))
                self.log.info("Bootstrapping table %s complete" % table_name)
            except Exception, e:
                #self.log.info(e)
                sys.exit(e)
        finally:
            hbase.disconnect()
  
    def dumpToFile(self, table, columns, outputfile):
        hostname = "localhost"
        port = 7071
        username = "lastfm"
        db = "last"
        sqlfile = "sqlfile"
    
        f = open(sqlfile, 'w')
        f.write("\copy %s(\"%s\") to %s" % (table, "\",\"".join(columns), outputfile))
        f.write(" with delimiter as '\t' null as '-'")
        f.close()
    
        opts = ["-h %s" % hostname,
                "-p %s" % port,
                "-U %s" % username,
                "-d %s" % db,
                "-f %s" % sqlfile]
        command = "psql " + " ".join(opts)
        print command
        os.system(command)
  
if __name__ == '__main__':
    bootstrap = HBaseBootstrap("HBaseReplic",sys.argv[1:])
    bootstrap.start()



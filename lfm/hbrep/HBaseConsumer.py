import sys, os, pgq, skytools, ConfigParser

from hbaseconnection import *
import tablemapping
import yaml

INSERT = 'I'
UPDATE = 'U'
DELETE = 'D'

class HBaseConsumer(pgq.Consumer):
    """HBaseConsumer is a pgq.Consumer that sends processed events to hbase as mutations."""
  
    def __init__(self, servicename, config_file, *args):
        pgq.Consumer.__init__(self, servicename, "postgresql_db", [config_file] + list(args))
    
        self.mappingsFile = "mappings.yaml"
        self.mappings = yaml.load(file(self.mappingsFile, 'r'))
        
        self.max_batch_size = int(self.cf.get("max_batch_size", "10000"))
        self.hbase_hostname = self.cf.get("hbase_hostname", "localhost")
        self.hbase_port = int(self.cf.get("hbase_port", "9090"))
        self.row_limit = int(self.cf.get("bootstrap_row_limit", 0))
    
    def process_batch(self, source_db, batch_id, event_list):
        try:
            self.log.debug("processing batch %s" % (batch_id))
            hbase = HBaseConnection(self.hbase_hostname, self.hbase_port)
            try:
                #TODO: use a persistent connection, that restarts if it goes down and we should use for multiple batches.
                self.log.debug("Connecting to HBase")
                hbase.connect()
                i = 0L
                for event in event_list:
                    i = i + 1
                    self.process_event(event, hbase)
                print "%i events processed" % (i)
            except Exception, e:
                self.log.info(e)
                sys.exit(e)
        finally:
            self.log.debug("Disconnecting from hbase")
            hbase.disconnect()
  
    def process_event(self, event, hbase):
        schema, table = event.ev_extra1.split('.')
        mapping = None
        if schema in self.mappings:
            mapping = self.mappings[schema].get(table, None)
        if mapping == None:
            self.log.info("table name %s not found in config, skipping event" % event.ev_extra1)
            event.tag_done()
            return
        
        event_data = skytools.db_urldecode(event.data)
        event_type = event.type.split(':')[0]
        
        rowprefix = mapping.get('rowprefix', '')
        rowcolumn = mapping.get('row', None)
        if rowcolumn: row = event_data.get(rowcolumn, None)
        if row:
            row = rowprefix + row
        else:
            raise Exception("row column %s not found" % rowcolumn)
        hbasetable = mapping['table']
        columns = mapping['columns']
        
        if event_type == INSERT or event_type == UPDATE:
            put = Put(row)
            for psqlCol, hbaseCol in columns.iteritems():
                value = event_data.get(psqlCol, None)
                family, qualifier = hbaseCol.split(':')
                if value:
                    put.add(family, qualifier, value)
            hbase.put(hbasetable, put)
        elif event_type == DELETE:
            delete = Delete(row)
            for psqlCol, hbaseCol in columns.iteritems():
                if psqlCol in event_data:
                    family, qualifier = hbaseCol.split(':')
                    delete.add(family, qualifier)
            hbase.delete(hbasetable, delete)
        else:
            raise Exception("Invalid event type: %s, event data was: %s" % (event_type, str(event_data)))
        event.tag_done()

        
if __name__ == '__main__':
    script = HBaseConsumer("HBaseReplic", sys.argv[1:])
    script.start()

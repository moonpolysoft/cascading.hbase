package cascading.hbase;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;

import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tap.hadoop.TapCollector;
import cascading.tap.hadoop.TapIterator;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

public class HBaseTap extends Tap {
  public String tableName;
  
  //===========================================================================
  //Constructors
  
  public HBaseTap(String tableName, HBaseScheme scheme) {
    super(scheme, SinkMode.APPEND);
    this.tableName = tableName;
  }
  
  public HBaseTap(String tableName, HBaseScheme scheme, SinkMode mode) {
    super(scheme, mode);
    this.tableName = tableName;
  }
  
  //===========================================================================
  //Tap API
  public boolean deletePath(JobConf conf) {
    try {
      HBaseAdmin admin = new HBaseAdmin(new HBaseConfiguration());

      if (!admin.tableExists(tableName)) {
        return true;
      }

      admin.disableTable(tableName);
      admin.deleteTable(tableName);

      return true;
    } catch (IOException e) {
      throw new TapException(e);
    }
  }
  
  public Path getPath() {
    return new Path(getURI().toString());
  }
  
  public long getPathModified() {
    return System.currentTimeMillis();
  }
  
  public boolean makeDirs(JobConf conf) {
    HBaseAdmin admin;
    try {
      admin = new HBaseAdmin(new HBaseConfiguration());


      if (admin.tableExists(tableName)) {
        return true;
      }

      HTableDescriptor desc = new HTableDescriptor(tableName);
      byte[][] columnFamilies = ((HBaseScheme) getScheme()).getFamilies();

      for (byte[] cf : columnFamilies) {
        HColumnDescriptor cfd = new HColumnDescriptor(cf);
        desc.addFamily(cfd);
      }

      admin.createTable(desc);

      return true;
    } catch (IOException e) {
      throw new TapException(e);
    }
  }
  
  public boolean pathExists(JobConf conf) {
    try {
      return new HBaseAdmin(new HBaseConfiguration()).tableExists(tableName);
    } catch (MasterNotRunningException e) {
      throw new TapException(e);
    }
  }

  public long getPathModified(JobConf jobConf) throws IOException {
    return System.currentTimeMillis();
  }

  public TupleEntryIterator openForRead(JobConf conf) {
    try {
      return new TupleEntryIterator(getSourceFields(), new TapIterator(this, conf));
    } catch (IOException e) {
      throw new TapException(e);
    }
  }
  
  public TupleEntryCollector openForWrite(JobConf conf) {
    try {
      return new TapCollector(this, conf);
    } catch (IOException e) {
      throw new TapException(e);
    }
  }
  
  public void sinkInit(JobConf conf) throws IOException {
    if (isReplace() & conf.get("mapred.task.partition") == null) {
      deletePath(conf);
    }
    
    makeDirs(conf);
    
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName);
    super.sinkInit(conf);
  }
  
  @SuppressWarnings("deprecation")
  public void sourceInit(JobConf conf) throws IOException {
    conf.set(TableInputFormat.INPUT_TABLE, tableName);
    FileInputFormat.addInputPath( conf, getPath() );
    super.sourceInit(conf);
  }
  //===========================================================================
  
  protected URI getURI() {
    try { 
      return new URI("hbase://", tableName, null);
    } catch(URISyntaxException ex) {
      throw new TapException("unable to create URI", ex);
    }
  }
}
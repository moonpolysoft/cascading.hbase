package cascading.hbase;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

/**
 * This is a wrapper class to mate the new HBase API with the old hadoop API, since I have no desire
 * to update cascading to use the new hadoop API.
 * 
 * @author cliff
 *
 * @param <KEY>
 */
@SuppressWarnings("deprecation")
public class TableOutputFormatWrap<KEY> extends TableOutputFormat<KEY> implements OutputFormat<KEY, Writable> {

  public void checkOutputSpecs(FileSystem arg0, JobConf arg1)
      throws IOException {
    // TODO Auto-generated method stub
    
  }

  public RecordWriter<KEY, Writable> getRecordWriter(FileSystem fs, JobConf conf,
      String name, Progressable progressable) throws IOException {
    String tableName = conf.get(OUTPUT_TABLE);
    HTable table = null;
    table = new HTable(new HBaseConfiguration(conf), tableName);
    table.setAutoFlush(false);
    return new TableRecordWriter<KEY>(table);
  }
  
  protected static class TableRecordWriter<KEY> 
  implements RecordWriter<KEY, Writable> {
    
    /** The table to write to. */
    private HTable table;

    /**
     * Instantiate a TableRecordWriter with the HBase HClient for writing.
     * 
     * @param table  The table to write to.
     */
    public TableRecordWriter(HTable table) {
      this.table = table;
    }

    /**
     * Closes the writer, in this case flush table commits.
     * 
     * @param context  The context.
     * @throws IOException When closing the writer fails.
     * @see org.apache.hadoop.mapreduce.RecordWriter#close(org.apache.hadoop.mapreduce.TaskAttemptContext)
     */
    public void close(Reporter reporter) 
    throws IOException {
      table.flushCommits();
    }

    /**
     * Writes a key/value pair into the table.
     * 
     * @param key  The key.
     * @param value  The value.
     * @throws IOException When writing fails.
     * @see org.apache.hadoop.mapreduce.RecordWriter#write(java.lang.Object, java.lang.Object)
     */
    public void write(KEY key, Writable value) 
    throws IOException {
      if (value instanceof Put) this.table.put(new Put((Put)value));
      else if (value instanceof Delete) this.table.delete(new Delete((Delete)value));
      else throw new IOException("Pass a Delete or a Put");
    }
  }

}

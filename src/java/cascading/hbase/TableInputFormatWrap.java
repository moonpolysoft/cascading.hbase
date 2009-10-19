package cascading.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.StringUtils;

public class TableInputFormatWrap extends TableInputFormat implements InputFormat<ImmutableBytesWritable, Result> {

  public RecordReader<ImmutableBytesWritable, Result> getRecordReader(
      InputSplit split, JobConf conf, Reporter reporter) throws IOException {
    TableSplit tSplit = (TableSplit)split;
    Scan sc = new Scan(getScan());
    sc.setStartRow(tSplit.getStartRow());
    sc.setStopRow(tSplit.getEndRow());
    TableRecordReader reader = new TableRecordReader();
    reader.setScan(sc);
    reader.setHTable(getHTable());
    reader.init();
    return reader;
  }

  public InputSplit[] getSplits(JobConf conf, int numSplits) throws IOException {
    HTable table = getHTable();
    Scan scanner = getScan();
    byte[] startRow = scanner.getStartRow();
    byte[] endRow = scanner.getStopRow();
    
    byte [][] startKeys = table.getStartKeys();
    if (startKeys == null || startKeys.length == 0) {
      throw new IOException("Expecting at least one region.");
    }
    if (table == null) {
      throw new IOException("No table was provided.");
    }
    byte [][] rangedKeys;
    
    if (startRow == null && endRow == null) {
      rangedKeys = startKeys;
    } else {
      List<byte[]> acc = new ArrayList<byte[]>();
      for (byte[] key : startKeys) {
        if ((startRow == null || Bytes.compareTo(key, startRow) >= 0) && (endRow == null || Bytes.compareTo(key, endRow) <= 0)) {
          acc.add(key);
        }
      }
      rangedKeys = acc.toArray(new byte[][] {});
    }
    
    int realNumSplits = rangedKeys.length;
    InputSplit[] splits = new InputSplit[realNumSplits];
    int middle = rangedKeys.length / realNumSplits;
    int startPos = 0;
    for (int i = 0; i < realNumSplits; i++) {
      int lastPos = startPos + middle;
      lastPos = rangedKeys.length % realNumSplits > i ? lastPos + 1 : lastPos;
      String regionLocation = table.getRegionLocation(rangedKeys[startPos]).
        getServerAddress().getHostname(); 
      splits[i] = new TableSplitWrap(table.getTableName(),
          rangedKeys[startPos], ((i + 1) < realNumSplits) ? rangedKeys[lastPos]:
          HConstants.EMPTY_START_ROW, regionLocation);
      startPos = lastPos;
    }
    return splits;
  }
  
  protected class TableRecordReader
  implements RecordReader<ImmutableBytesWritable, Result> {
    
    private ResultScanner scanner = null;
    private Scan scan = null;
    private HTable htable = null;
    private byte[] lastRow = null;

    /**
     * Restart from survivable exceptions by creating a new scanner.
     *
     * @param firstRow  The first row to start at.
     * @throws IOException When restarting fails.
     */
    public void restart(byte[] firstRow) throws IOException {
      Scan newScan = new Scan(scan);
      newScan.setStartRow(firstRow);
      this.scanner = this.htable.getScanner(newScan);      
    }

    /**
     * Build the scanner. Not done in constructor to allow for extension.
     *
     * @throws IOException When restarting the scan fails. 
     */
    public void init() throws IOException {
      restart(scan.getStartRow());
    }

    /**
     * Sets the HBase table.
     * 
     * @param htable  The {@link HTable} to scan.
     */
    public void setHTable(HTable htable) {
      this.htable = htable;
    }

    /**
     * Sets the scan defining the actual details like columns etc.
     *  
     * @param scan  The scan to set.
     */
    public void setScan(Scan scan) {
      this.scan = scan;
    }

    /**
     * Closes the split.
     * 
     * @see org.apache.hadoop.mapreduce.RecordReader#close()
     */
    public void close() {
      this.scanner.close();
    }

    /**
     * Initializes the reader.
     * 
     * @param inputsplit  The split to work with.
     * @param context  The current task context.
     * @throws IOException When setting up the reader fails.
     * @throws InterruptedException When the job is aborted.
     * @see org.apache.hadoop.mapreduce.RecordReader#initialize(
     *   org.apache.hadoop.mapreduce.InputSplit, 
     *   org.apache.hadoop.mapreduce.TaskAttemptContext)
     */
    public void initialize(InputSplit inputsplit,
        TaskAttemptContext context) throws IOException,
        InterruptedException {
    }

    /**
     * Positions the record reader to the next record.
     *  
     * @return <code>true</code> if there was another record.
     * @throws IOException When reading the record failed.
     * @throws InterruptedException When the job was aborted.
     * @see org.apache.hadoop.mapreduce.RecordReader#nextKeyValue()
     */
    public boolean next(ImmutableBytesWritable key, Result value) throws IOException {
      if (key == null) key = new ImmutableBytesWritable();
      if (value == null) value = new Result();
      try {
        value = this.scanner.next();
      } catch (IOException e) {
        restart(lastRow);
        scanner.next();    // skip presumed already mapped row
        value = scanner.next();
      }
      if (value != null && value.size() > 0) {
        key.set(value.getRow());
        lastRow = key.get();
        return true;
      }
      return false;
    }

    public ImmutableBytesWritable createKey() {
      return new ImmutableBytesWritable();
    }

    public Result createValue() {
      return new Result();
    }

    public long getPos() throws IOException {
      return 0;
    }

    public float getProgress() throws IOException {
      return 0;
    }
  }

}

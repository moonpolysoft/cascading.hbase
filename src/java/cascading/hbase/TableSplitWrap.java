package cascading.hbase;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.mapred.InputSplit;

public class TableSplitWrap extends TableSplit implements InputSplit {
  public TableSplitWrap() {
    this(HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY,
      HConstants.EMPTY_BYTE_ARRAY, "");
  }
  
  public TableSplitWrap(byte [] tableName, byte [] startRow, byte [] endRow,
      final String location) {
    super(tableName, startRow, endRow, location);
  }
}

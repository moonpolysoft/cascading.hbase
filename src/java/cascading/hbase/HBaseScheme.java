/*
 * Copyright (c) 2009 Concurrent, Inc.
 *
 * This work has been released into the public domain
 * by the copyright holder. This applies worldwide.
 *
 * In case this is not legally possible:
 * The copyright holder grants any entity the right
 * to use this work for any purpose, without any
 * conditions, unless such conditions are required by law.
 */

package cascading.hbase;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;

import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * The HBaseScheme class is a {@link Scheme} subclass. It is used in conjunction
 * with the {@HBaseTap} to allow for the reading and writing of data
 * to and from a HBase cluster.
 * 
 * @see HBaseTap
 */
@SuppressWarnings("deprecation")
public class HBaseScheme extends Scheme {
  private Fields keyField;
  private transient Scan scanner;
  private Map<String, byte[][]> fieldMap;
  private Fields valueFields;

  // ===========================================================================
  // Constructors

  public HBaseScheme(Scan scanner, Fields keyField,
      Map<String, byte[][]> fieldMap) {
    this.keyField = keyField;
    this.scanner = scanner;
    this.fieldMap = fieldMap;
    this.valueFields = new Fields((Comparable[]) fieldMap.keySet().toArray());

    setSourceSink(keyField, new Fields[] { valueFields });

    validate();
  }

  @SuppressWarnings("unchecked")
  public Tuple source(Object key, Object value) {
    Tuple result = new Tuple();

    ImmutableBytesWritable keyWritable = (ImmutableBytesWritable) key;
    Result row = (Result) value;

    result.add(Bytes.toString(keyWritable.get()));
    
    for (Comparable c : valueFields) {
      String fieldName = (String)c;
      byte[][] cfqual = fieldMap.get(fieldName);
      byte[] cf = cfqual[0];
      byte[] qualifier = cfqual[1];
      byte[] cell = row.getValue(cf, qualifier);
      result.add(cell);
    }

    return result;
  }

  @SuppressWarnings("unchecked")
  public void sink(TupleEntry tupleEntry, OutputCollector outputCollector)
      throws IOException {
    Tuple key = tupleEntry.selectTuple(keyField);

    byte[] keyBytes = Bytes.toBytes(key.getString(0));
    Put put = new Put(keyBytes);
    
    Tuple values = tupleEntry.selectTuple(valueFields);
    Iterator it = valueFields.iterator();
    for (int i = 0; it.hasNext(); i++) {
      String name = (String) it.next();
      byte[] cell = (byte[]) values.getObject(i);
      byte[][] cfqual = fieldMap.get(name);
      byte[] cf = cfqual[0];
      byte[] qualifier = cfqual[1];
      put.add(cf, qualifier, cell);
    }

    outputCollector.collect(null, put);
  }

  @SuppressWarnings("deprecation")
  public void sinkInit(Tap tap, JobConf conf) throws IOException {
    conf
        .setOutputFormat((Class<? extends OutputFormat>) TableOutputFormat.class);

    conf.setOutputKeyClass(ImmutableBytesWritable.class);
    conf.setOutputValueClass(Put.class);
  }

  @SuppressWarnings("deprecation")
  public void sourceInit(Tap tap, JobConf conf) throws IOException {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      scanner.write(new DataOutputStream(baos));
      conf.set(TableInputFormat.SCAN, baos.toString());
      conf.setInputFormat((Class<? extends InputFormat>) TableInputFormat.class);
    } catch (IOException ex) {
      throw new TapException(ex);
    }
  }

  private void validate() {
    if (keyField.size() != 1)
      throw new IllegalArgumentException("may only have one key field, found: "
          + keyField.print());
  }

  private void setSourceSink(Fields keyFields, Fields[] columnFields) {
    Fields allFields = Fields.join(keyFields, Fields.join(columnFields)); // prepend

    setSourceFields(allFields);
    setSinkFields(allFields);
  }

  /**
   * Method getFamilyNames returns the set of familyNames of this HBaseScheme
   * object.
   * 
   * @return the familyNames (type String[]) of this HBaseScheme object.
   */
  public byte[][] getFamilies() {
    Set<byte[]> families = new HashSet<byte[]>();
    for (byte[][] cfqual : fieldMap.values()) {
      families.add(cfqual[0]);
    }
    
    return families.toArray(new byte[][] {});
  }
}

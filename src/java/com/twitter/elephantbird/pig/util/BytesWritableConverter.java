package com.twitter.elephantbird.pig.util;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

/**
 * Supports conversion from BytesWritable to Pig bytearray, and from Pig bytearray to
 * {@link BytesWritable}.
 *
 * @author Andy Schlaikjer
 */
public class BytesWritableConverter extends AbstractWritableConverter<BytesWritable> {
  public BytesWritableConverter() {
    super(new BytesWritable());
  }

  @Override
  public ResourceFieldSchema getLoadSchema() throws IOException {
    ResourceFieldSchema schema = new ResourceFieldSchema();
    schema.setType(DataType.BYTEARRAY);
    return schema;
  }

  @Override
  protected BytesWritable toWritable(DataByteArray value) throws IOException {
    BytesWritable bw = (BytesWritable)writable;
    bw.set(value.get(), 0, value.size());
    return bw;
  }

}

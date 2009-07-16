/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fm.last.feathers.output;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.typedbytes.TypedBytesOutput;
import org.apache.hadoop.typedbytes.TypedBytesWritable;
import org.apache.hadoop.util.*;

public class IndexedPlainTypedBytes extends PlainTypedBytes {

  protected static class IndexingPairWriter
    extends PlainTypedBytes.PairWriter {

    private DataOutputStream indexOut;
    private TypedBytesOutput indexTbOut;
    
    private byte[] prevKey = new byte[0];
    private long written = 0;

    public IndexingPairWriter(DataOutputStream out, 
                              DataOutputStream indexOut) {
      super(out);
      this.indexOut = indexOut;
      indexTbOut = new TypedBytesOutput(indexOut);
    }

    @Override
    public synchronized void 
      write(TypedBytesWritable key, 
            TypedBytesWritable value) throws IOException {
      super.write(key, value);
      int keyLength = key.getSize();
      byte[] keyBytes = new byte[keyLength];
      System.arraycopy(key.get(), 0, keyBytes, 0, keyLength);
      if (!Arrays.equals(prevKey, keyBytes)) {
        indexOut.write(keyBytes);
        indexTbOut.writeLong(written);
      }
      prevKey = keyBytes;
      written += keyLength + value.getSize();
    }

    public synchronized void close(Reporter reporter) throws IOException {
      super.close(reporter);
      indexOut.close();
    }
  }

  @Override
  public RecordWriter<TypedBytesWritable, TypedBytesWritable> 
    getRecordWriter(FileSystem ignored,
                    JobConf job,
                    String name,
                    Progressable progress) throws IOException {
    Path file = FileOutputFormat.getTaskOutputPath(job, name);
    Path index = file.suffix("-index");
    FileSystem fs = file.getFileSystem(job);
    FSDataOutputStream fileOut = fs.create(file, progress);
    FSDataOutputStream indexOut = fs.create(index);
    return new IndexingPairWriter(fileOut, indexOut);
  }
}

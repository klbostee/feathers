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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.typedbytes.TypedBytesWritable;
import org.apache.hadoop.util.*;

public class PlainTypedBytes
  extends FileOutputFormat<TypedBytesWritable, TypedBytesWritable> {

  protected static class PairWriter
    implements RecordWriter<TypedBytesWritable, TypedBytesWritable> {

    private DataOutputStream out;

    public PairWriter(DataOutputStream out) {
      this.out = out;
    }

    public synchronized void 
      write(TypedBytesWritable key, 
            TypedBytesWritable value) throws IOException {
      out.write(key.get(), 0, key.getSize());
      out.write(value.get(), 0, value.getSize());
    }

    public synchronized void close(Reporter reporter) throws IOException {
      out.close();
    }
  }

  public RecordWriter<TypedBytesWritable, TypedBytesWritable> 
    getRecordWriter(FileSystem ignored,
                    JobConf job,
                    String name,
                    Progressable progress) throws IOException {
    Path file = FileOutputFormat.getTaskOutputPath(job, name);
    FileSystem fs = file.getFileSystem(job);
    FSDataOutputStream fileOut = fs.create(file, progress);
    return new PairWriter(fileOut);
  }
}

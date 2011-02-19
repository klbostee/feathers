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

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.util.Progressable;

import fm.last.feathers.output.RawFileOutputFormat;

/**
 * This class extends the MultipleOutputFormat, allowing to write the output data-
 * to different output files in raw file output format.
 */
public class MultipleRawFileOutputFormat <K,V>
  extends MultipleOutputFormat<K, V> {

  private RawFileOutputFormat<K,V> theRawFileOutputFormat = null;

  @Override
  protected RecordWriter<K, V> getBaseRecordWriter(FileSystem fs,
                                                   JobConf job,
                                                   String name,
                                                   Progressable arg3)
  throws IOException {
    if (this.theRawFileOutputFormat == null)
      this.theRawFileOutputFormat = new RawFileOutputFormat<K, V>();
    return this.theRawFileOutputFormat.getRecordWriter(fs, job, name, arg3);
  }

  @Override
  protected String generateFileNameForKeyValue(K key, V value, String name) {
    return new Path(key.toString(), name).toString();
  }

  @Override
  protected String getInputFileBasedOutputFileName(JobConf job, String name) {
    if (job.getBoolean("feathers.output.filename.strippart", false))
      name = new Path(name).getParent().toString();
    return super.getInputFileBasedOutputFileName(job, name);
  }
}

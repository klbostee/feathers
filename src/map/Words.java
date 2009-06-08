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

package fm.last.feathers.map;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.typedbytes.TypedBytesWritable;

public class Words extends MapReduceBase
  implements Mapper<LongWritable, Text,
                    TypedBytesWritable, TypedBytesWritable> {

  private final static Pattern pattern;
  private final static TypedBytesWritable one = new TypedBytesWritable();
  
  static {
    pattern = Pattern.compile(" ");
    one.setValue(1);    
  }
  
  private final TypedBytesWritable word = new TypedBytesWritable();

  public void map(LongWritable key, Text value, 
                  OutputCollector<TypedBytesWritable, TypedBytesWritable> output, 
                  Reporter reporter) throws IOException {
    for (String str : pattern.split(value.toString())) {
      word.setValue(str);
      output.collect(word, one);
    }
  }

}

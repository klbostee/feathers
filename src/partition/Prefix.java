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

package fm.last.feathers.partition;

import java.util.ArrayList;

import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.typedbytes.TypedBytesWritable;

public class Prefix 
  extends HashPartitioner<TypedBytesWritable, TypedBytesWritable> {

  private final TypedBytesWritable partKey = new TypedBytesWritable();

  public int getPartition(TypedBytesWritable key,
                          TypedBytesWritable value,
                          int numPartitions) {
    partKey.setValue(((ArrayList) key.getValue()).get(0));
    return super.getPartition(partKey, value, numPartitions);
  }

}

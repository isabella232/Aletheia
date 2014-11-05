/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.outbrain.aletheia.ui.data;

public class TierMetrics {

  private final String tier;
  private final long[] buckets;
  private final long[] counts;
  private final long[] lag;

  public TierMetrics(final String tier, final long[] buckets, final long[] counts, final long[] lag) {
    this.buckets = buckets;
    this.tier = tier;
    this.counts = counts;
    this.lag = lag;
  }

  public String getTier() {
    return tier;
  }

  public long[] getBuckets() {
    return buckets;
  }

  public long[] getCounts() {
    return counts;
  }

  public long[] getLag() {
    return lag;
  }

  public long getTotal() {
    long sum = 0;
    for (int i = 0; i < counts.length; i++)
      sum += counts[i];
    return sum;
  }

}

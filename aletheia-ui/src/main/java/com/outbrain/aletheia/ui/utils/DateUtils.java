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

package com.outbrain.aletheia.ui.utils;

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class DateUtils {
  public static DateTimeZone PST = DateTimeZone.forID("America/Los_Angeles");
  public static DateTimeFormatter MINUTE_FORMATTER = getDateTimeFormatter("YYYY-MM-dd-HH-mm");

  public static DateTimeFormatter getDateTimeFormatter(final String str) {
    return DateTimeFormat.forPattern(str).withZone(PST);
  }

  public static long getPartition(final long timeGranularityMs, final long timestamp) {
    return (timestamp / timeGranularityMs) * timeGranularityMs;
  }
}
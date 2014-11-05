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

import com.outbrain.aletheia.ui.utils.Clock;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.ArrayList;
import java.util.List;


/**
 * Check if we need to add or delete partitions on the database
 *
 */
public class PartitionMaintenanceTask implements Runnable {

  private static final Logger logger = Logger.getLogger(PartitionMaintenanceTask.class);
  private static final DateTimeFormatter PARTITION_FORMAT = DateTimeFormat.forPattern("YYYYMMdd");
  private final BreadcrumbDao db;
  private final int daysPrevious;
  private final int daysFuture;
  private final Clock clock;

  public PartitionMaintenanceTask(final BreadcrumbDao db, final int daysPrevious, final int daysFuture, final Clock clock) {
    this.db = db;
    this.daysPrevious = daysPrevious;
    this.daysFuture = daysFuture;
    this.clock = clock;
  }

  public void run() {
    logger.debug("Running partition maintenance check.");
    final long startTime = clock.time();

    try {
      final List<String> partitions = this.db.fetchPartitions();

      // find obsolete partitions and newest partition
      final List<String> obsolete = new ArrayList<String>();
      final DateTime now = new DateTime(clock.time());
      final DateTime oldestValidPartition = now.minus(Days.days(daysPrevious));
      DateTime latest = new DateTime(0);
      for(final String part: partitions) {
        final DateTime curr = timeFromPartition(part);
        if(curr.isAfter(latest))
          latest = curr;
        if(curr.isBefore(oldestValidPartition))
          obsolete.add(part);
      }

      // add new partitions
      // begin with the the latest existing partition, but not earlier than the oldest valid partition
      final DateTime start = roundToMidnight(new DateTime(Math.max(oldestValidPartition.getMillis(), Math.min(latest.getMillis(), now.getMillis()))));
      final DateTime end = roundToMidnight(now).plus(Days.days(daysFuture+1));
      for(DateTime curr = start; curr.isBefore(end); curr = curr.plus(Days.ONE)) {
        final String part = partitionFromTime(curr);
        if(!partitions.contains(part)) {
          logger.info("Creating partition " + part + " with timestamp " + curr.getMillis());
          db.addPartition(part, curr.getMillis());
        }
      }

      // delete old partitions
      for(final String part: obsolete) {
        logger.info("Deleting partition " + part);
        db.dropPartition(part);
      }

    } catch(final Exception e) {
      logger.error("Error in partition maintenence task", e);
    }

    logger.debug("Partition maintenance completed in " + (clock.time() - startTime) + " ms.");
  }

  /* The time for a partition p20111216 is exactly midnight on 2011-11-16 */
  public static DateTime timeFromPartition(final String name) {
    final DateTime time = PARTITION_FORMAT.parseDateTime(name.substring(1));
    return time.plusDays(1);
  }

  public static String partitionFromTime(final DateTime d) {
    return "p" + PARTITION_FORMAT.print(d.minusDays(1));
  }

  private DateTime roundToMidnight(final DateTime t) {
    return new DateTime(t.getYear(), t.getMonthOfYear(), t.getDayOfMonth(), 0, 0, 0, 0);
  }

}

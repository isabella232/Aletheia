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

package com.outbrain.aletheia.breadcrumbs.persistence;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import com.outbrain.aletheia.breadcrumbs.Breadcrumb;
import com.outbrain.aletheia.breadcrumbs.persistence.utils.JdbcUtils;
import com.outbrain.aletheia.breadcrumbs.persistence.utils.MysqlDataSource;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Instant;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class BreadcrumbSqlDao implements Closeable, BreadcrumbPersister {

  protected static final Logger logger = Logger.getLogger(BreadcrumbSqlDao.class);

  protected final MysqlDataSource ds;
  private BreadcrumbFieldToColumnNameMapper breadcrumbMapper;

  public BreadcrumbSqlDao(final MysqlDataSource mysqlDataSource, BreadcrumbFieldToColumnNameMapper breadcrumbMapper) {
    this.breadcrumbMapper = breadcrumbMapper;
    final GenericObjectPool.Config config = new GenericObjectPool.Config();
    config.whenExhaustedAction = GenericObjectPool.WHEN_EXHAUSTED_BLOCK;
    this.ds = mysqlDataSource;
  }

  public BreadcrumbFieldToColumnNameMapper getBreadcrumbMapper() {
    return breadcrumbMapper;
  }

  private String getInsertQueryString(final List<String> columns) {

    final String[] questionMarks = new String[columns.size()];
    Arrays.fill(questionMarks, "?");
    final String columnsString = Joiner.on(',').join(columns);
    final String questionMarksString = Joiner.on(',').join(questionMarks);

    return String.format("insert into %s (%s) values(%s);",
                         breadcrumbMapper.tableName(),
                         columnsString,
                         questionMarksString);
  }

  @Override
  public int persist(final List<Breadcrumb> breadcrumbs) {
    final Connection conn = JdbcUtils.connection(ds);
    PreparedStatement stmt = null;
    try {

      final String query = getInsertQueryString(breadcrumbMapper.allColumnNames());

      logger.debug(query);

      stmt = conn.prepareStatement(query);
      for (final Breadcrumb breadcrumb : breadcrumbs) {
        int i = 1;
        stmt.setString(i++, breadcrumb.getType());
        stmt.setString(i++, breadcrumb.getSource());
        stmt.setString(i++, breadcrumb.getDestination());
        stmt.setLong(i++, breadcrumb.getBucketStartTime().getMillis());
        stmt.setLong(i++, breadcrumb.getBucketEndTime().getMillis());
        stmt.setLong(i++, breadcrumb.getProcessingTimestamp().getMillis());
        stmt.setLong(i++, breadcrumb.getCount());
        stmt.setString(i++, breadcrumb.getTier());
        stmt.setString(i++, breadcrumb.getApplication());
        stmt.setString(i++, breadcrumb.getDatacenter());
        stmt.addBatch();
      }
      final int[] updated = stmt.executeBatch();
      int totalUpdates = 0;
      for (final int anUpdated : updated) {
        totalUpdates += anUpdated;
      }
      return totalUpdates;
      // what is update fails for some rows?
    } catch (final Exception e) {
      throw new RuntimeException(e);
    } finally {
      JdbcUtils.close(stmt);
      JdbcUtils.close(conn);
    }
  }

  /**
   * Aggregate data over the non-null parameters. For any of the parameters null indicates "for all".
   */
  public List<Breadcrumb> aggregateBreadcrumbs(final String topicFilter,
                                               final String tierFilter,
                                               final DateTime begin,
                                               final DateTime end,
                                               final List<String> select,
                                               final List<String> order) {

    // figure out what columns we are actually using--this is tricky but avoids us having to write N choose 2 such methods

    // select columns
    final List<String> selects = new ArrayList<>(select);

    final List<String> groupings = new ArrayList<>(selects);
    selects.add("SUM(" + breadcrumbMapper.count() + ") count");

    // where clauses
    final List<String> whereClauses = new ArrayList<>();
    if (topicFilter != null)
      whereClauses.add(breadcrumbMapper.datumType() + " = ?");
    if (tierFilter != null)
      whereClauses.add(breadcrumbMapper.tier() + " = ?");
    if (begin != null)
      whereClauses.add(breadcrumbMapper.bucketStart() + " >= ?");
    if (end != null)
      whereClauses.add(breadcrumbMapper.bucketStart() + " < ?");

    // order clauses
    final List<String> orderings = new ArrayList<>(order);

    // which sections to include?
    final boolean hasWhere = !(topicFilter == null && tierFilter == null && begin == null && end == null);
    final boolean hasGroup = groupings.size() > 0;
    final boolean hasOrder = order != null && order.size() > 0;

    // construct the query with all the columns indicated
    final String query = "SELECT " + Joiner.on(", ").join(selects) + " " +
            " FROM " + breadcrumbMapper.tableName() + " " +
            (hasWhere ? " WHERE " + Joiner.on(" AND ").join(whereClauses) : "") +
            (hasGroup ? " GROUP BY " + Joiner.on(", ").join(groupings) : "") +
            (hasOrder ? " ORDER BY " + Joiner.on(", ").join(orderings) : "");

    logger.debug(query);

    // execute the query
    final Connection conn = JdbcUtils.connection(ds);
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try {
      stmt = conn.prepareStatement(query);
      int i = 1;
      if (topicFilter != null)
        stmt.setString(i++, topicFilter);
      if (tierFilter != null)
        stmt.setString(i++, tierFilter);
      if (begin != null)
        stmt.setLong(i++, begin.getMillis());
      if (end != null)
        stmt.setLong(i++, end.getMillis());
      rs = stmt.executeQuery();
      final Set<String> selected = Sets.newHashSet(select);
      final List<Breadcrumb> results = new ArrayList<>();
      while (rs.next()) {

        final String datumType =
                selected.contains(breadcrumbMapper.datumType()) ? rs.getString(breadcrumbMapper.datumType()) : null;
        final String source =
                selected.contains(breadcrumbMapper.source()) ? rs.getString(breadcrumbMapper.source()) : null;
        final String destination =
                selected.contains(breadcrumbMapper.destination()) ? rs.getString(breadcrumbMapper.destination()) : null;
        final Long bucketStart =
                selected.contains(breadcrumbMapper.bucketStart()) ? rs.getLong(breadcrumbMapper.bucketStart()) : null;
        final Long bucketEnd =
                selected.contains(breadcrumbMapper.bucketEnd()) ? rs.getLong(breadcrumbMapper.bucketEnd()) : null;
        final Long processingTime =
                selected.contains(breadcrumbMapper.processingTimestamp()) ? rs.getLong(breadcrumbMapper.processingTimestamp()) : null;
        final long count = rs.getLong("count");
        final String tier =
                selected.contains(breadcrumbMapper.tier()) ? rs.getString(breadcrumbMapper.tier()) : null;
        final String application =
                selected.contains(breadcrumbMapper.application()) ? rs.getString(breadcrumbMapper.application()) : null;
        final String datacenter =
                selected.contains(breadcrumbMapper.datacenter()) ? rs.getString(breadcrumbMapper.datacenter()) : null;

        results.add(new Breadcrumb(datumType,
                                   source,
                                   destination,
                                   new Instant(bucketStart),
                                   new Instant(bucketEnd),
                                   new Instant(processingTime),
                                   count,
                                   datacenter,
                                   application,
                                   tier));
      }
      return results;
    } catch (final Exception e) {
      throw new RuntimeException(e);
    } finally {
      JdbcUtils.close(rs);
      JdbcUtils.close(stmt);
      JdbcUtils.close(conn);
    }
  }

  @Override
  public void close() {
    try {
      this.ds.close();
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }
}

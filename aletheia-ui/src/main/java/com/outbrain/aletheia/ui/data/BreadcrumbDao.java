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

import com.google.common.base.Joiner;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

public class BreadcrumbDao {

  private static final Logger logger = Logger.getLogger(BreadcrumbDao.class);

  private final MysqlDataSource ds;

  public BreadcrumbDao(final MysqlDataSource ds) {
    final GenericObjectPool.Config config = new GenericObjectPool.Config();
    config.whenExhaustedAction = GenericObjectPool.WHEN_EXHAUSTED_BLOCK;
    this.ds = ds;
  }

  public void close() {
    try {
      this.ds.close();
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  public Set<String> fetchTopics() {
    final Connection conn = JdbcUtils.connection(ds);
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try {
      final String query = "SELECT topic FROM kafka_topics";
      logger.debug(query);
      stmt = conn.prepareStatement(query);
      rs = stmt.executeQuery();
      final HashSet<String> s = new HashSet<String>();
      while (rs.next())
        s.add(rs.getString("topic"));

      return s;
    } catch (final Exception e) {
      throw new RuntimeException(e);
    } finally {
      JdbcUtils.close(rs);
      JdbcUtils.close(stmt);
      JdbcUtils.close(conn);
    }
  }

  public void insertTopic(final String topic) {
    final Connection conn = JdbcUtils.connection(ds);
    PreparedStatement stmt = null;
    try {
      final String query = "INSERT IGNORE INTO kafka_topics (topic, create_time) VALUES (?,?)";
      logger.debug(query);
      stmt = conn.prepareStatement(query);
      int i = 1;
      stmt.setString(i++, topic);
      stmt.setLong(i++, System.currentTimeMillis());
      stmt.executeUpdate();
    } catch (final Exception e) {
      throw new RuntimeException(e);
    } finally {
      JdbcUtils.close(stmt);
      JdbcUtils.close(conn);
    }
  }

  public boolean deleteTopic(final String topic) {
    final Connection conn = JdbcUtils.connection(ds);
    PreparedStatement stmt = null;
    try {
      final String query = "DELETE FROM kafka_topics where topic = ?";
      logger.debug(query);
      stmt = conn.prepareStatement(query);
      stmt.setString(1, topic);
      final int deleted = stmt.executeUpdate();
      if (deleted > 1)
        throw new RuntimeException("Deleted two topics with the same name: " + topic);
      return deleted == 1;
    } catch (final Exception e) {
      throw new RuntimeException(e);
    } finally {
      JdbcUtils.close(stmt);
      JdbcUtils.close(conn);
    }
  }

  public List<String> fetchPartitions() {
    final Connection conn = JdbcUtils.connection(ds);
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try {
      final String query = "SELECT partition_name FROM information_schema.partitions WHERE table_name='kafka_audit'";
      logger.debug(query);
      stmt = conn.prepareStatement(query);
      rs = stmt.executeQuery();
      final List<String> results = new ArrayList<String>();
      while (rs.next())
        results.add(rs.getString("partition_name"));
      return results;
    } catch (final Exception e) {
      throw new RuntimeException(e);
    } finally {
      JdbcUtils.close(rs);
      JdbcUtils.close(stmt);
      JdbcUtils.close(conn);
    }
  }

  public void dropPartition(final String partition) {
    final Connection conn = JdbcUtils.connection(ds);
    PreparedStatement stmt = null;
    try {
      final String query = "ALTER TABLE kafka_audit DROP PARTITION " + partition;
      logger.debug(query);
      stmt = conn.prepareStatement(query);
      stmt.executeUpdate();
    } catch (final Exception e) {
      throw new RuntimeException(e);
    } finally {
      JdbcUtils.close(stmt);
      JdbcUtils.close(conn);
    }
  }

  public void addPartition(final String partition, final long end) {
    final Connection conn = JdbcUtils.connection(ds);
    PreparedStatement stmt = null;
    try {
      final String query = "ALTER TABLE kafka_audit ADD PARTITION (PARTITION " + partition + " VALUES LESS THAN (?))";
      logger.debug(query);
      stmt = conn.prepareStatement(query);
      stmt.setLong(1, end);
      stmt.executeUpdate();
    } catch (final Exception e) {
      throw new RuntimeException(e);
    } finally {
      JdbcUtils.close(stmt);
      JdbcUtils.close(conn);
    }
  }

  public int insertAuditData(final List<AuditData> data) {
    final Connection conn = JdbcUtils.connection(ds);
    PreparedStatement stmt = null;
    try {
      // question: what are we ignoring here?
      final String query = "INSERT IGNORE kafka_audit " +
              "(guid, time, server, service, tier, topic, count, begin_timestamp, end_timestamp, update_time) " +
              "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
      logger.debug(query);
      stmt = conn.prepareStatement(query);
      for (final AuditData d : data) {
        int i = 1;
        stmt.setString(i++, d.getGuid());
        stmt.setLong(i++, d.getSendTime());
        stmt.setString(i++, d.getHost());
        stmt.setString(i++, d.getApplication());
        stmt.setString(i++, d.getTier());
        stmt.setString(i++, d.getTopic());
        stmt.setLong(i++, d.getCount());
        stmt.setLong(i++, d.getBeginTime());
        stmt.setLong(i++, d.getEndTime());
        stmt.setLong(i++, System.currentTimeMillis());
        stmt.addBatch();
      }
      final int[] updated = stmt.executeBatch();
      int totalUpdates = 0;
      for (int i = 0; i < updated.length; i++)
        totalUpdates += updated[i];
      return totalUpdates;
      // what is update fails for some rows?
    } catch (final Exception e) {
      throw new RuntimeException(e);
    } finally {
      JdbcUtils.close(stmt);
      JdbcUtils.close(conn);
    }
  }

  public int deleteAllAuditData() {
    final Connection conn = JdbcUtils.connection(ds);
    PreparedStatement stmt = null;
    try {
      stmt = conn.prepareStatement("delete from kafka_audit");
      return stmt.executeUpdate();
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
  public List<AuditData> aggregateAuditData(final String topicFilter,
                                            final String tierFilter,
                                            final DateTime begin,
                                            final DateTime end,
                                            final AuditColumn[] select,
                                            final AuditColumn[] order) {

    // figure out what columns we are actually using--this is tricky but avoids us having to write N choose 2 such methods

    // select columns
    final List<String> selects = new ArrayList<String>();
    if (select != null) {
      for (final AuditColumn c : select)
        selects.add(c.name);
    }
    final List<String> groupings = new ArrayList<String>(selects);
    selects.add("SUM(" + AuditColumn.COUNT.name + ") count");

    // where clauses
    final List<String> whereClauses = new ArrayList<String>();
    if (topicFilter != null)
      whereClauses.add(AuditColumn.TOPIC.name + " = ?");
    if (tierFilter != null)
      whereClauses.add(AuditColumn.TIER.name + " = ?");
    if (begin != null)
      whereClauses.add(AuditColumn.BEGIN.name + " >= ?");
    if (end != null)
      whereClauses.add(AuditColumn.BEGIN.name + " < ?");

    // order clauses
    final List<String> orderings = new ArrayList<String>();
    if (order != null) {
      for (final AuditColumn c : order)
        orderings.add(c.name);
    }

    // which sections to include?
    final boolean hasWhere = !(topicFilter == null && tierFilter == null && begin == null && end == null);
    final boolean hasGroup = groupings.size() > 0;
    final boolean hasOrder = order != null && order.length > 0;

    // construct the query with all the columns indicated
    final String query = "SELECT " + Joiner.on(", ").join(selects) + " " +
            " FROM dabd_breadcrumb_data " +
            (hasWhere ? " WHERE " + Joiner.on(" AND ").join(whereClauses) : "") +
            (hasGroup ? " GROUP BY " + Joiner.on(", ").join(groupings) : "") +
            (hasOrder ? " ORDER BY " + Joiner.on(", ").join(orderings) : "");

    logger.debug(query);

    // exectute the query
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
      final Set<AuditColumn> selected = EnumSet.copyOf(Arrays.asList(select));
      final List<AuditData> results = new ArrayList<AuditData>();
      while (rs.next()) {
        final Long time = selected.contains(AuditColumn.TIME) ? rs.getLong(AuditColumn.TIME.name) : null;
        final String topicValue = selected.contains(AuditColumn.TOPIC) ? rs.getString(AuditColumn.TOPIC.name) : null;
        final String tierValue = selected.contains(AuditColumn.TIER) ? rs.getString(AuditColumn.TIER.name) : null;
        final Long beginValue = selected.contains(AuditColumn.BEGIN) ? rs.getLong(AuditColumn.BEGIN.name) : null;
        final Long endValue = selected.contains(AuditColumn.END) ? rs.getLong(AuditColumn.END.name) : null;
        final long count = rs.getLong("count");
        results.add(new AuditData(time, beginValue, endValue, topicValue, tierValue, count));
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

  public enum AuditColumn {
    TOPIC("dabd_hit_type"),
    TIER("dabd_tier"),
    BEGIN("dabd_bucket_start_time"),
    END("dabd_bucket_end_time"),
    COUNT("dabd_bucket_hit_count"),
    TIME("dabd_bucket_processing_timestamp");
    //SRC("dabd_source"),
    //DST("dabd_destination");

    public final String name;

    private AuditColumn(final String name) {
      this.name = name;
    }
  }

}

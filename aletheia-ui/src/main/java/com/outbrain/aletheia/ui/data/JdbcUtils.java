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

import javax.sql.DataSource;
import java.sql.*;

/**
 * JDBC helpers
 */
public class JdbcUtils {

  public static Connection connection(final DataSource ds) {
    try {
      return ds.getConnection();
    } catch (final SQLException e) {
      throw new SqlException(e);
    }
  }

  public static void close(final Connection connection) {
    if (connection != null) {
      try {
        connection.close();
      } catch (final SQLException e) {
        e.printStackTrace();
      }
    }
  }

  public static void close(final Statement s) {
    if (s != null) {
      try {
        s.close();
      } catch (final SQLException e) {
        e.printStackTrace();
      }
    }
  }

  public static void close(final ResultSet r) {
    if (r != null) {
      try {
        r.close();
      } catch (final SQLException e) {
        e.printStackTrace();
      }
    }
  }

  public static PreparedStatement prepareStatement(final Connection conn, final String sql, final int batchSize) {
    final int rsOpts = ResultSet.CONCUR_READ_ONLY | ResultSet.FETCH_FORWARD;
    try {
      final PreparedStatement p;
      if (rsOpts >= 0)
        p = conn.prepareStatement(sql, rsOpts);
      else
        p = conn.prepareStatement(sql);

      p.setFetchSize(batchSize);
      return p;
    } catch (final SQLException e) {
      throw new SqlException(e);
    }
  }

}

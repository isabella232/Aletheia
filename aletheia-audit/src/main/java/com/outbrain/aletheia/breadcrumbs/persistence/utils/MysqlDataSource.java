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

package com.outbrain.aletheia.breadcrumbs.persistence.utils;


import org.apache.commons.dbcp2.BasicDataSource;

public class MysqlDataSource extends BasicDataSource {

  public MysqlDataSource(final String host,
                         final int port,
                         final String dbName,
                         final String user,
                         final String password,
                         final int numConnections) {
    super();
    final String url = "jdbc:mysql://" + (host + ":" + port + "/" + dbName);
    setDriverClassName(com.mysql.jdbc.Driver.class.getName());
    setUsername(user);
    setPassword(password);
    setUrl(url);
    setMaxTotal(numConnections);
    setValidationQuery("/* ping */ select 1");
    setTestOnBorrow(true);
  }

}

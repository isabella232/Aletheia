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

package com.outbrain.aletheia.ui.web;

import com.outbrain.aletheia.ui.data.BreadcrumbMetricsCalculator;
import com.outbrain.aletheia.ui.data.BreadcrumbDao;
import com.outbrain.aletheia.ui.data.MysqlDataSource;
import com.outbrain.aletheia.ui.utils.Props;
import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTimeZone;
import org.mortbay.http.HttpContext;
import org.mortbay.http.HttpServer;
import org.mortbay.http.SocketListener;
import org.mortbay.jetty.servlet.Default;
import org.mortbay.jetty.servlet.ServletHandler;

import java.io.File;
import java.util.TimeZone;

public class AletheiaUIApp {

  private static final Logger logger = Logger.getLogger(AletheiaUIApp.class);

  private static String staticDir = "static";
  private static final String basePath = AletheiaUIApp.class.getResource("/").getPath();


  public static void main(final String[] args) throws Exception {
//    if (args.length == 0) {
//      System.err.println("USAGE: java " + KafkaAuditApplication.class.getName()
//                                 + " config.properties [static_dir]");
//      System.exit(1);
//    }
//
//
//      Props props = new Props(new File(args[0]));

    final Props props = new Props(new File(FilenameUtils.concat(basePath, "audit-app.properties")));
    if (args.length == 2)
      staticDir = args[1];

    logProperties(props);

    new AletheiaUIApp(props);
  }

  public AletheiaUIApp(final Props props) throws Exception {

    // setup database
    final String host = props.getString("db.host");
    final int port = props.getInt("db.port");
    final String dbName = props.getString("db.name");
    final String user = props.getString("db.user");
    final String password = props.getString("db.password");
    final int numConnections = props.getInt("db.connections", 10);

    final MysqlDataSource dataSource = new MysqlDataSource(host, port, dbName, user, password, numConnections);
    final BreadcrumbDao db = new BreadcrumbDao(dataSource);
    final BreadcrumbMetricsCalculator service = new BreadcrumbMetricsCalculator(db);

    // timezone
    final String timezone = props.getString("default.timezone", "UTC");
    DateTimeZone.setDefault(DateTimeZone.forID(timezone));
    TimeZone.setDefault(TimeZone.getTimeZone(timezone));

    final long granularity = props.getLong("granularity.ms");

    // setup the http server
    final int portNumber = props.getInt("port");
    final int numHttpThreads = props.getInt("http.threads");
    final HttpServer server = new HttpServer();
    final SocketListener listener = new SocketListener();
    listener.setPort(portNumber);
    listener.setMinThreads(1);
    listener.setMaxThreads(numHttpThreads);
    server.addListener(listener);

    final HttpContext context = new HttpContext();
    context.setContextPath("/");
    server.addContext(context);

    // setup the servlets
    final ServletHandler servlets = new ServletHandler();
    context.addHandler(servlets);
    context.setResourceBase(FilenameUtils.concat(basePath, staticDir));
    servlets.addServlet("Index", "/*", Default.class.getName());
    servlets.addServlet("Query", "/topics/*", QueryServlet.class.getName());

    context.setAttribute(ContextKeys.AUDIT_SERVICE, service);
    context.setAttribute(ContextKeys.AUDIT_GRANULARITY, granularity);


    Runtime.getRuntime().addShutdownHook(new Thread() {

      public void run() {
        logger.info("Shutting down.");
        try {
          logger.info("Stopping http server.");
          server.stop();
          server.destroy();

          logger.info("Stopping scheduler threads");
        } catch (final Exception e) {
          logger.error("Error occurred in closing", e);
        }
      }
    });

    // start http server
    server.start();
    server.join();
  }

  private static void logProperties(final Props props) {
    final String nl = System.getProperty("line.separator");
    String s = "Kafka Audit properties: " + nl;
    for (final String key : props.keys())
      s += "\t" + key + " = " + props.getString(key) + nl;
    logger.info(s);
  }
}
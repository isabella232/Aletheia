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

import com.google.common.base.Preconditions;
import com.outbrain.aletheia.ui.data.BreadcrumbMetricsCalculator;
import com.outbrain.aletheia.ui.utils.DateUtils;
import com.outbrain.aletheia.ui.data.TierMetrics;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Map;


public class QueryServlet extends AbstractServlet {

  private static final long serialVersionUID = 1L;
  private static final DateTimeFormatter DATE_FORMAT = DateUtils.getDateTimeFormatter("YYYY-MM-dd HH:mm");
  private static final Logger logger = Logger.getLogger(QueryServlet.class);

  private BreadcrumbMetricsCalculator service;

  @Override
  public void init(final ServletConfig config) throws ServletException {
    super.init(config);

    Preconditions.checkNotNull(config.getServletContext().getAttribute(ContextKeys.AUDIT_SERVICE));

    this.service = (BreadcrumbMetricsCalculator) config.getServletContext().getAttribute(ContextKeys.AUDIT_SERVICE);
  }

  @Override
  protected void doGet(final HttpServletRequest request,
                       final HttpServletResponse response) throws ServletException, IOException {
    handleRequest(request, response);
  }

  @Override
  protected void doPost(final HttpServletRequest request,
                        final HttpServletResponse response) throws ServletException, IOException {
    handleRequest(request, response);
  }

  private void handleRequest(final HttpServletRequest request,
                             final HttpServletResponse response) throws ServletException, IOException {
    response.setHeader("content-type", "application/json");
    dontCacheMe(response);
    final PrintWriter writer = response.getWriter();
    final String path = request.getPathInfo();
    String[] pieces = new String[0];
    if (path != null)
      pieces = path.split("/");

    try {
      if (pieces.length == 0) {
        // SLevin: change back
        final ArrayList<String> topics = new ArrayList<String>(service.fetchTopics());
//        List<String> topics = Collections.singletonList("dwlt_listing.datum");
        Collections.sort(topics);
        writer.print(new JSONArray(topics));
      } else if (pieces.length >= 3 && pieces[2].equals("metrics")) {
        final String topic = pieces[1];
        long start = System.currentTimeMillis() - 24 * 60 * 60 * 1000;
        if (hasParam(request, "start"))
          start = Long.parseLong(getParam(request, "start"));
        long end = System.currentTimeMillis();
        if (hasParam(request, "end"))
          end = Long.parseLong(getParam(request, "end"));
        double complete = 0.999;
        if (hasParam(request, "complete"))
          complete = Double.parseDouble(getParam(request, "complete"));

        final Map<String, TierMetrics> metrics =
                this.service.fetchMetrics(topic,
                                          (long) getServletContext().getAttribute(ContextKeys.AUDIT_GRANULARITY),
                                          new DateTime(start),
                                          new DateTime(end),
                                          complete);

        // make json
        final JSONObject results = new JSONObject();
        for (final Map.Entry<String, TierMetrics> entry : metrics.entrySet()) {
          final TierMetrics metric = entry.getValue();
          final JSONObject tier = new JSONObject();
          tier.put("times", metric.getBuckets());
          tier.put("counts", metric.getCounts());
          tier.put("delay", metric.getLag());
          tier.put("total", metric.getTotal());
          results.put(entry.getKey(), tier);
        }
        writer.print(results);
      }
      writer.flush();
    } catch (final Exception e) {
      logger.error("Error in query", e);
      throw new IOException(e);
    } finally {
      writer.close();
    }
  }

  private void dontCacheMe(final HttpServletResponse response) {
    response.setHeader("Expires", "Tue, 25 May 1980 06:00:00 EST");
    response.setHeader("Last-Modified", new Date().toString());
    response.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0, post-check=0, pre-check=0");
    response.setHeader("Pragma", "no-cache");
  }

}
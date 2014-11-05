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

public class AuditData {

  private final String guid;
  private final Long send;
  private final Long begin;
  private final Long end;
  private final String host;
  private final String application;
  private final String datacenter;
  private final String tier;
  private final String topic;
  private final Long count;

  public AuditData(final String guid,
                   final Long send,
                   final Long begin,
                   final Long end,
                   final String host,
                   final String application,
                   final String datacenter,
                   final String tier,
                   final String topic,
                   final Long count) {
    this.guid = guid;
    this.send = send;
    this.host = host;
    this.application = application;
    this.datacenter = datacenter;
    this.tier = tier;
    this.topic = topic;
    this.count = count;
    this.begin = begin;
    this.end = end;
  }

  public AuditData(final Long time,
                   final Long begin,
                   final Long end,
                   final String topic,
                   final String tier,
                   final long count) {
    this(null, time, begin, end, null, null, null, tier, topic, count);
  }

  public String getGuid() {
    return guid;
  }

  public Long getSendTime() {
    return send;
  }

  public String getHost() {
    return host;
  }

  public String getApplication() {
    return application;
  }

  public String getDatacenter() {
    return datacenter;
  }

  public String getTier() {
    return tier;
  }

  public String getTopic() {
    return topic;
  }

  public Long getCount() {
    return count;
  }

  public Long getBeginTime() {
    return begin;
  }

  public Long getEndTime() {
    return end;
  }

  @Override
  public String toString() {
    return "AuditData(fixedGuid=" + guid + ", send=" + send
            + ", server=" + host + ", application=" + application + ", datacenter=" + datacenter + ", tier=" + tier + ", topic="
            + topic + ", count=" + count + ", beginTime=" + begin
            + ", endTime=" + end + ")";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
            + ((application == null) ? 0 : application.hashCode());
    result = prime * result + ((begin == null) ? 0 : begin.hashCode());
    result = prime * result + ((count == null) ? 0 : count.hashCode());
    result = prime * result
            + ((datacenter == null) ? 0 : datacenter.hashCode());
    result = prime * result + ((end == null) ? 0 : end.hashCode());
    result = prime * result + ((guid == null) ? 0 : guid.hashCode());
    result = prime * result + ((host == null) ? 0 : host.hashCode());
    result = prime * result + ((send == null) ? 0 : send.hashCode());
    result = prime * result + ((tier == null) ? 0 : tier.hashCode());
    result = prime * result + ((topic == null) ? 0 : topic.hashCode());
    return result;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    final AuditData a = (AuditData) obj;
    return getApplication().equals(a.getApplication()) &&
            getBeginTime().equals(a.getBeginTime()) &&
            getDatacenter().equals(a.getDatacenter()) &&
            getEndTime().equals(a.getEndTime()) &&
            getCount().equals(a.getCount()) &&
            getGuid().equals(a.getGuid()) &&
            getHost().equals(a.getHost()) &&
            getSendTime().equals(a.getSendTime()) &&
            getTier().equals(a.getTier()) &&
            getTopic().equals(a.getTopic());
  }
}
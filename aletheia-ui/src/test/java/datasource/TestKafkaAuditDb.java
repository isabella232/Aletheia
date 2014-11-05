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

package datasource;

import com.outbrain.aletheia.ui.data.AuditData;
import com.outbrain.aletheia.ui.data.BreadcrumbDao;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;


public class TestKafkaAuditDb extends AbstractDbTest {

  private Random random = new Random();

  @Test
  public void testCreateAndDeleteTopics() {
    String topicName = "test_topic + " + System.nanoTime();
    try {
      Set<String> topics = getDb().fetchTopics();
      getDb().insertTopic(topicName);
      Set<String> newTopics = getDb().fetchTopics();
      newTopics.removeAll(topics);
      assertEquals("There should be only one new topic.", 1, newTopics.size());
      assertEquals(topicName, newTopics.iterator().next());
    } finally {
      getDb().deleteTopic(topicName);
    }
  }

  @Test
  public void testCreateAndDeletePartitions() {
    List<String> partitions = getDb().fetchPartitions();
    String partition = "p20000102";
    try {
      getDb().addPartition(partition, Long.MAX_VALUE);
      List<String> updated = getDb().fetchPartitions();
      updated.removeAll(partitions);
      assertEquals("There should be only one new partitions.", 1, updated.size());
      assertEquals(partition, updated.get(0));
    } finally {
      getDb().dropPartition(partition);
    }
  }

  @Test
  public void testInsertAndAggregate() {
    getDb().deleteAllAuditData();
    assertEquals(0L, countAll());
    List<AuditData> values = new ArrayList<AuditData>();
    long total = 0;
    String[] tiers = new String[]{"T1", "T2"};
    String[] topics = new String[]{"A", "B"};
    final long countPerEvent = 10;
    final int eventsPerTopicTier = 10;
    for (String topic : topics) {
      for (String tier : tiers) {
        for (long i = 0; i < eventsPerTopicTier; i++) {
          // String topic, String tier, Long begin, Long end, long count
          byte[] guid = new byte[16];
          random.nextBytes(guid);
          AuditData data = new AuditData(new String(guid),
                                         System.currentTimeMillis(),
                                         i / 4,
                                         i,
                                         "",
                                         "",
                                         "",
                                         tier,
                                         topic,
                                         countPerEvent);
          values.add(data);
          total += countPerEvent;
        }
      }
    }
    getDb().insertAuditData(values);
    assertEquals(total, countAll());

    // filter by tier
    List<AuditData> counts = getDb().aggregateAuditData(null, "T1", null, null, null, null);
    assertEquals(total / 2L, (long) counts.get(0).getCount());

    // filter by tier and topic
    counts = getDb().aggregateAuditData("A", "T1", null, null, null, null);
    assertEquals(total / 4L, (long) counts.get(0).getCount());

    // group by tier, topic
    counts = getDb().aggregateAuditData(null,
                                        null,
                                        null,
                                        null,
                                        new BreadcrumbDao.AuditColumn[]{BreadcrumbDao.AuditColumn.TIER,
                                                BreadcrumbDao.AuditColumn.TOPIC},
                                        null);
    assertEquals(tiers.length * topics.length, counts.size());

    counts = getDb().aggregateAuditData(null, null, new DateTime(0), new DateTime(1), null, null);
    assertEquals(1, counts.size());
    assertEquals((Long) (tiers.length * topics.length * countPerEvent), counts.get(0).getCount());

    getDb().deleteAllAuditData();
  }

  private long countAll() {
    return getDb().aggregateAuditData(null, null, null, null, null, null).get(0).getCount();
  }


}

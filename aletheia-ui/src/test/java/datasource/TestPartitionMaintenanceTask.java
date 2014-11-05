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

import com.outbrain.aletheia.ui.data.PartitionMaintenanceTask;
import com.outbrain.aletheia.ui.utils.Clock;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestPartitionMaintenanceTask extends AbstractDbTest {
		
	@Test
	public void testPartitionMaintenance() {
		int daysPrevious = 10;
		int daysFuture = 2;
		MockClock clock = new MockClock();
		
		// delete all but a starting partition
		List<String> original = getDb().fetchPartitions();
		Collections.sort(original);
		for(String part: original.subList(1, original.size()))
			getDb().dropPartition(part);
		
		PartitionMaintenanceTask task = new PartitionMaintenanceTask(getDb(), daysPrevious, daysFuture, clock);
		task.run();
		List<String> after1stRun = getDb().fetchPartitions();
		Collections.sort(after1stRun);
		System.out.println(after1stRun);

		// check that we have all partitions
		checkPartitionCounts(after1stRun, daysFuture, daysPrevious, clock);

		// now it is tomorrow
		clock.addMilliseconds(24*60*60*1000);
		
		// run again
		task.run();
		List<String> after2ndRun = getDb().fetchPartitions();
		Collections.sort(after2ndRun);
		System.out.println(after2ndRun);
		
		// check that we have the right number of partitions
		checkPartitionCounts(after2ndRun, daysFuture, daysPrevious, clock);
		
		// check that we have deleted the first partition and added the last
		assertTrue("Expect that our new last item is new", !after1stRun.contains(after2ndRun.get(after2ndRun.size() - 1)));
		
		// clean up
		after2ndRun.remove(original.get(0));
		for(String p: after2ndRun)
			getDb().dropPartition(p);
	}
	
	private void checkPartitionCounts(List<String> partitions, int future, int past, Clock clock) {
		// check that we have all partitions
		int pastCount = 1;
		int futureCount = 0;
		DateTime prev = PartitionMaintenanceTask.timeFromPartition(partitions.get(0));
		DateTime now = new DateTime(clock.time());
		for(String part: partitions.subList(1, partitions.size())) {
			DateTime curr = PartitionMaintenanceTask.timeFromPartition(part);
			assertEquals("Expect successive partitions to occur on successive days.", prev.plusDays(1), curr);
			if(curr.isBefore(now))
				pastCount++;
			else
				futureCount++;
			prev = curr;
		}
		assertTrue("Expect that we have no more than past days worth of partitions (" + pastCount + " <= " + past + ")", pastCount <= past);
		assertTrue("Expect that we have at least future days worth of partitions (" + futureCount + " >= " + future + ")", futureCount >= future);
	}
	

}

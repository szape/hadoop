/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Test the tracker to calculate the resource utilization from cumulative
 * values.
 */
public class TestResourceTimeTracker {

  /**
   * Test a fake disk that provides number of bytes read.
   */
  @Test
  public void testResourceTimeTracker() {
    ResourceTimeTracker resourceTimeTracker = new ResourceTimeTracker();

    // Setting the starting value at 1s to 1000 bytes read
    resourceTimeTracker.updateElapsedResource(1000, 1 * 1000);
    // As it's the first value, it should be unavailable
    assertEquals(ResourceTimeTracker.UNAVAILABLE,
        resourceTimeTracker.getResourceTrackerUsagePerSec(), 0);

    // At 2s, if the value didn't increase, we should have 0 bytes/sec
    resourceTimeTracker.updateElapsedResource(1000, 2 * 1000);
    assertEquals(0, resourceTimeTracker.getResourceTrackerUsagePerSec(), 0);

    // At 3s, if we read 1000 bytes, we should have 1000 bytes/second
    resourceTimeTracker.updateElapsedResource(2000, 3 * 1000);
    assertEquals(1000, resourceTimeTracker.getResourceTrackerUsagePerSec(), 0);
  }

}

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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.math.BigInteger;

/**
 * Utility for sampling and computing resource usage.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ResourceTimeTracker {

  /** Resource or value is unavailable. */
  public static final int UNAVAILABLE = -1;

  /** Minimum interval between two samples in (ms). */
  private final long minimumTimeInterval;

  /** Resource used time since system is on (ms). */
  protected BigInteger cumulativeResource = BigInteger.ZERO;

  /** Resource used time read last time (ms). */
  protected BigInteger lastCumulativeResource = BigInteger.ZERO;

  /** Time stamp while reading the resource value (ms). */
  protected long sampleTime;
  protected long lastSampleTime;

  /** Calculate resource usage. */
  protected float resourceUsage;

  /** Conversion factor. */
  protected BigInteger convertionFactor;

  public ResourceTimeTracker() {
    this(1);
  }

  public ResourceTimeTracker(long convertionFactor) {
    this.convertionFactor = BigInteger.valueOf(convertionFactor);
    this.resourceUsage = UNAVAILABLE;
    this.sampleTime = UNAVAILABLE;
    this.lastSampleTime = UNAVAILABLE;
    this.minimumTimeInterval =  10 * convertionFactor;
  }

  /**
   * Return resource usage spent over the time since last update. Resource time
   * spent is based on consumed resources.
   * @return Return resource usage since last update per millisecond, {@link
   * ResourceTimeTracker#UNAVAILABLE} if there haven't been 2 updates more than
   * {@link ResourceTimeTracker#minimumTimeInterval} apart
   */
  public float getResourceTrackerUsage() {
    if (lastSampleTime == UNAVAILABLE || lastSampleTime > sampleTime) {
      // lastSampleTime > sampleTime may happen when the system time is changed
      lastSampleTime = sampleTime;
      lastCumulativeResource = cumulativeResource;
      return resourceUsage;
    }
    // When lastSampleTime is sufficiently old, update resourceUsage
    if (sampleTime > lastSampleTime + minimumTimeInterval) {
      float diffResource = cumulativeResource.subtract(
          lastCumulativeResource).floatValue();
      float diffTime = (float) (sampleTime - lastSampleTime);
      resourceUsage = diffResource / diffTime;

      // Take a sample of the current time and cumulative resource for the
      // use of the next calculation.
      lastSampleTime = sampleTime;
      lastCumulativeResource = cumulativeResource;
    }
    return resourceUsage;
  }

  /**
   * Return resource usage spent over the time since last update per second.
   * Resource time spent is based on consumed resources. {@link
   * ResourceTimeTracker#getResourceTrackerUsage()}
   * @return Resource usage per second.
   */
  public float getResourceTrackerUsagePerSec() {
    float usagePerMs = getResourceTrackerUsage();
    if (usagePerMs == UNAVAILABLE) {
      return UNAVAILABLE;
    }
    return usagePerMs * 1000F;
  }

  /**
   * Obtain the cumulative CPU time since the system is on.
   * @return cumulative CPU time in milliseconds
   */
  public long getCumulativeResource() {
    return cumulativeResource.longValue();
  }

  /**
   * Update the cumulative amount of resource consumed.
   * @param elapsedResource Updated consumed resources.
   * @param pSampleTime New sample time.
   */
  public void updateElapsedResource(long elapsedResource, long pSampleTime) {
    updateElapsedResource(BigInteger.valueOf(elapsedResource), pSampleTime);
  }
  /**
   * Update the cumulative amount of resource consumed.
   * @param elapsedResource Updated consumed resources.
   * @param sampleTime New sample time.
   */
  public void updateElapsedResource(
      BigInteger elapsedResource, long pSampleTime) {
    this.cumulativeResource = elapsedResource.multiply(convertionFactor);
    this.sampleTime = pSampleTime;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("SampleTime " + this.sampleTime);
    sb.append(" CummulativeResource " + this.cumulativeResource);
    sb.append(" LastSampleTime " + this.lastSampleTime);
    sb.append(" LastCummulativeResource " + this.lastCumulativeResource);
    sb.append(" ResourceUsage " + this.resourceUsage);
    sb.append(" JiffyLengthMillisec " + this.convertionFactor);
    return sb.toString();
  }
}

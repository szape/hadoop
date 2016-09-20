/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.api.records;

import java.io.Serializable;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.yarn.util.Records;

public abstract class PerformanceMetric implements Comparable<PerformanceMetric> {
  // TODO use targetHost instead of targetNodeId
  @Public
  @Stable
  public static PerformanceMetric newInstance(String key, double value) {
    PerformanceMetric performanceMetric = Records.newRecord(PerformanceMetric.class);
    performanceMetric.setKey(key);
    performanceMetric.setValue(value);
    return performanceMetric;
  }
  
  @Public
  @Stable
  public static class PerformanceMetricComparator implements
      java.util.Comparator<PerformanceMetric>, Serializable {
    
    private static final long serialVersionUID = 1L;
    
    @Override
    public int compare(PerformanceMetric pm1, PerformanceMetric pm2) {
      
      // Compare by key and value
      int ret = pm1.getKey().compareTo(pm2.getKey());
      if (ret == 0) {
        ret = new Double(pm1.getValue()).compareTo(pm2.getValue());
      }
      return ret;
    }
  }
  
  /**
   * Get the <code>Key</code> of the request.
   * @return <code>Key</code> of the request
   */
  @Public
  @Stable
  public abstract String getKey();
  
  /**
   * Set the <code>Key</code> of the request
   * @param key <code>Key</code> of the request
   */
  @Public
  @Stable
  public abstract void setKey(String key);
  
  /**
   * Get the <code>Value</code> of the request.
   * @return <code>Value</code> of the request
   */
  @Public
  @Stable
  public abstract double getValue();
  
  /**
   * Set the <code>Value</code> of the request
   * @param value <code>Value</code> of the request
   */
  @Public
  @Stable
  public abstract void setValue(double value);
  
  @Override
  public int hashCode() {
    final int prime = 2153;
    int result = 2459;
    String key = getKey();
    double value = getValue();
    
    result = prime * result + ((key == null) ? 0 : key.hashCode());
    result = prime * result + new Double(value).hashCode();
    return result;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    PerformanceMetric other = (PerformanceMetric) obj;
    String key = getKey();
    String otherKey = other.getKey();
    if (key == null) {
      if (otherKey != null)
        return false;
    } else if (!key.equals(otherKey))
      return false;
    double value = getValue();
    double otherValue = other.getValue();
    return value == otherValue;
  }
  
  @Override
  public int compareTo(PerformanceMetric other) {
    int keyComparison = this.getKey().compareTo(other.getKey());
    if (keyComparison == 0) {
      int valueComparison =
          new Double(this.getValue()).compareTo(other.getValue());
      return valueComparison;
    } else {
      return keyComparison;
    }
  }
}
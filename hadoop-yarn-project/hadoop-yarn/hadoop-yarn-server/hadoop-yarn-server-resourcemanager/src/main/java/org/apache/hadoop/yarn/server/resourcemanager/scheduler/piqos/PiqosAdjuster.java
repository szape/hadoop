package org.apache.hadoop.yarn.server.resourcemanager.scheduler.piqos;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;

public interface PiqosAdjuster {
  
  boolean up(ContainerId containerId, Resource capability);
  
  boolean down(ContainerId containerId, Resource capability);
  
  boolean left(ContainerId containerId);
  
  boolean right(ApplicationAttemptId applicationAttemptId, Resource capability);
}

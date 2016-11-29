package org.apache.hadoop.yarn.server.resourcemanager.scheduler.piqos;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoAppAttempt;

public abstract class AbstractPiqosScheduler extends
    AbstractYarnScheduler<FifoAppAttempt, FiCaSchedulerNode> {
  
  public AbstractPiqosScheduler(String name) {
    super(name);
  }
  
  public abstract boolean up(ContainerId containerId, Resource capability);
  
  public abstract boolean down(ContainerId containerId, Resource capability);
  
  public abstract boolean left(ContainerId containerId);
  
  public abstract boolean right(Resource capability);
}

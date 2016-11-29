package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.Records;

@Public
@Unstable
public abstract class LeftRequest {
  @Public
  @Unstable
  public static LeftRequest newInstance(ContainerId containerId) {
    LeftRequest request = Records.newRecord(LeftRequest.class);
    request.setContainerId(containerId);
    return request;
  }
  
  @Public
  @Unstable
  public abstract ContainerId getContainerId();
  
  @Public
  @Unstable
  public abstract void setContainerId(ContainerId containerId);
}
package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Records;

@Public
@Unstable
public abstract class UpRequest {
  @Public
  @Unstable
  public static UpRequest newInstance(ContainerId containerId, Resource capability) {
    UpRequest request = Records.newRecord(UpRequest.class);
    request.setContainerId(containerId);
    request.setCapability(capability);
    return request;
  }
  
  @Public
  @Unstable
  public abstract ContainerId getContainerId();
  
  @Public
  @Unstable
  public abstract void setContainerId(ContainerId containerId);
  
  @Public
  @Unstable
  public abstract Resource getCapability();
  
  @Public
  @Unstable
  public abstract void setCapability(Resource capability);
}

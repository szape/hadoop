package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Records;

@Public
@Unstable
public abstract class RightRequest {
  @Public
  @Unstable
  public static RightRequest newInstance(Resource capability) {
    RightRequest request = Records.newRecord(RightRequest.class);
    request.setCapability(capability);
    return request;
  }
  
  @Public
  @Unstable
  public abstract Resource getCapability();
  
  @Public
  @Unstable
  public abstract void setCapability(Resource capability);
}
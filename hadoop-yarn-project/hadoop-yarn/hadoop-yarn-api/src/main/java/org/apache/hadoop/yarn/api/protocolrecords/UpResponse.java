package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

public abstract class UpResponse {
  @Public
  @Unstable
  public static UpResponse newInstance(boolean success) {
    UpResponse response = Records.newRecord(UpResponse.class);
    response.setSuccess(success);
    return response;
  }
  
  @Public
  @Unstable
  public abstract boolean getSuccess();
  
  @Public
  @Unstable
  public abstract void setSuccess(boolean success);
}

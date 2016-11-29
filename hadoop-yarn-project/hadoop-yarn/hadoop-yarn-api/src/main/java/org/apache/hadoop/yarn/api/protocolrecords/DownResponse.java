package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

public abstract class DownResponse {
  @Public
  @Unstable
  public static DownResponse newInstance(boolean success) {
    DownResponse response = Records.newRecord(DownResponse.class);
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
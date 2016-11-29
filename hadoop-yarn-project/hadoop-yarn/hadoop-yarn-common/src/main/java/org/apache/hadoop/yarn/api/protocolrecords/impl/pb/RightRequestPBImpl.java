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

package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.protocolrecords.RightRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationAttemptIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourcePBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationAttemptIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.RightRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.RightRequestProtoOrBuilder;

@Public
@Unstable
public class RightRequestPBImpl extends RightRequest {
  RightRequestProto proto = RightRequestProto.getDefaultInstance();
  RightRequestProto.Builder builder = null;
  boolean viaProto = false;
  
  private ApplicationAttemptId applicationAttemptId = null;
  private Resource capability = null;
  
  public RightRequestPBImpl() {
    builder = RightRequestProto.newBuilder();
  }
  
  public RightRequestPBImpl(RightRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public RightRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }
  
  @Override
  public int hashCode() {
    return getProto().hashCode();
  }
  
  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }
  
  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }
  
  private void mergeLocalToBuilder() {
    if (applicationAttemptId != null) {
      builder.setApplicationAttemptId(convertToProtoFormat(this.applicationAttemptId));
    }
    if (capability != null) {
      builder.setCapability(convertToProtoFormat(this.capability));
    }
  }
  
  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }
  
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = RightRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
  
  @Override
  public ApplicationAttemptId getApplicationAttemptId() {
    if (this.applicationAttemptId != null) {
      return this.applicationAttemptId;
    }
    RightRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasApplicationAttemptId()) {
      return null;
    }
    this.applicationAttemptId = convertFromProtoFormat(p.getApplicationAttemptId());
    return this.applicationAttemptId;
  }
  
  @Override
  public void setApplicationAttemptId(ApplicationAttemptId applicationAttemptId) {
    maybeInitBuilder();
    if (applicationAttemptId == null) {
      builder.clearApplicationAttemptId();
    }
    this.applicationAttemptId = applicationAttemptId;
  }
  
  @Override
  public Resource getCapability() {
    if (this.capability != null) {
      return this.capability;
    }
    RightRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasCapability()) {
      return null;
    }
    this.capability = convertFromProtoFormat(p.getCapability());
    return this.capability;
  }
  
  @Override
  public void setCapability(Resource capability) {
    maybeInitBuilder();
    if (capability == null) {
      builder.clearCapability();
    }
    this.capability = capability;
  }
  
  private ApplicationAttemptIdPBImpl convertFromProtoFormat(ApplicationAttemptIdProto p) {
    return new ApplicationAttemptIdPBImpl(p);
  }
  
  private ApplicationAttemptIdProto convertToProtoFormat(ApplicationAttemptId t) {
    return ((ApplicationAttemptIdPBImpl) t).getProto();
  }
  
  private ResourcePBImpl convertFromProtoFormat(ResourceProto p) {
    return new ResourcePBImpl(p);
  }
  
  private ResourceProto convertToProtoFormat(Resource t) {
    return ((ResourcePBImpl) t).getProto();
  }
}

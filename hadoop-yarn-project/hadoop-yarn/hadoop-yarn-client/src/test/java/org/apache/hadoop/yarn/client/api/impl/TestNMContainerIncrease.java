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

package org.apache.hadoop.yarn.client.api.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.UpdateContainerError;
import org.apache.hadoop.yarn.api.records.UpdatedContainer;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.NMTokenCache;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.util.Records;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class TestNMContainerIncrease {
  private final static int DEFAULT_ITERATION = 3;
  
  Configuration conf = null;
  MiniYARNCluster yarnCluster = null;
  YarnClientImpl yarnClient = null;
  
  int nodeCount = 3;
  List<NodeReport> nodeReports = null;
  static NodeId node1;
  static NodeId node2;
  static NodeId node3;
  static String rack1;
  static String rack2;
  static String rack3;
  ApplicationAttemptId attemptId = null;
  NMTokenCache nmTokenCache = null;
  static Priority priority1;
  static Priority priority2;
  static Priority priority3;
  static Resource capability1;
  static Resource capability2;
  static Resource capability3;
  
  @Before
  public void setup() throws YarnException, IOException {
    // start minicluster
    conf = new YarnConfiguration();
    conf.setLong(
        YarnConfiguration.RM_AMRM_TOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS,
        13);
    conf.setLong(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS, 4000);
    conf.setInt(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS, 100);
    // set the minimum allocation so that resource decrease can go under 1024
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 512);
    conf.setLong(YarnConfiguration.NM_LOG_RETAIN_SECONDS, 1);
    conf.set(YarnConfiguration.RM_SCHEDULER, "org.apache.hadoop.yarn.server.resourcemanager" +
        ".scheduler.capacity.CapacityScheduler");
    yarnCluster =
        new MiniYARNCluster(TestAMRMClient.class.getName(), nodeCount, 1, 1);
    yarnCluster.init(conf);
    yarnCluster.start();
    assertNotNull(yarnCluster);
    assertEquals(STATE.STARTED, yarnCluster.getServiceState());
    
    // start rm client
    yarnClient = (YarnClientImpl) YarnClient.createYarnClient();
    yarnClient.init(conf);
    yarnClient.start();
    assertNotNull(yarnClient);
    assertEquals(STATE.STARTED, yarnClient.getServiceState());
    
    priority1 = Priority.newInstance(1);
    priority2 = Priority.newInstance(2);
    priority3 = Priority.newInstance(3);
    
    capability1 = Resource.newInstance(1024, 1);
    capability2 = Resource.newInstance(1536, 1);
    capability3 = Resource.newInstance(1536, 1);
    
    // get node info
    nodeReports = yarnClient.getNodeReports(NodeState.RUNNING);
    node1 = nodeReports.get(0).getNodeId();
    node2 = nodeReports.get(1).getNodeId();
    node3 = nodeReports.get(2).getNodeId();
    rack1 = nodeReports.get(0).getRackName();
    rack2 = nodeReports.get(1).getRackName();
    rack3 = nodeReports.get(2).getRackName();
    
    // submit new app
    ApplicationSubmissionContext appContext =
        yarnClient.createApplication().getApplicationSubmissionContext();
    ApplicationId appId = appContext.getApplicationId();
    // set the application name
    appContext.setApplicationName("Test");
    // Set the priority for the application master
    Priority pri = Priority.newInstance(0);
    appContext.setPriority(pri);
    // Set the queue to which this application is to be submitted in the RM
    appContext.setQueue("default");
    // Set up the container launch context for the application master
    ContainerLaunchContext amContainer = Records
        .newRecord(ContainerLaunchContext.class);
    appContext.setAMContainerSpec(amContainer);
    // unmanaged AM
    appContext.setUnmanagedAM(true);
    // Create the request to send to the applications manager
    SubmitApplicationRequest appRequest = Records
        .newRecord(SubmitApplicationRequest.class);
    appRequest.setApplicationSubmissionContext(appContext);
    // Submit the application to the applications manager
    yarnClient.submitApplication(appContext);
    
    // wait for app to start
    int iterationsLeft = 30;
    RMAppAttempt appAttempt = null;
    while (iterationsLeft > 0) {
      ApplicationReport appReport = yarnClient.getApplicationReport(appId);
      if (appReport.getYarnApplicationState() ==
          YarnApplicationState.ACCEPTED) {
        attemptId = appReport.getCurrentApplicationAttemptId();
        appAttempt =
            yarnCluster.getResourceManager().getRMContext().getRMApps()
                .get(attemptId.getApplicationId()).getCurrentAppAttempt();
        while (true) {
          if (appAttempt.getAppAttemptState() == RMAppAttemptState.LAUNCHED) {
            break;
          }
        }
        break;
      }
      sleep(1000);
      --iterationsLeft;
    }
    if (iterationsLeft == 0) {
      fail("Application hasn't bee started");
    }
    
    // Just dig into the ResourceManager and get the AMRMToken just for the sake
    // of testing.
    UserGroupInformation.setLoginUser(UserGroupInformation
        .createRemoteUser(UserGroupInformation.getCurrentUser().getUserName()));
    UserGroupInformation.getCurrentUser().addToken(appAttempt.getAMRMToken());
  }
  
  @After
  public void tearDown() {
    yarnClient.stop();
    yarnCluster.stop();
  }
  
  @Test(timeout = 200000)
  public void testNMContainerMove() throws YarnException, IOException {
    
    AMRMClientImpl<ContainerRequest> rmClient = null;
    NMClientImpl nmClient = null;
    try {
      //creating an instance NMTokenCase
      nmTokenCache = new NMTokenCache();
      // start am rm client
      rmClient = (AMRMClientImpl<ContainerRequest>) AMRMClient.createAMRMClient();
      rmClient.setNMTokenCache(nmTokenCache);
      rmClient.init(conf);
      rmClient.start();
      // start am nm client
      nmClient = (NMClientImpl) NMClient.createNMClient();
      nmClient.setNMTokenCache(rmClient.getNMTokenCache());
      nmClient.init(conf);
      nmClient.start();
      
      addContainerRequest(rmClient, capability1, null, null, priority1);
      List<Container> allocatedContainers = getAllocatedContainers(rmClient, DEFAULT_ITERATION);
      System.out.println("### Allocated containers: " + allocatedContainers);
      assertEquals(1, allocatedContainers.size());
      runContainers(nmClient, allocatedContainers);
      
      Thread.sleep(1000);
      rmClient.requestContainerResourceChange(allocatedContainers.get(0), capability2);
      List<Container> updatedContainers = getUpdatedContainers(rmClient, DEFAULT_ITERATION);
      System.out.println("### Updated Containers: " + updatedContainers);
      System.out.println("### Updated container status before increase: " +
          getContainerStatus(nmClient, allocatedContainers.get(0)));
      nmClient.increaseContainerResource(updatedContainers.get(0));
      Thread.sleep(1000);
      System.out.println("### Updated container status after increase: " +
          getContainerStatus(nmClient, allocatedContainers.get(0)));
      
      rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
          null, null);
      // stop the running containers on close
      nmClient.cleanupRunningContainersOnStop(true);
      nmClient.stop();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  private void addContainerRequest(AMRMClient<ContainerRequest> rmClient, Resource capability,
      String[] nodes, String[] racks, Priority priority) {
    ContainerRequest request =
        new ContainerRequest(capability, nodes, racks, priority);
    rmClient.addContainerRequest(request);
  }
  
  private void runContainers(NMClient nmClient, List<Container> containers) {
    for (Container c : containers) {
      runContainer(nmClient, c);
    }
  }
  
  private void runContainer(NMClient nmClient, Container container) {
    try {
      Credentials cred = new Credentials();
      DataOutputBuffer dob = new DataOutputBuffer();
      cred.writeTokenStorageToStream(dob);
      ByteBuffer securityTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
      ContainerLaunchContext clc = Records.newRecord(ContainerLaunchContext.class);
      clc.setCommands(Collections.singletonList("sleep 3s"));
      clc.setTokens(securityTokens);
      nmClient.startContainer(container, clc);
    } catch (IOException | YarnException e) {
      throw (AssertionError) new AssertionError("Exception is not expected: " + e).initCause(e);
    }
  }
  
  private ContainerStatus getContainerStatus(NMClient nmClient, Container container) {
    try {
      ContainerStatus status = nmClient.getContainerStatus(
          container.getId(), container.getNodeId());
      return status;
    } catch (IOException | YarnException e) {
      throw (AssertionError) new AssertionError("Exception is not expected: " + e).initCause(e);
    }
  }
  
  private List<Container> getAllocatedContainers(
      AMRMClientImpl<ContainerRequest> amClient, int iterationsLeft)
      throws YarnException, IOException {
    List<Container> allocatedContainers = new ArrayList<Container>();
    while (iterationsLeft-- > 0) {
      AllocateResponse allocResponse = amClient.allocate(0.1f);
      allocatedContainers.addAll(allocResponse.getAllocatedContainers());
      for (UpdatedContainer uc : allocResponse.getUpdatedContainers()) {
        allocatedContainers.add(uc.getContainer());
      }
      if (allocatedContainers.isEmpty()) {
        // sleep to let NM's heartbeat to RM and trigger allocations
        sleep(100);
      }
    }
    return allocatedContainers;
  }
  
  private List<Container> getUpdatedContainers(
      AMRMClientImpl<ContainerRequest> amClient, int iterationsLeft)
      throws YarnException, IOException {
    List<Container> updatedContainers = new ArrayList<Container>();
    while (iterationsLeft-- > 0) {
      AllocateResponse allocResponse = amClient.allocate(0.1f);
      for (UpdatedContainer uc : allocResponse.getUpdatedContainers()) {
        updatedContainers.add(uc.getContainer());
      }
      for(UpdateContainerError error : allocResponse.getUpdateErrors()) {
        System.out.println("### Error in container increase: " + error.getReason());
      }
      if (updatedContainers.isEmpty()) {
        // sleep to let NM's heartbeat to RM and trigger allocations
        sleep(100);
      }
    }
    return updatedContainers;
  }
  
  private void sleep(int sleepTime) {
    try {
      Thread.sleep(sleepTime);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
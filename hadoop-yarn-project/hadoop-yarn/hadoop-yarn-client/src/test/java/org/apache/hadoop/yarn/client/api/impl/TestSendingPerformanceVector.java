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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.PerformanceMetric;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSendingPerformanceVector {
  private static final Log LOG = LogFactory.getLog(TestRMContainerMove.class);
  
  static Configuration conf = null;
  static MiniYARNCluster yarnCluster = null;
  static YarnClient yarnClient = null;
  static List<NodeReport> nodeReports = null;
  static ApplicationAttemptId attemptId = null;
  static int nodeCount = 2;
  
  static final int rolling_interval_sec = 13;
  static final long am_expire_ms = 4000;
  
  static Priority priority1;
  static Priority priority2;
  static Resource capability1;
  static Resource capability2;
  static String node1;
  static String node2;
  static String[] nodes1;
  static String[] nodes2;
  static String[] nodes;
  
  @BeforeClass
  public static void setup() throws Exception {
    // start minicluster
    conf = new YarnConfiguration();
    conf.setLong(
        YarnConfiguration.RM_AMRM_TOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS,
        rolling_interval_sec);
    conf.setLong(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS, am_expire_ms);
    conf.setInt(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS, 100);
    // set the minimum allocation so that resource decrease can go under 1024
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 512);
    conf.setLong(YarnConfiguration.NM_LOG_RETAIN_SECONDS, 1);
    conf.set(YarnConfiguration.RM_SCHEDULER, "org.apache.hadoop.yarn.server.resourcemanager" +
        ".scheduler.fifo.FifoScheduler");
    yarnCluster = new MiniYARNCluster(TestAMRMClient.class.getName(), nodeCount, 1, 1);
    yarnCluster.init(conf);
    yarnCluster.start();
    
    // start rm client
    yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);
    yarnClient.start();
    
    priority1 = Priority.newInstance(1);
    priority2 = Priority.newInstance(2);
    
    capability1 = Resource.newInstance(512, 1);
    capability2 = Resource.newInstance(1024, 1);
    
    // get node info
    nodeReports = yarnClient.getNodeReports(NodeState.RUNNING);
    
    for (int i = 0; i < nodeReports.size(); i++) {
      System.out.println(nodeReports.get(i).getCapability());
    }
    
    node1 = nodeReports.get(0).getNodeId().getHost();
    node2 = nodeReports.get(1).getNodeId().getHost();
    nodes1 = new String[]{node1};
    nodes2 = new String[]{node2};
    nodes = new String[]{node1, node2};
  }
  
  @Before
  public void startApp() throws Exception {
    // submit new app
    ApplicationSubmissionContext appContext =
        yarnClient.createApplication().getApplicationSubmissionContext();
    ApplicationId appId = appContext.getApplicationId();
    // set the application name
    appContext.setApplicationName("Test");
    // Set the priority for the application master
    Priority pri = Records.newRecord(Priority.class);
    pri.setPriority(0);
    appContext.setPriority(pri);
    // Set the queue to which this application is to be submitted in the RM
    appContext.setQueue("default");
    // Set up the container launch context for the application master
    ContainerLaunchContext amContainer =
        BuilderUtils.newContainerLaunchContext(
            Collections.<String, LocalResource>emptyMap(),
            new HashMap<String, String>(), Arrays.asList("sleep", "100"),
            new HashMap<String, ByteBuffer>(), null,
            new HashMap<ApplicationAccessType, String>());
    appContext.setAMContainerSpec(amContainer);
    appContext.setResource(Resource.newInstance(1024, 1));
    // Create the request to send to the applications manager
    SubmitApplicationRequest appRequest = Records
        .newRecord(SubmitApplicationRequest.class);
    appRequest.setApplicationSubmissionContext(appContext);
    // Submit the application to the applications manager
    yarnClient.submitApplication(appContext);
    
    // wait for app to start
    RMAppAttempt appAttempt;
    while (true) {
      ApplicationReport appReport = yarnClient.getApplicationReport(appId);
      if (appReport.getYarnApplicationState() == YarnApplicationState.ACCEPTED) {
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
    }
    
    // Just dig into the ResourceManager and get the AMRMToken just for the sake
    // of testing.
    UserGroupInformation.setLoginUser(UserGroupInformation
        .createRemoteUser(UserGroupInformation.getCurrentUser().getUserName()));
    
    // emulate RM setup of AMRM token in credentials by adding the token
    // *before* setting the token service
    UserGroupInformation.getCurrentUser().addToken(appAttempt.getAMRMToken());
    appAttempt.getAMRMToken().setService(ClientRMProxy.getAMRMTokenService(conf));
  }
  
  @After
  public void cancelApp() throws YarnException, IOException {
    yarnClient.killApplication(attemptId.getApplicationId());
    attemptId = null;
  }
  
  @AfterClass
  public static void tearDown() {
    if (yarnClient != null && yarnClient.getServiceState() == Service.STATE.STARTED) {
      yarnClient.stop();
    }
    if (yarnCluster != null && yarnCluster.getServiceState() == Service.STATE.STARTED) {
      yarnCluster.stop();
    }
  }
  
  @Test(timeout = 60000)
  public void testSendingPerformanceVectorToRM() throws YarnException, IOException {
    AMRMClientImpl<AMRMClient.ContainerRequest> amClient = null;
    try {
      // start am rm client
      amClient =
          (AMRMClientImpl<AMRMClient.ContainerRequest>) AMRMClient
              .<AMRMClient.ContainerRequest>createAMRMClient();
      amClient.init(conf);
      amClient.start();
      amClient.registerApplicationMaster("Host", 10000, "");
      
      PerformanceMetric performanceMetric1 = PerformanceMetric.newInstance("latency", 512d);
      PerformanceMetric performanceMetric2 = PerformanceMetric.newInstance("throughput", 2000d);
      
      amClient.allocate(0.1f, Collections.singletonList(performanceMetric1));
      
      AMRMClient.ContainerRequest storedContainer1 =
          new AMRMClient.ContainerRequest(capability1, nodes1, null, priority1);
      AMRMClient.ContainerRequest storedContainer2 =
          new AMRMClient.ContainerRequest(capability2, nodes2, null, priority2);
      
      amClient.allocate(0.1f, Collections.singletonList(performanceMetric2));
      
      amClient.addContainerRequest(storedContainer1);
      amClient.addContainerRequest(storedContainer2);
      assertEquals(amClient.ask.size(), 6);
      assertEquals(amClient.moveAsk.size(), 0);
      assertEquals(amClient.pendingMoveAsk.size(), 0);
      
      amClient.allocate(0.1f, Arrays.asList(performanceMetric1, performanceMetric2));
      
      assertFalse(((FifoScheduler) yarnCluster.getResourceManager().getResourceScheduler())
          .performanceMetrics.isEmpty());
      
      amClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
          null, null);
    } finally {
      if (amClient != null && amClient.getServiceState() == Service.STATE.STARTED) {
        amClient.stop();
      }
    }
  }
}
/**
 * Copyright 2012-2014 Cask, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.tigon.sql.manager;

import com.continuuity.http.AbstractHttpHandler;
import com.continuuity.http.HttpResponder;
import co.cask.tigon.sql.internal.HealthInspector;
import co.cask.tigon.sql.internal.MetricsRecorder;
import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;


/**
 * HubHttpHandler
 * Handler used by the internal DiscoveryServer. Serves various REST end points as described in the GSHub documentation
 *
 * This HTTP server is only used by internal SQL Compiler processes. They use this service to communicate with other
 * internal processes, by getting and setting the state.
 *
 * SQL Compiler has 3 types of processes:
 * 1) RTS
 * 2) HFTA
 * 3) GSEXIT
 *
 * These processes use the REST endpoints to communicate with other processes. The expected order of requests is as
 * follows:
 *
 * 1) /announce-instance              from RTS process
 * 2) /announce-initialized-instance  from RTS process
 * 3) /discover-instance              from HFTA processes
 * 4) /discover-initialized-instance  from GSEXIT processes
 * 5) /discover-source                from GSEXIT processes
 * 6) /discover-sink                  from GSEXIT processes
 * 7) /announce-stream-subscription   from GSEXIT processes
 * 8) /discover-start-processing      from RTS processes
 * 9) /announce-fta-instance          from each FTA instance
 * 10) /log-metrics                    from each FTA instance
 *
 * Most of these processes poll on specific end points until  a 200 OK response is returned. If a request cannot be
 * served at a specific time because the state is not set or the specific information is not available then the response
 * is an error code like 400.
 */
@Path("/v1")
public class HubHttpHandler extends AbstractHttpHandler {
  private static final Gson gsonObject = new Gson();
  private final AtomicReference<HubDataStore> hubDataStoreReference;
  private final AtomicInteger gsexitCount;
  private final ProcessReadyListener listener;
  private final HealthInspector healthInspector;
  private final MetricsRecorder metricsRecorder;
  private final Map<String, String> processNameMap;

  public HubHttpHandler(HubDataStore ds, HealthInspector inspector, MetricsRecorder recorder) {
    hubDataStoreReference = new AtomicReference<HubDataStore>(ds);
    gsexitCount = new AtomicInteger(ds.getHubDataSinks().size());
    listener = new ProcessReadyListener();
    healthInspector = inspector;
    metricsRecorder = recorder;
    processNameMap = Maps.newHashMap();
  }

  private String getStringContent(HttpRequest request) {
    return request.getContent().toString(Charsets.UTF_8);
  }

  private class JSONRequestData {
    private JsonObject requestData;

    JSONRequestData(HttpRequest request) {
      try {
        requestData = HubHttpHandler.gsonObject.fromJson(getStringContent(request), JsonObject.class);
      } catch (Exception e) {
        throw new RuntimeException("Cannot parse JSON data from the HTTP response");
      }
    }

    String getInstanceName() {
      return requestData.get("name").getAsString();
    }

    String getProcessID() {
      return requestData.get("ftaid").toString();
    }

    String getProcessName() {
      return requestData.get("fta_name").getAsString();
    }

    String getIP() {
      return requestData.get("ip").getAsString();
    }

    Integer getPort() {
      return requestData.get("port").getAsInt();
    }

    JsonObject getMetrics() {
      return requestData.get("metrics").getAsJsonObject();
    }
  }

  private JsonObject createResponse(String ip, int port) {
    JsonObject res = new JsonObject();
    res.addProperty("ip", ip);
    res.addProperty("port", port);
    return res;
  }

  void setHubAddress(InetSocketAddress address) {
    HubDataStore hds;
    HubDataStore current;
    do {
      current = hubDataStoreReference.get();
      hds = new HubDataStore.Builder(current).setHubAddress(address).build();
    } while (!hubDataStoreReference.compareAndSet(current, hds));
  }

  @Path("/announce-instance")
  @POST
  public void announceInstance(HttpRequest request, HttpResponder responder) {
    JSONRequestData requestData = new JSONRequestData(request);
    HubDataStore hds;
    HubDataStore current;
    do {
      current = hubDataStoreReference.get();
      hds = new HubDataStore.Builder(current).setInstanceName(requestData.getInstanceName())
        .setClearingHouseAddress(new InetSocketAddress(requestData.getIP(), requestData.getPort())).build();
    } while (!hubDataStoreReference.compareAndSet(current, hds));
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @Path("/announce-initialized-instance")
  @POST
  public void announceInitializedInstance(HttpRequest request, HttpResponder responder) {
    JSONRequestData requestData = new JSONRequestData(request);
    if (hubDataStoreReference.get().getInstanceName().equals(requestData.getInstanceName())) {
      HubDataStore hds;
      HubDataStore current;
      do {
        current = hubDataStoreReference.get();
        hds = new HubDataStore.Builder(current).initialize().build();
      } while (!hubDataStoreReference.compareAndSet(current, hds));
      responder.sendStatus(HttpResponseStatus.OK);
      return;
    }
    responder.sendError(HttpResponseStatus.BAD_REQUEST, "Instance name does not match");
  }

  @Path("/announce-stream-subscription")
  @POST
  public void announceStreamSubscription(HttpRequest request, HttpResponder responder) {
    JSONRequestData requestData = new JSONRequestData(request);
    int val = 1;
    if (!hubDataStoreReference.get().getInstanceName().equals(requestData.getInstanceName())) {
      responder.sendError(HttpResponseStatus.BAD_REQUEST, "Instance name does not match");
      return;
    }
    val = gsexitCount.decrementAndGet();
    if (val <= 0) {
      HubDataStore hds;
      HubDataStore current;
      do {
        current = hubDataStoreReference.get();
        hds = new HubDataStore.Builder(current).outputReady().build();
      } while (!hubDataStoreReference.compareAndSet(current, hds));
      // Invokes Event Listener. All GSEXIT processes have completed /announce-stream-processing
      listener.announceReady();
    }
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @Path("/announce-fta-instance")
  @POST
  public void announceMetrics(HttpRequest request, HttpResponder responder) {
    JSONRequestData requestData = new JSONRequestData(request);
    if (!requestData.getInstanceName().equals(hubDataStoreReference.get().getInstanceName())) {
      responder.sendError(HttpResponseStatus.BAD_REQUEST, "Instance name does not match");
      return;
    }
    healthInspector.register(requestData.getProcessID());
    processNameMap.put(requestData.getProcessID(), requestData.getProcessName());
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @Path("/log-metrics")
  @POST
  public void logMetrics(HttpRequest request, HttpResponder responder) {
    JSONRequestData requestData = new JSONRequestData(request);
    if (!requestData.getInstanceName().equals(hubDataStoreReference.get().getInstanceName())) {
      responder.sendError(HttpResponseStatus.BAD_REQUEST, "Instance name does not match");
      return;
    }
    healthInspector.ping(requestData.getProcessID());
    metricsRecorder.recordMetrics(processNameMap.get(requestData.getProcessID()), requestData.getMetrics());
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @Path("/discover-instance/{instance}")
  @GET
  public void discoverInstance(HttpRequest request, HttpResponder responder,
                               @PathParam("instance") String instance) {
    if (!instance.equals(hubDataStoreReference.get().getInstanceName())) {
      responder.sendStatus(HttpResponseStatus.BAD_REQUEST);
      return;
    }
    InetSocketAddress address = hubDataStoreReference.get().getClearingHouseAddress();
    if (address == null) {
      responder.sendStatus(HttpResponseStatus.BAD_REQUEST);
      return;
    }
    responder.sendJson(HttpResponseStatus.OK, createResponse(address.getAddress().getHostAddress(), address.getPort()));
  }

  @Path("/discover-initialized-instance/{instance}")
  @GET
  public void discoverInitializedInstance(HttpRequest request, HttpResponder responder,
                                          @PathParam("instance") String instance) {
    if ((!hubDataStoreReference.get().isInitialized())
      || (!instance.equals(hubDataStoreReference.get().getInstanceName()))) {
      //TODO: Make sure Stream Engine accepts error Strings and then we can sendError instead of sendStatus
      //Instance name does not match or Service not initialized
      responder.sendStatus(HttpResponseStatus.BAD_REQUEST);
      return;
    }
    InetSocketAddress address = this.hubDataStoreReference.get().getClearingHouseAddress();
    if (address == null) {
      //TODO: Make sure Stream Engine accepts error Strings and then we can sendError instead of sendStatus
      //Clearing house address not defined
      responder.sendStatus(HttpResponseStatus.BAD_REQUEST);
      return;
    }
    responder.sendJson(HttpResponseStatus.OK, createResponse(address.getAddress().getHostAddress(), address.getPort()));
  }

  @Path("/discover-source/{dataSourceName}")
  @GET
  public void discoverSource(HttpRequest request, HttpResponder responder,
                             @PathParam("dataSourceName") String dataSourceName) {
    List<HubDataSource> dataSources = hubDataStoreReference.get().getHubDataSources();
    HubDataSource ds = null;
    for (HubDataSource hds : dataSources) {
      if (dataSourceName.equals(hds.getName())) {
        ds = hds;
        break;
      }
    }
    if (ds == null) {
      responder.sendError(HttpResponseStatus.BAD_REQUEST, "Data source : '" + dataSourceName + "' not found");
      return;
    }
    responder.sendJson(HttpResponseStatus.OK, createResponse(ds.getAddress().getAddress().getHostAddress(),
                                                             ds.getAddress().getPort()));
  }

  @Path("/discover-sink/{dataSinkName}")
  @GET
  public void discoverSink(HttpRequest request, HttpResponder responder,
                           @PathParam("dataSinkName") String dataSinkName) {
    List<HubDataSink> dataSink = hubDataStoreReference.get().getHubDataSinks();
    HubDataSink ds = null;
    for (HubDataSink hds : dataSink) {
      if (dataSinkName.equals(hds.getName())) {
        ds = hds;
        break;
      }
    }
    if (ds == null) {
      responder.sendError(HttpResponseStatus.BAD_REQUEST, "Data sink : '" + dataSinkName + "' not found");
      return;
    }
    responder.sendJson(HttpResponseStatus.OK, createResponse(ds.getAddress().getAddress().getHostAddress(),
                                                             ds.getAddress().getPort()));
  }

  @Path("/discover-start-processing/{instance}")
  @GET
  public void discoverStartProcessing(HttpRequest request, HttpResponder responder,
                                      @PathParam("instance") String instance) {
    if ((hubDataStoreReference.get().isOutputReady()) &&
      (hubDataStoreReference.get().getInstanceName().equals(instance))) {
      responder.sendJson(HttpResponseStatus.OK, new JsonObject());
      return;
    }
    responder.sendError(HttpResponseStatus.BAD_REQUEST, "Instance name does not match");
  }
}

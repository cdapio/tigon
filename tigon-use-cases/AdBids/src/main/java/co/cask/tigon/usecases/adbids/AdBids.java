/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.tigon.usecases.adbids;


import co.cask.tigon.api.common.Bytes;
import co.cask.tigon.api.flow.Flow;
import co.cask.tigon.api.flow.FlowSpecification;
import co.cask.tigon.usecases.adbids.advertizers.SportsAdvertiserFlowlet;
import co.cask.tigon.usecases.adbids.advertizers.TravelAdvertiserFlowlet;

/**
 * An application for advertisers to bid on ads for user-views.
 *
 * This {@link Flow} accepts the following runtime arguments:
 *  - input.service.port : Port to run the id input service on. Defaults to a random available port.
 *
 *  Standalone specific:
 *  - bids.output.file : Path of file to log bids to. Defaults to only logging the output.
 *
 *  Distributed specific:
 *  - hbase.conf.path : Path to HBase configuration file. A new table is created in this HBase instance to track
 *    granted bids. This argument is compulsory.
 */
public final class AdBids implements Flow {
  public static final byte[] BID_TABLE_NAME = Bytes.toBytes("tigon.usecases.adbids");

  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName("AdBids")
      .setDescription("An application for advertisers to bid on ads for user-views.")
      .withFlowlets()
      .add("id-collector", new UserIdInputFlowlet(), 1)
      .add("advertiser-notification", new AdvertiserNotificationFlowlet(), 1)
      .add("travel-advertiser", new TravelAdvertiserFlowlet(), 1)
      .add("sports-advertiser", new SportsAdvertiserFlowlet(), 1)
      .add("grant-bids", new GrantBidsFlowlet(), 1)
      .connect()
      .from("id-collector").to("advertiser-notification")
      .from("advertiser-notification").to("travel-advertiser")
      .from("advertiser-notification").to("sports-advertiser")
      .from("travel-advertiser").to("grant-bids")
      .from("sports-advertiser").to("grant-bids")
      .build();
  }
}

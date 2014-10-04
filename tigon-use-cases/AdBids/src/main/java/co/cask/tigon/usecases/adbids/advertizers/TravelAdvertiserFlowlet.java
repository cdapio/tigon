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

package co.cask.tigon.usecases.adbids.advertizers;

import co.cask.tigon.api.annotation.ProcessInput;
import co.cask.tigon.api.flow.flowlet.AbstractFlowlet;
import co.cask.tigon.api.flow.flowlet.OutputEmitter;
import co.cask.tigon.usecases.adbids.Bid;
import co.cask.tigon.usecases.adbids.IdData;
import co.cask.tigon.usecases.adbids.Item;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An advertiser that makes bids on users to show ads related to travel.
 */
public final class TravelAdvertiserFlowlet extends AbstractFlowlet {
  private static final Logger LOG = LoggerFactory.getLogger(TravelAdvertiserFlowlet.class);
  private OutputEmitter<Bid> bidOutputEmitter;

  @ProcessInput(Item.TRAVEL)
  @SuppressWarnings("UnusedDeclaration")
  public void process(IdData idData) throws Exception {
    double bidAmount;
    if (idData.getTotalCount() == 0) {
      bidAmount = 12;
    } else {
      bidAmount = ((idData.getTotalCount() - idData.getItemCount()) / (double) idData.getTotalCount()) * 12;
    }
    bidOutputEmitter.emit(new Bid(idData.getId(), Item.TRAVEL, bidAmount), "userId", idData.getId());
    LOG.info("Bid {} for user {}", bidAmount, idData.getId());
  }
}

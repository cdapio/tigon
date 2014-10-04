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

/**
 * A Bid made by an advertiser for a user-id.
 */
public final class Bid {
  private final String id;
  private final String item;
  private final double amount;

  /**
   * Create a new Bid for a user.
   * @param id id of the user.
   * @param item type of the advertisement.
   * @param amount bid amount for the advertisement.
   */
  public Bid(String id, String item, double amount) {
    this.id = id;
    this.item = item;
    this.amount = amount;
  }

  public String getId() {
    return id;
  }

  public String getItem() {
    return item;
  }

  public double getAmount() {
    return amount;
  }
}

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
 * A {@link Item} specific set of information used by advertisers to make a bid for a user-view.
 */
public final class IdData {
  private final String id;
  private final int itemCount;
  private final int totalCount;

  /**
   * Creates a new {@link IdData} to use by advertisers to make a bid.
   * @param id id of the user.
   * @param itemCount count of advertisements for a particular item.
   * @param totalCount total count of all advertisements for this user.
   */
  public IdData(String id, int itemCount, int totalCount) {
    this.id = id;
    this.itemCount = itemCount;
    this.totalCount = totalCount;
  }

  public String getId() {
    return id;
  }

  public int getItemCount() {
    return itemCount;
  }

  public int getTotalCount() {
    return totalCount;
  }
}

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
package co.cask.tigon.analytics;

import co.cask.tigon.api.flow.Flow;
import co.cask.tigon.api.flow.FlowSpecification;

/**
 * A {@link Flow} that collects Tweets and runs a Sentiment analysis algorithm on them.
 */
public class TwitterAnalytics implements Flow {

  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName("TwitterAnalytics")
      .setDescription("Analysis of tweets to generate top 10 hashtags.")
      .withFlowlets()
      .add("collector", new TweetCollector())
      .add("analyzer", new Analysis())
      .connect()
      .from("collector").to("analyzer")
      .build();
  }
}

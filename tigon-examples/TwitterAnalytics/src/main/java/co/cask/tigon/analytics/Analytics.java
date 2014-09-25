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

import co.cask.tigon.api.annotation.Batch;
import co.cask.tigon.api.annotation.ProcessInput;
import co.cask.tigon.api.annotation.Tick;
import co.cask.tigon.api.flow.flowlet.AbstractFlowlet;
import co.cask.tigon.api.flow.flowlet.FlowletContext;
import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Tweet analytics flowlet.
 */
public class Analytics extends AbstractFlowlet {

  private static final Logger LOG = LoggerFactory.getLogger(Analytics.class);
  private Map<String, Integer> hashtagCounts;

  @Override
  public void initialize(FlowletContext context) throws Exception {
    super.initialize(context);
    hashtagCounts = Maps.newConcurrentMap();
    LOG.info("Initialized Analysis flowlet.");
  }

  @Batch(100)
  @ProcessInput
  public void processTweets(Iterator<SimpleTweet> tweets) throws FileNotFoundException {
    while (tweets.hasNext()) {
      SimpleTweet tweet = tweets.next();
      for (String hashtag : tweet.getHashtags()) {
        if (hashtagCounts.containsKey(hashtag)) {
          hashtagCounts.put(hashtag, hashtagCounts.get(hashtag) + 1);
        } else {
          hashtagCounts.put(hashtag, 1);
        }
      }
    }
  }

  @Tick(unit = TimeUnit.SECONDS, delay = 60)
  public void logTopTweets() throws InterruptedException {
    if (hashtagCounts.isEmpty()) {
      return;
    }

    Ordering<Map.Entry<String, Integer>> entryOrdering = Ordering.natural()
      .onResultOf(new Function<Map.Entry<String, Integer>, Integer>() {
        public Integer apply(Map.Entry<String, Integer> entry) {
          return entry.getValue();
        }
      });

    List<Map.Entry<String, Integer>> topHashtags = entryOrdering.greatestOf(hashtagCounts.entrySet(), 10);
    hashtagCounts.clear();
    LOG.info("Top hashtags in the last minute : " + topHashtags);
  }
}

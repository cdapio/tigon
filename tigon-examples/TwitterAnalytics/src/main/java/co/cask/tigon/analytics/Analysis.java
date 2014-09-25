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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Sentiment analysis flowlet.
 */
public class Analysis extends AbstractFlowlet {

  private static final Logger LOG = LoggerFactory.getLogger(Analysis.class);
  private Map<String, Integer> hashtagCounts;

  @Override
  public void initialize(FlowletContext context) throws Exception {
    super.initialize(context);
    hashtagCounts = Maps.newConcurrentMap();
    LOG.info("Initialized Analysis flowlet.");
  }

  @Batch(100)
  @ProcessInput
  public void processTweets(Iterator<String> tweets) throws FileNotFoundException {
    while (tweets.hasNext()) {
      parseHashtags(tweets.next());
    }
  }

  @Tick(unit = TimeUnit.SECONDS, delay = 60)
  public void logAnalytics() throws InterruptedException {
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
    System.out.println(hashtagCounts);
    System.out.println(topHashtags);
  }

  private void parseHashtags(String tweet) {
    Matcher matcher = Pattern.compile("#\\s*(\\w+)").matcher(tweet);
    while (matcher.find()) {
      String hashtag = matcher.group(1);
      if (hashtagCounts.containsKey(hashtag)) {
        hashtagCounts.put(hashtag, hashtagCounts.get(hashtag) + 1);
      } else {
        hashtagCounts.put(hashtag, 1);
      }
    }
  }
}

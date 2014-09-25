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

import co.cask.tigon.api.annotation.Tick;
import co.cask.tigon.api.flow.flowlet.AbstractFlowlet;
import co.cask.tigon.api.flow.flowlet.FlowletContext;
import co.cask.tigon.api.flow.flowlet.OutputEmitter;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.HashtagEntity;
import twitter4j.Status;
import twitter4j.StatusAdapter;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Poll for tweets and batch them to the next Flowlet.
 */
public class TweetCollector extends AbstractFlowlet {
  private static final Logger LOG = LoggerFactory.getLogger(TweetCollector.class);

  private OutputEmitter<SimpleTweet> output;

  private CollectingThread collector;
  private BlockingQueue<Status> queue;

  private int tweetAmplification;

  private TwitterStream twitterStream;

  @Override
  public void initialize(FlowletContext context) throws Exception {
    super.initialize(context);
    Map<String, String> args = context.getRuntimeArguments();

    if (args.containsKey("disable.public")) {
      String publicArg = args.get("disable.public");
      LOG.info("Public Twitter source turned off (disable.public={})", publicArg);
      return;
    }
    tweetAmplification = 1;
    if (args.containsKey("tweet.amplification")) {
      tweetAmplification = Integer.parseInt(args.get("tweet.amplification"));
      LOG.info("Tweets are being amplified (tweet.amplification={})", tweetAmplification);
    }
    queue = new LinkedBlockingQueue<Status>(10000);
    collector = new CollectingThread();
    collector.start();
  }

  @Override
  public void destroy() {
    collector.interrupt();
    twitterStream.cleanUp();
    twitterStream.shutdown();
  }

  @Tick(unit = TimeUnit.MILLISECONDS, delay = 100)
  public void collect() throws InterruptedException {
    if (this.queue == null) {
      // Sleep and return if public timeline is disabled
      Thread.sleep(1000);
      return;
    }
    int batchSize = 100;

    for (int i = 0; i < batchSize; i++) {
      Status tweet = queue.poll();
      if (tweet == null) {
        break;
      }

      // emitting more data to get higher throughput
      for (int k = 0; k < tweetAmplification; k++) {
        List<String> hashtags = Lists.newArrayList();
        for (HashtagEntity hashtag : tweet.getHashtagEntities()) {
          hashtags.add(hashtag.getText());
        }
        output.emit(new SimpleTweet(tweet.getText(), hashtags));
      }
    }
  }

  private class CollectingThread extends Thread {

    @Override
    public void run() {
      try {
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(false);
        cb.setAsyncNumThreads(1);

        Map<String, String> args = getContext().getRuntimeArguments();
        // Override twitter4j.properties file, if provided in runtime args.
        if (args.containsKey("oauth.consumerKey") && args.containsKey("oauth.consumerSecret") &&
          args.containsKey("oauth.accessToken") && args.containsKey("oauth.accessTokenSecret")) {

          cb.setOAuthConsumerKey(args.get("oauth.consumerKey"));
          cb.setOAuthConsumerSecret(args.get("oauth.consumerSecret"));
          cb.setOAuthAccessToken(args.get("oauth.accessToken"));
          cb.setOAuthAccessTokenSecret(args.get("oauth.accessTokenSecret"));
        }

        twitterStream = new TwitterStreamFactory(cb.build()).getInstance();

        StatusListener listener = new StatusAdapter() {
          @Override
          public void onStatus(Status status) {
            String tweet = status.getText();
            String lang = status.getLang();
            if (!lang.equals("en")) {
              return;
            }
            try {
              queue.put(status);
            } catch (InterruptedException e) {
              LOG.warn("Interrupted writing to queue", e);
              return;
            }
          }

          @Override
          public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
            LOG.error("Got track limitation notice:" + numberOfLimitedStatuses);
          }

          @Override
          public void onException(Exception ex) {
            LOG.warn("Error during reading from stream" + ex.getMessage());
          }
        };

        twitterStream.addListener(listener);
        twitterStream.sample();
      } catch (Exception e) {
        LOG.error("Got exception {}", e);
      } finally {
        LOG.info("CollectingThread run() exiting");
      }
    }
  }
}

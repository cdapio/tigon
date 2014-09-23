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

package co.cask.tigon.sentiment;

import co.cask.tigon.api.annotation.Batch;
import co.cask.tigon.api.annotation.ProcessInput;
import co.cask.tigon.api.flow.flowlet.AbstractFlowlet;
import co.cask.tigon.api.flow.flowlet.FlowletContext;
import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;

/**
 * Basic java-based sentiment classifier.
 */
public class Analysis extends AbstractFlowlet {

  private static final Logger LOG = LoggerFactory.getLogger(Analysis.class);
  private static final String LOCALIZED_FILENAME = "localized.txt";

  TextClassifier classifierModel = null;

  @Override
  public void initialize(FlowletContext context) throws Exception {
    super.initialize(context);

    InputStream in = null;
    FileOutputStream out = null;
    try {
      in = this.getClass().getClassLoader().getResourceAsStream("java_trained_classifier.txt");
      out = new FileOutputStream(LOCALIZED_FILENAME); // localized within container, so it get cleaned.
      ByteStreams.copy(in, out);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    } finally {
      try {
        if (in != null) {
          in.close();
        }
        if (out != null) {
          out.close();
        }
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
    InputStream modelInputStream = new FileInputStream(new File(LOCALIZED_FILENAME));
    classifierModel = TextClassifier.createFromObjectStream(new ObjectInputStream(modelInputStream));
    LOG.info("Initialized Analysis flowlet.");
  }

  @Batch(100)
  @ProcessInput
  public void classifyTweet(String tweet) throws FileNotFoundException, ClassifierResultException {
//    while (tweetIterator.hasNext()) {
//      System.out.println(classify(tweetIterator.next()));
//    }
    System.out.println(classify(tweet).toString());
  }

  public ClassificationResult classify(String text) throws FileNotFoundException, ClassifierResultException {
    return classifierModel.classify(text);
  }
}

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

import com.google.common.base.Objects;

/**
 * This class represents the value and confidence of any classification.
 */
public class ClassificationResult {
  private String value;
  private double confidence;
  private Sentiment sentiment;

  /**
   * Create a new ClassificationResult with a value and it's confidence. An appropriate sentiment is assigned.
   * @param value Value of the classification.
   * @param confidence Confidence of the classification.
   * @throws ClassifierResultException
   */
  public ClassificationResult(String value, double confidence) throws ClassifierResultException {
    this.value = value;
    this.confidence = confidence;

    if (value.equals("pos")) {
      sentiment = Sentiment.positive;
    } else if (value.equals("neg")) {
      sentiment = Sentiment.negative;
    } else if (value.equals("neu")) {
      sentiment = Sentiment.neutral;
    } else {
      throw new ClassifierResultException("Classifier return result not recognized. ");
    }
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("sentiment", sentiment.toString())
      .add("value", value)
      .add("confidence", confidence).toString();
  }

  /**
   * Enum of possible sentiments.
   */
  public static enum Sentiment {
    positive, neutral, negative;
  }

}

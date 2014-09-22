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

package co.cask.tigon.internal.app.runtime;

import co.cask.tigon.data.queue.ForwardingQueueConsumer;
import co.cask.tigon.data.queue.QueueConsumer;

import java.io.Closeable;
import java.io.IOException;

/**
 * A {@link com.continuuity.tephra.TransactionAware} {@link QueueConsumer} that removes itself from dataset context
 * when closed. All queue operations are forwarded to another {@link QueueConsumer}.
 */
final class CloseableQueueConsumer extends ForwardingQueueConsumer implements Closeable {

  private final AbstractDataFabricFacade fabricFacade;

  CloseableQueueConsumer(AbstractDataFabricFacade fabricFacade, QueueConsumer consumer) {
    super(consumer);
    this.fabricFacade = fabricFacade;
  }

  @Override
  public void close() throws IOException {
    try {
      if (consumer instanceof Closeable) {
        ((Closeable) consumer).close();
      }
    } finally {
      fabricFacade.removeTransactionAware(this);
    }
  }
}

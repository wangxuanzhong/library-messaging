/*
 * MIT License
 *
 * Copyright (c) 2019 Syswin
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.syswin.library.messaging.embedded;

import com.syswin.library.messaging.MqConsumer;
import java.util.function.Consumer;

public class EmbeddedMqConsumer implements MqConsumer {

  private final String topic;
  private final MessageQueue messageQueue;
  private final Consumer<String> messageListener;

  public EmbeddedMqConsumer(MessageQueue messageQueue, String topic, Consumer<String> messageListener) {
    this.topic = topic;
    this.messageQueue = messageQueue;
    this.messageListener = messageListener;
  }

  @Override
  public void start() {
    messageQueue.subscribe(topic, messageListener);
  }

  @Override
  public void shutdown() {

  }

  @Override
  public String topic() {
    return topic;
  }
}

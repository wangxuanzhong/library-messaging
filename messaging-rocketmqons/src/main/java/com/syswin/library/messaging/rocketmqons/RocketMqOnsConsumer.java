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

package com.syswin.library.messaging.rocketmqons;

import com.aliyun.openservices.ons.api.Action;
import com.aliyun.openservices.ons.api.Consumer;
import com.aliyun.openservices.ons.api.ONSFactory;
import java.util.Properties;

public class RocketMqOnsConsumer extends AbstractRocketMqOnsConsumer<Consumer> {

  public RocketMqOnsConsumer(
      RocketMqOnsConfig mqConfig,
      String topic,
      String tag,
      String messageModel,
      java.util.function.Consumer<String> messageConsumer) {
    super(mqConfig, topic, tag, messageModel, messageConsumer);
  }

  @Override
  protected Consumer createConsumer(Properties properties) {
    return ONSFactory.createConsumer(properties);
  }

  @Override
  protected void subscribe(String topic, String tag) {
    rmqConsumer.subscribe(topic, tag,
        (message, context) -> {
          try {
            RocketMqOnsConsumer.this.consume(message);
          } catch (Exception e) {
            log.error("Failed to consume message from Rocket MQ: {}", message, e);
            return Action.ReconsumeLater;
          }
          return Action.CommitMessage;
        });
  }

}

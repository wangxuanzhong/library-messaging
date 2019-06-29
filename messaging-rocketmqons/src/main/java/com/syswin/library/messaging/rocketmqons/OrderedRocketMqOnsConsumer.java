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

import com.aliyun.openservices.ons.api.ONSFactory;
import com.aliyun.openservices.ons.api.order.OrderAction;
import com.aliyun.openservices.ons.api.order.OrderConsumer;
import java.util.Properties;
import java.util.function.Consumer;

public class OrderedRocketMqOnsConsumer extends AbstractRocketMqOnsConsumer<OrderConsumer> {

  public OrderedRocketMqOnsConsumer(
      RocketMqOnsConfig mqConfig,
      String topic,
      String tag,
      String messageModel,
      Consumer<String> messageConsumer) {
    super(mqConfig, topic, tag, messageModel, messageConsumer);
  }

  @Override
  protected OrderConsumer createConsumer(Properties properties) {
    return ONSFactory.createOrderedConsumer(consumerProperties);
  }

  @Override
  protected void subscribe(String topic, String tag) {
    rmqConsumer.subscribe(topic, tag,
        (message, context) -> {
          try {
            OrderedRocketMqOnsConsumer.this.consume(message);
          } catch (Exception e) {
            log.error("Failed to consume message from Rocket MQ: {}", message, e);
            return OrderAction.Suspend;
          }
          return OrderAction.Success;
        });
  }
}

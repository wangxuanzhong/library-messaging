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

import com.aliyun.openservices.ons.api.Admin;
import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.syswin.library.messaging.MessagingException;
import com.syswin.library.messaging.MqConsumer;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractRocketMqOnsConsumer<C extends Admin> implements MqConsumer {

  protected static final Logger log = LoggerFactory.getLogger(AbstractRocketMqOnsConsumer.class);
  protected final C rmqConsumer;
  private final String topic;
  private final String tag;
  private final Consumer<String> messageConsumer;
  protected Properties consumerProperties;

  AbstractRocketMqOnsConsumer(RocketMqOnsConfig mqConfig, String topic, String tag, String messageModel,
      Consumer<String> messageConsumer) {
    this.topic = topic;
    this.tag = tag;
    this.messageConsumer = messageConsumer;

    consumerProperties = new Properties();
    consumerProperties.putAll(mqConfig);
    // messageModel的取值见：PropertyValueConst
    consumerProperties.put(PropertyKeyConst.MessageModel, messageModel);
    consumerProperties.put(PropertyKeyConst.InstanceName, UUID.randomUUID().toString());
    rmqConsumer = createConsumer(consumerProperties);
  }

  @Override
  public final void start() throws MessagingException {
    try {
      subscribe(topic, tag);
      rmqConsumer.start();
      log.debug("Rocket MQ consumer {} in group {} is listening on topic {} tag {} with namesrv {}",
          getInstanceName(),
          getGroupId(),
          topic,
          tag,
          getNamesrvAddr());
    } catch (RuntimeException e) {
      throw new MessagingException("Failed to start Rocket MQ consumer with namesrv " +
          getNamesrvAddr(), e);
    }
  }

  @Override
  public final void shutdown() {
    rmqConsumer.shutdown();
    log.debug("Rocket MQ consumer {} in group {} shut down successfully",
        getInstanceName(),
        getGroupId());
  }

  @Override
  public String topic() {
    return topic;
  }

  protected final void consume(Message message) {
    log.debug("Rocket MQ consumer received message {} on topic {} tag {}", message, topic, tag);
    messageConsumer.accept(new String(message.getBody()));
  }

  protected abstract C createConsumer(Properties properties);

  protected abstract void subscribe(String topic, String tag);

  private String getNamesrvAddr() {
    return consumerProperties.getProperty(PropertyKeyConst.NAMESRV_ADDR);
  }

  private String getGroupId() {
    return consumerProperties.getProperty(PropertyKeyConst.GROUP_ID);
  }

  private String getInstanceName() {
    return consumerProperties.getProperty(PropertyKeyConst.InstanceName);
  }

}

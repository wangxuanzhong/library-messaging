package com.syswin.library.messaging.rocketmqons;

import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.ONSFactory;
import com.aliyun.openservices.ons.api.Producer;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.aliyun.openservices.ons.api.SendResult;
import com.aliyun.openservices.ons.api.order.OrderProducer;
import com.syswin.library.messaging.MessagingException;
import com.syswin.library.messaging.MqProducer;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocketMqOnsProducer implements MqProducer {

  protected static final Logger log = LoggerFactory.getLogger(RocketMqOnsProducer.class);

  protected final Properties producerProperties;
  protected final Producer rmqProducer;
  protected final OrderProducer rmqOrderProducer;

  public RocketMqOnsProducer(RocketMqOnsConfig mqConfig) {
    producerProperties = new Properties();
    producerProperties.putAll(mqConfig);
    producerProperties.setProperty(PropertyKeyConst.InstanceName, UUID.randomUUID().toString());
    rmqProducer = ONSFactory.createProducer(mqConfig);
    rmqOrderProducer = ONSFactory.createOrderProducer(mqConfig);
  }

  public final void start() throws MessagingException {
    try {
      rmqProducer.start();
      rmqOrderProducer.start();
      log.debug("Rocket MQ producer {} in group {} started successfully with namesrv {}",
          producerProperties.get(PropertyKeyConst.InstanceName),
          producerProperties.get(PropertyKeyConst.GROUP_ID),
          producerProperties.get(PropertyKeyConst.NAMESRV_ADDR));
    } catch (RuntimeException e) {
      throw new MessagingException("Failed to start Rocket MQ producer with broker "
          + producerProperties.get(PropertyKeyConst.NAMESRV_ADDR), e);
    }
  }

  public final void shutdown() {
    rmqProducer.shutdown();
    rmqOrderProducer.shutdown();
    log.debug("Rocket MQ producer {} in group {} shut down successfully",
        producerProperties.get(PropertyKeyConst.InstanceName),
        producerProperties.get(PropertyKeyConst.GROUP_ID));
  }

  @Override
  public final void send(String message, String topic, String tags, String keys)
      throws MessagingException {
    doSend(message, topic, tags, keys, this::orderedSend);
  }

  @Override
  public final void sendRandomly(String message, String topic)
      throws MessagingException {
    doSend(message, topic, "", "", rmqProducer::send);
  }

  @Override
  public final void sendOrderly(String message, String topic)
      throws MessagingException {
    doSend(message, topic, "", "", this::orderedSend);
  }

  private void doSend(String message,
      String topic,
      String tags,
      String keys,
      MsgSender<Message, SendResult> msgSender)
      throws MessagingException {

    Message mqMsg = new Message(topic, tags, keys, message.getBytes(StandardCharsets.UTF_8));
    log.debug("Sending message {} with topic: {}, tag: {}, keys: {} to Rocket MQ", message, topic, tags, keys);
    SendResult sendResult = msgSender.send(mqMsg);
    log.debug("Sent message {} with topic: {}, tag: {}, keys: {} to Rocket MQ with result {}", message, topic, tags,
        keys, sendResult);
  }

  private SendResult orderedSend(Message message) throws MessagingException {
    String topic = message.getTopic();
    try {
      return rmqOrderProducer.send(message, topic);
    } catch (Exception e) {
      throw new MessagingException(String
          .format("Failed to send message with topic: %s to Rocket MQ %s", topic,
              producerProperties.get(PropertyKeyConst.NAMESRV_ADDR)), e);
    }
  }
}

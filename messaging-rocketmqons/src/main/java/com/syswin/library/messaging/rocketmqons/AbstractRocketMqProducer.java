package com.syswin.library.messaging.rocketmqons;

import com.aliyun.openservices.ons.api.Admin;
import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.aliyun.openservices.ons.api.SendResult;
import com.aliyun.openservices.ons.api.transaction.LocalTransactionChecker;
import com.syswin.library.messaging.MessagingException;
import com.syswin.library.messaging.MqProducer;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author 姚华成
 * @date 2019-06-27
 */
public abstract class AbstractRocketMqProducer implements MqProducer {

  protected static final Logger log = LoggerFactory.getLogger(AbstractRocketMqProducer.class);

  protected final Properties producerProperties;
  protected final Admin rmqProducer;

  public AbstractRocketMqProducer(RocketMqConfig mqConfig, LocalTransactionChecker checker) {
    producerProperties = new Properties();
    producerProperties.putAll(mqConfig);
    producerProperties.setProperty(PropertyKeyConst.InstanceName, UUID.randomUUID().toString());
    rmqProducer = createProducer(mqConfig, checker);
  }

  protected abstract Admin createProducer(RocketMqConfig mqConfig, LocalTransactionChecker checker);


  public final void start() throws MessagingException {
    try {
      rmqProducer.start();
      log.info("Rocket MQ producer {} in group {} started successfully with namesrv {}",
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
    log.info("Rocket MQ producer {} in group {} shut down successfully",
        producerProperties.get(PropertyKeyConst.InstanceName),
        producerProperties.get(PropertyKeyConst.GROUP_ID));
  }

  @Override
  public void send(String message, String topic, String tags, String keys)
      throws MessagingException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void sendOrderly(String message, String topic)
      throws MessagingException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void sendRandomly(String message, String topic)
      throws MessagingException {
    throw new UnsupportedOperationException();
  }

  protected void doSend(String message,
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
}

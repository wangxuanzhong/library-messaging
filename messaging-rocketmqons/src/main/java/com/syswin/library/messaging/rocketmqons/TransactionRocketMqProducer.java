package com.syswin.library.messaging.rocketmqons;

import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.Message.SystemPropKey;
import com.aliyun.openservices.ons.api.ONSFactory;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.aliyun.openservices.ons.api.SendResult;
import com.aliyun.openservices.ons.api.impl.rocketmq.TransactionProducerImpl;
import com.aliyun.openservices.ons.api.transaction.LocalTransactionChecker;
import com.aliyun.openservices.ons.api.transaction.LocalTransactionExecuter;
import com.aliyun.openservices.ons.api.transaction.TransactionProducer;
import com.syswin.library.messaging.MessagingException;
import java.nio.charset.StandardCharsets;

public class TransactionRocketMqProducer extends AbstractRocketMqProducer {

  private final LocalTransactionExecuter transactionExecuter;

  public TransactionRocketMqProducer(RocketMqConfig mqConfig,
      LocalTransactionChecker checker,
      LocalTransactionExecuter transactionExecuter) {
    super(mqConfig, checker);
    this.transactionExecuter = transactionExecuter;
  }

  @Override
  protected TransactionProducer createProducer(RocketMqConfig mqConfig, LocalTransactionChecker checker) {
    return ONSFactory.createTransactionProducer(mqConfig, checker);
  }

  public final void sendTransactional(String message, String topic, String tags, String keys, Object arg)
      throws MessagingException {
    Message mqMsg = new Message(topic, tags, keys, message.getBytes(StandardCharsets.UTF_8));
    log.debug("Sending message {} with topic: {}, tag: {}, keys: {} to Rocket MQ", message, topic, tags, keys);
    SendResult sendResult = loadBalancedSend(mqMsg, arg);
    log.debug("Sent message {} with topic: {}, tag: {}, keys: {} to Rocket MQ with result {}", message, topic, tags,
        keys, sendResult);
  }

  private SendResult loadBalancedSend(Message message, Object arg) throws MessagingException {
    String msgTags = message.getUserProperties(SystemPropKey.TAG);
    String topic = message.getTopic();
    String keys = message.getKey();
    try {
      return ((TransactionProducerImpl) rmqProducer).send(message, transactionExecuter, arg);
    } catch (Exception e) {
      throw new MessagingException(String
          .format("Failed to send message with topic: %s, tag: %s, keys: %s to Rocket MQ %s", topic, msgTags, keys,
              producerProperties.get(PropertyKeyConst.NAMESRV_ADDR)), e);
    }
  }

}

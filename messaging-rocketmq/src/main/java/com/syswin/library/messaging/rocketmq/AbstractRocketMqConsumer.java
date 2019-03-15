package com.syswin.library.messaging.rocketmq;

import com.syswin.library.messaging.MessagingException;
import com.syswin.library.messaging.MqConsumer;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class AbstractRocketMqConsumer implements MqConsumer {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  final DefaultMQPushConsumer consumer;

  private final String topic;
  private final String tag;
  private final String brokerAddress;
  private final MessageModel messageModel;
  private final Consumer<String> messageConsumer;

  AbstractRocketMqConsumer(
      String brokerAddress,
      String groupName,
      String topic,
      String tag,
      MessageModel messageModel,
      Consumer<String> messageConsumer) {

    this.consumer = new DefaultMQPushConsumer(groupName);
    this.topic = topic;
    this.tag = tag;
    this.brokerAddress = brokerAddress;
    this.messageModel = messageModel;
    this.messageConsumer = messageConsumer;
  }

  @Override
  public final void start() throws MessagingException {
    try {
      consumer.setNamesrvAddr(brokerAddress);
      consumer.setMessageModel(messageModel);
      consumer.setInstanceName(UUID.randomUUID().toString());
      consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
      consumer.subscribe(topic, tag);
      addMessageListener();
      consumer.start();
      log.info("Rocket MQ consumer {} in group {} is listening on topic {} tag {} with broker {}",
          consumer.getInstanceName(),
          consumer.getConsumerGroup(),
          topic,
          tag,
          brokerAddress);
    } catch (MQClientException e) {
      throw new MessagingException("Failed to start Rocket MQ consumer with broker " + brokerAddress, e);
    }
  }

  @Override
  public final void shutdown() {
    consumer.shutdown();
    log.info("Rocket MQ consumer {} in group {} shut down successfully", consumer.getInstanceName(), consumer.getConsumerGroup());
  }

  final void consume(List<MessageExt> messages) {
    for (MessageExt msg : messages) {
      log.debug("Rocket MQ consumer received message {} on topic {} tag {}", msg, topic, tag);
      messageConsumer.accept(new String(msg.getBody()));
    }
  }

  abstract void addMessageListener();
}

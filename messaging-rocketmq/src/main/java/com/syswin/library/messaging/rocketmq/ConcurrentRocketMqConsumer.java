package com.syswin.library.messaging.rocketmq;

import java.lang.invoke.MethodHandles;
import java.util.function.Consumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConcurrentRocketMqConsumer extends AbstractRocketMqConsumer {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public ConcurrentRocketMqConsumer(String brokerAddress,
      String groupName,
      String topic,
      String tag,
      MessageModel messageModel,
      Consumer<String> messageConsumer) {

    super(brokerAddress, groupName, topic, tag, messageModel, messageConsumer);
  }

  @Override
  void addMessageListener() {
    consumer.registerMessageListener((MessageListenerConcurrently) (messages, context) -> {
      try {
        consume(messages);
      } catch (Exception e) {
        log.error("Failed to consume messages from Rocket MQ: {}", messages, e);
        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
      }
      return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    });
    log.info("Registered concurrent message listener");
  }
}

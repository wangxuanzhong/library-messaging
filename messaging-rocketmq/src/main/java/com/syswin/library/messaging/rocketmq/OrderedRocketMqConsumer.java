package com.syswin.library.messaging.rocketmq;

import java.lang.invoke.MethodHandles;
import java.util.function.Consumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrderedRocketMqConsumer extends AbstractRocketMqConsumer {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  OrderedRocketMqConsumer(String brokerAddress,
      String groupName,
      String topic,
      String tag,
      MessageModel messageModel) {
    super(brokerAddress, groupName, topic, tag, messageModel);
  }

  @Override
  void addMessageListener(Consumer<String> messageConsumer) {
    consumer.registerMessageListener((MessageListenerOrderly) (messages, context) -> {
      try {
        consume(messageConsumer, messages);
      } catch (Exception e) {
        log.error("Failed to consume messages from Rocket MQ: {}", messages, e);
        return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
      }
      return ConsumeOrderlyStatus.SUCCESS;

    });
    log.info("Registered ordered message listener");
  }
}

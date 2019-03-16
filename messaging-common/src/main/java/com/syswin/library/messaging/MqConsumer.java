package com.syswin.library.messaging;

public interface MqConsumer {

  void start() throws MessagingException;

  void shutdown();

  String topic();
}

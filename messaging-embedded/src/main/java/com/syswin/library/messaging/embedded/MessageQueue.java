package com.syswin.library.messaging.embedded;

import java.util.function.Consumer;

public interface MessageQueue {

  void start();

  void send(String topic, String message);

  void subscribe(String topic, Consumer<String> messageListener);

  void shutdown();
}

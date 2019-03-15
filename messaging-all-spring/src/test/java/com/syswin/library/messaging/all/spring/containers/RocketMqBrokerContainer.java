package com.syswin.library.messaging.all.spring.containers;

import static org.testcontainers.containers.BindMode.READ_ONLY;

import org.testcontainers.containers.FixedHostPortGenericContainer;

public class RocketMqBrokerContainer extends FixedHostPortGenericContainer<RocketMqBrokerContainer> {

  public RocketMqBrokerContainer() {
    super("seanyinx/rocketmq-broker:4.3.0");
  }

  @Override
  protected void configure() {
    super.configure();

    withClasspathResourceMapping("broker.properties", "/opt/rocketmq-4.3.0/conf/broker.properties", READ_ONLY);
  }
}

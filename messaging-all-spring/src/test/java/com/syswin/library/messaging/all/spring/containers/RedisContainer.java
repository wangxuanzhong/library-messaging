package com.syswin.library.messaging.all.spring.containers;

import org.testcontainers.containers.GenericContainer;

public class RedisContainer extends GenericContainer<RedisContainer> {
  public RedisContainer() {
    super("redis:4.0-alpine");
  }

  @Override
  protected void configure() {
    super.configure();

    withExposedPorts(6379);
  }
}

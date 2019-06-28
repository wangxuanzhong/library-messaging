package com.syswin.library.messaging.rocketmqons;

/**
 * @author 姚华成
 * @date 2019-06-27
 */
public class AbstractRocketMqTest {

  protected static final String topic = "simpleTopic";
  protected static final String orderTopic = "PartitionOrderTopic";
  protected static final String tag = "*";
  protected static final String message1 = "message1";
  protected static final String message2 = "message2";
  protected static final String message3 = "message3";
  private static final String NAMESRV_ADDR = "localhost:9876";
  private static final String ACCESS_KEY = "access_key";
  private static final String SECRET_KEY = "secret_key";
  private static final String GROUP_ID = "GID-temail-test";
  protected static final RocketMqConfig mqConfig = new RocketMqConfig(NAMESRV_ADDR, ACCESS_KEY, SECRET_KEY, GROUP_ID);
}

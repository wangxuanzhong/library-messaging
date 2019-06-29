package com.syswin.library.messaging.rocketmqons;

import com.aliyun.openservices.ons.api.PropertyKeyConst;
import java.util.Properties;

/**
 * @author 姚华成
 * @date 2019-06-26
 */
public class RocketMqOnsConfig extends Properties {

  public RocketMqOnsConfig(String namesrvAddr, String accessKey, String secretKey, String groupId) {
    setProperty(PropertyKeyConst.NAMESRV_ADDR, namesrvAddr);
    setProperty(PropertyKeyConst.AccessKey, accessKey);
    setProperty(PropertyKeyConst.SecretKey, secretKey);
    setProperty(PropertyKeyConst.GROUP_ID, groupId);
  }
}

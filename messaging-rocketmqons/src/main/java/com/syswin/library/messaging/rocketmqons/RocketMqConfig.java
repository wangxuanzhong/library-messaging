package com.syswin.library.messaging.rocketmqons;

import com.aliyun.openservices.ons.api.PropertyKeyConst;
import java.util.Properties;
import lombok.NoArgsConstructor;

/**
 * @author 姚华成
 * @date 2019-06-26
 */
@NoArgsConstructor
public class RocketMqConfig extends Properties {

  public RocketMqConfig(String namesrvAddr, String accessKey, String secretKey, String groupId) {
    setNamesrvAddr(namesrvAddr);
    setAccessKey(accessKey);
    setSecretKey(secretKey);
    setGroupId(groupId);
  }

  public String getGroupId() {
    return getProperty(PropertyKeyConst.GROUP_ID);
  }

  public void setGroupId(String groupId) {
    setProperty(PropertyKeyConst.GROUP_ID, groupId);
  }

  public String getAccessKey() {
    return getProperty(PropertyKeyConst.AccessKey);
  }

  public void setAccessKey(String accessKey) {
    setProperty(PropertyKeyConst.AccessKey, accessKey);
  }

  public String getSecretKey() {
    return getProperty(PropertyKeyConst.SecretKey);
  }

  public void setSecretKey(String secretKey) {
    setProperty(PropertyKeyConst.SecretKey, secretKey);
  }

  public String getNamesrvAddr() {
    return getProperty(PropertyKeyConst.NAMESRV_ADDR);
  }

  public void setNamesrvAddr(String namesrvAddr) {
    setProperty(PropertyKeyConst.NAMESRV_ADDR, namesrvAddr);
  }

}

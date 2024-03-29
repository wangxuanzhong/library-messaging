# Library Messaging
包含MQ客户端的以下实现
* RocketMQ
* Redis
* Blocking Queue

## 示例
1. 为每个`MQ producer / consumer` 暴露一个对应的`MqProducerConfig / MqConsumerConfig`
    ```java
      @Bean
      MqProducerConfig producerConfig(String group) {
        return new MqProducerConfig(group, ROCKET_MQ);
      }
    
      @Bean
      MqConsumerConfig consumerConfig(
          String group,
          String topic,
          String tag,
          Consumer<String> listener
      ) {
        return MqConsumerConfig.create()
            .group(group)
            .topic(topic)
            .tag(tag)      // 默认为""
            .type(CLUSTER) // 默认为集群模式
            .implementation(ROCKET_MQ)
            .listener(listener)
            .concurrent()  // 默认为并行消费模式
            .build();
      }
    ```
1. 在业务中注入并使用`MqProducer`，`producers`以`producerGroup`为键值
    ```java
      @Autowired
      private Map<String, MqProducer> producers;
    ```
1. 通过不同的服务启动配置参数控制使用哪种MQ
    ```
    library.messaging.embedded.enabled=true // Blocking Queue实现
    library.messaging.redis.enabled=true    // Redis MQ实现
    library.messaging.rocketmq.enabled=true // Rocket MQ实现
    spring.rocketmq.host=rocketmq.syswin.com
    ```

![](/logo/Aletheia.png)

Aletheia is a library making it easy to mobilize large scale streaming data in a monitored, versioned manner. Alethehia presents a uniform API for leveraging existing frameworks such as Kafka, and can be extended to support custom frameworks.

Aletheia was designed with the following features in mind:

 1. **Uniform API** - whether you're dispatching data to Kafka,  a log file, or some custom destination, you're given a uniform API, making it easy to switch over from one messaging system to another. Custom destinations can also be implemented and plugged into the core.
 2. **Data flow monitoring** - Each datum produced, and each datum sent are counted and reported in real time so as to allow quick detection of problems in one's data flow. Monitoring information is a built-in datum type (called a *breadcrumb*), and can be dispatched and consumed just as any of your propriety datum types.
 3. **Datum schema versioning** - your data may change over time, some fields get added, other removed, or even renamed. Aletheia uses Avro to support schema evolution, making it a good fit for cases you need to decouple the producing and consuming ends so that your pipeline could keep crunching in light of live changes.

For example:

```java
DatumProducer<Click> datumProducer = 
    DatumProducerBuilder
      .forDomainClass(Click.class)
      .registerProductionEndPointType(KafkaTopicProductionEndPoint.class,
                                      new KafkaSenderFactory())
      .deliverDataTo(new KafkaTopicProductionEndPoint(...))
      .build(new DatumProducerConfig(...));

datumProducer.deliver(new Click(...));
```    

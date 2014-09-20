![](/logo/Aletheia.png)

Aletheia is a library that makes it easier to deliver large scale streaming data using a uniform API, in a monitored, versioned manner. Alethehia can be used to leverage existing mesasging systems such as Kafka, logging frameworks such as Log4j. It is also suitable for use atop custom delivery frameworks.

# The Benefits

 1. **Uniform API** - whether you're dispatching data to Kafka, writing to a log file, or dong some other custom, funky stuff, you're given a uniform API, making it easy to switch over from one delivery system to another.
 2. **Data flow monitoring** - each datum produced and each datum consumed is counted and reported in real time so as to allow quick detection of problems in one's data flow. Monitoring information is a built-in datum type (called a *breadcrumb*), and can be dispatched and consumed just as any of your propriety datum types.
 3. **Datum schema evolution** - your data may change over time, some fields get added, other removed, or even renamed. Aletheia uses Avro to support schema evolution, making it a good fit for cases where you need to further decouple the producing and consuming ends so that your pipeline could keep crunching even in light of live changes.

For example, build a `DatumProducer` that will produce data to a Kafka topic:

```java
DatumProducer<Click> datumProducer = 
    DatumProducerBuilder
      .forDomainClass(Click.class)
      .registerProductionEndPointType(KafkaTopicProductionEndPoint.class,
                                      new KafkaDatumEnvelopeSenderFactory())
      .deliverDataTo(new KafkaTopicProductionEndPoint(...))
      .build(new DatumProducerConfig(...));
```

Then, produce away:

```java
datumProducer.deliver(new Click(...));
```    

# Documentation
*  [Wiki](https://github.com/outbrain/Aletheia/wiki)

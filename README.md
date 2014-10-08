<img src="/logo/Aletheia-logo.png" height="115"></img>
![](https://github.com/outbrain/Aletheia/blob/master/logo/Aletheia.png)

Aletheia is a library that provides an abstraction for moving streaming data between various locations in a monitored and versioned manner. It has a pluggable architecture allowing multiple endpoints such as log files and kafka topics to be added easily. Adding a custom endpoint type is very simple as well. Aletheia supports multiple data encodings natively.

Aletheia's goal is to become a general data pipeline framework.

# The Benefits

 1. **Uniform API** - Whether you're dispatching data to Kafka, writing to a log file, or sending data to your own funky end point, the API is identical making it seamless to add delivery end points.
 2. **Data flow monitoring** - each datum produced and each datum consumed is counted and reported in real time so as to allow quick detection of problems in one's data flow. Monitoring information is a built-in datum type (called a *breadcrumb*), and can be dispatched and consumed just as any of your propriety datum types.
 3. **Datum schema evolution** - your data may change over time, some fields get added, other removed, or even renamed. Aletheia supports Avro in a native way, making it a good fit for cases where you need to further decouple the producing and consuming ends so that your pipeline could keep crunching even in light of live changes.

# An Example

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
*  [Javadoc](http://outbrain.github.io/Aletheia/)

# Developers
Aletheia is developed by the data infrastructure team in Outbrain. Please contact <datainfrastructure@outbrain.com> for any feedback or questions.

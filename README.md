<img src="/logo/Aletheia-logo.png" height="115"></img>
![](https://github.com/outbrain/Aletheia/blob/master/logo/Aletheia.png)

Aletheia is a library that provides a uniform uniform and extendable way for moving streaming data between endpoints with built in fine-grained visibility.

Aletheia’s key features are:
* Datum-level near real time visibility
* Pluggable endpoint types 
* Pluggable serialization formats

Aletheia's is aiming to be general-purpose data delivery framework.

# Endpoint Types
The following endpoint types are supported out-of-the-box:
* Kafka 0.7 (production and consumption)
* Kafka 0.8 (production and consumption)
* Log files (production only)

Custom endpoint types are easy to write. See the [Wiki](https://github.com/outbrain/Aletheia/wiki) for details.

# Datum Production Example
Build the Datum Producer once:

```java
DatumProducer<Click> datumProducer = 
    DatumProducerBuilder
      .forDomainClass(Click.class)
      .registerProductionEndPointType(KafkaTopicProductionEndPoint.class,
                                      new KafkaDatumEnvelopeSenderFactory())
      .registerProductionEndPointType(LogFileProductionEndPoint.class,
                                      new LogFileDatumEnvelopeSenderFactory())
      .deliverDataTo(new KafkaTopicProductionEndPoint(...))
      .deliverDataTo(new LogFileProductionEndPoint(...))
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
Aletheia is developed by the data infrastructure team in Outbrain. Please contact us for any details:
* Stas Levin - <slevin@outbrain.com>
* Harel Ben-Attia - <harel@outbrain.com>
* Izik Shmulewitz - <ishmulewitz@outbrain.com>

Aletheia
========

Aletheia is a data delivery and consumption framework providing real time monitoring information. It can be used atop existing, as well as custom built messaging systems to seamlessly provide versioned, real time monitored, data delivery and consumption.

Aletheia was designed to address the following concerns:

 1. Real time, easily consumable, monitoring information
 2. Pluggble delivery/consumption components
 3. Data schema version evolution


Delivering data:
-----------------

<pre>
DatumDelivererBuilder
    .forDomainClass(SampleDomainClass.class)
    .deliverBreadcrumbsTo(new InMemoryDeliveryEndPoint<>(InMemoryDeliveryEndPoint.Encoding.String),
                          new BreadcrumbsConfig(Duration.standardSeconds(30), Duration.standardSeconds(30)))
    .deliverDataTo(new InMemoryDeliveryEndPoint<>(InMemoryDeliveryEndPoint.Encoding.DatumEnvelope),
                   new AvroDatumSerDe<>(
                           new SampleDomainClassAvroRoundTripProjector(),
                           new CachedSchemaRepository(new StaticDatumAvroSchemaRepository())),
                   new SampleDomainClassFilter())
    .build(new DatumDelivererConfig("app", "src", "tier", "dc", 1, "originalHostname"));
</pre>

Consuming data:
---------------

<pre>
DatumInboxBuilder
    .forDomainType(SampleDomainClass.class)
    .addDatumSource(new DeliveryEndPoint() {
                      @Override
                      public String getName() {
                        return "NoWhere";
                      }
                    },
                    new AvroDatumSerDe<>(new SampleDomainClassAvroRoundTripProjector(),
                                         new CachedSchemaRepository(new StaticDatumAvroSchemaRepository())))
    .deliverBreadcrumbsTo(new InMemoryDeliveryEndPoint<>(InMemoryDeliveryEndPoint.Encoding.String),
                          new BreadcrumbsConfig(Duration.standardSeconds(30), Duration.standardSeconds(30)))
    .delegateIncomingDataTo(datumHandler)
    .build(new DatumInboxConfig("app", "src", "tier", "dc", 1));
</pre>

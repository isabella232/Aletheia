package com.outbrain.aletheia;

import com.google.common.base.Predicate;
import com.outbrain.aletheia.datum.DatumUtils;
import com.outbrain.aletheia.datum.serialization.DatumSerDe;
import com.outbrain.aletheia.datum.serialization.Json.JsonDatumSerDe;
import com.outbrain.aletheia.datum.serialization.SampleDomainClassAvroRoundTripProjector;
import com.outbrain.aletheia.datum.serialization.avro.AvroDatumSerDe;
import com.outbrain.aletheia.datum.serialization.avro.schema.CachedDatumSchemaRepository;
import com.outbrain.aletheia.datum.serialization.avro.schema.StaticDatumAvroSchemaRepository;
import com.outbrain.aletheia.datum.type.SampleDomainClass;
import org.apache.commons.lang.RandomStringUtils;
import org.joda.time.Instant;
import org.junit.Test;

/**
 * Created by slevin on 8/16/14.
 */
public class SampleDomainClassDatumIntegrationTest extends AletheiaIntegrationTest<SampleDomainClass> {

  private final Predicate<SampleDomainClass> filter = new Predicate<SampleDomainClass>() {
    @Override
    public boolean apply(final SampleDomainClass input) {
      return input.isDiscarded();
    }
  };

  private final AvroDatumSerDe<SampleDomainClass> avroDatumSerDe =
          new AvroDatumSerDe<>(DatumUtils.getDatumTypeId(SampleDomainClass.class),
                               new SampleDomainClassAvroRoundTripProjector(),
                               CachedDatumSchemaRepository.from(new StaticDatumAvroSchemaRepository()));

  private final DatumSerDe<SampleDomainClass> jsonDatumSerDe = new JsonDatumSerDe<>(SampleDomainClass.class);

  public SampleDomainClassDatumIntegrationTest() {
    super(SampleDomainClass.class);
  }

  @Override
  protected SampleDomainClass domainClassRandomDatum(final boolean shouldBeSent) {
    return new SampleDomainClass(random.nextInt(),
                                 random.nextDouble(),
                                 RandomStringUtils.randomAlphanumeric(random.nextInt(20)),
                                 new Instant(),
                                 shouldBeSent);
  }

  @Test
  public void test_whenDeliveringDatumWithAvroSerDe_datumAndBreadcrumbArrive() throws InterruptedException {
    testEnd2End(avroDatumSerDe, filter);
  }

  @Test
  public void test_whenDeliveringDatumWithJsonSerDe_datumAndBreadcrumbArrive() throws InterruptedException {
    testEnd2End(jsonDatumSerDe, filter);
  }
}

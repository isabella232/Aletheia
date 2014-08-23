package com.outbrain.aletheia;

import com.google.common.base.Predicate;
import com.outbrain.aletheia.datum.serialization.SampleClassStringSerDe;
import com.outbrain.aletheia.datum.serialization.SampleDomainClassAvroRoundTripProjector;
import com.outbrain.aletheia.datum.serialization.avro.AvroDatumSerDe;
import com.outbrain.aletheia.datum.serialization.avro.schema.CachedSchemaRepository;
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
      return input.shouldBeSent();
    }
  };

  private final AvroDatumSerDe<SampleDomainClass> avroDatumSerDe =
          new AvroDatumSerDe<>(new SampleDomainClassAvroRoundTripProjector(),
                               new CachedSchemaRepository(new StaticDatumAvroSchemaRepository()));

  private final SampleClassStringSerDe stringSerDe = new SampleClassStringSerDe();

  public SampleDomainClassDatumIntegrationTest() {
    super(SampleDomainClass.class);
  }

  @Override
  protected SampleDomainClass randomDomainClassDatum(final boolean shouldBeSent) {
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
  public void test_whenDeliveringDatumWithStringSerDe_datumAndBreadcrumbArrive() throws InterruptedException {
    testEnd2End(stringSerDe, filter);
  }
}

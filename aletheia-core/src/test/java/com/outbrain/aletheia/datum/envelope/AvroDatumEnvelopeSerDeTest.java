package com.outbrain.aletheia.datum.envelope;

import com.outbrain.aletheia.MyDatum;
import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope;
import com.outbrain.aletheia.datum.envelope.avro.DatumEnvelope_old;
import com.outbrain.aletheia.datum.serialization.Json.JsonDatumSerDe;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.ByteBufferInputStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.UUID;

import static org.apache.avro.io.DecoderFactory.get;
import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class AvroDatumEnvelopeSerDeTest {

    private JsonDatumSerDe<MyDatum> jsonDatumSerDe = new JsonDatumSerDe<>(MyDatum.class);
    private DatumEnvelope datumEnvelope;
    private DatumEnvelope_old datumEnvelopeOld;



    @Before
    public void before() {

        MyDatum myDatum = new MyDatum("sd");
        // new version of datum
        datumEnvelope = new DatumEnvelope();

        datumEnvelope.setDatumUniqueId(UUID.randomUUID().toString());
        datumEnvelope.setDatumTypeId(UUID.randomUUID().toString());
        datumEnvelope.setLogicalTimestamp(11L);
        datumEnvelope.setSourceHost("sdgsd");
        datumEnvelope.setIncarnation(3);
        datumEnvelope.setDatumBytes(jsonDatumSerDe.serializeDatum(myDatum).getPayload());
        datumEnvelope.setEnvelopeVersion(2);


        //old version
        datumEnvelopeOld = new DatumEnvelope_old();
        datumEnvelopeOld.setDatumTypeId(UUID.randomUUID().toString());
        datumEnvelopeOld.setLogicalTimestamp(11L);
        datumEnvelopeOld.setSourceHost("sdgsd");
        datumEnvelopeOld.setIncarnation(3);
        datumEnvelopeOld.setDatumBytes(jsonDatumSerDe.serializeDatum(myDatum).getPayload());
        datumEnvelopeOld.setDatumSchemaVersion(1);
        datumEnvelopeOld.setDatumTypeId(UUID.randomUUID().toString());




    }


    @Test
    public void testSanity() {
        //producer and consume have the same schema
        AvroDatumEnvelopeSerDe serde = new AvroDatumEnvelopeSerDe();

        ByteBuffer byteBuffer = serde.serializeDatumEnvelope(datumEnvelope);
        DatumEnvelope envelopeOutput = serde.deserializeDatumEnvelope(byteBuffer);

        assertEquals(envelopeOutput.getDatumUniqueId().toString(), datumEnvelope.getDatumUniqueId().toString());
        assertEquals(envelopeOutput.getEnvelopeVersion().intValue(), 2);
        assertEquals(datumEnvelope.getEnvelopeVersion().intValue(), 2);

    }


    //old Producer with new Consumer
    @Test
    public void testBackwardsCompatibilty() throws IOException {

        AvroDatumEnvelopeSerDe serde = new AvroDatumEnvelopeSerDe();
        final ByteArrayOutputStream envelopeByteStream = new ByteArrayOutputStream();

        final SpecificDatumWriter<DatumEnvelope_old> envelopeWriter = new SpecificDatumWriter<>(datumEnvelopeOld.getSchema());
        BinaryEncoder binaryEncoder = EncoderFactory.get().directBinaryEncoder(envelopeByteStream, null);
        binaryEncoder = EncoderFactory.get().directBinaryEncoder(envelopeByteStream, binaryEncoder);

        envelopeWriter.write(datumEnvelopeOld, binaryEncoder);
        envelopeByteStream.flush();

        ByteBuffer buffer = ByteBuffer.wrap(envelopeByteStream.toByteArray());
        DatumEnvelope result = serde.deserializeDatumEnvelope(buffer);

        Assert.assertNotNull(result);
        Assert.assertEquals("00000000-0000-0000-0000-000000000000", result.getDatumUniqueId().toString());
    }


    //new Producer old Consumer new version of datum
    @Test
    public void testForwardsCompatibilty() throws IOException {

        AvroDatumEnvelopeSerDe serde = new AvroDatumEnvelopeSerDe();
        ByteBuffer buffer = serde.serializeDatumEnvelope(datumEnvelope);



        int version = buffer.getInt(); //must do
        DatumReader<DatumEnvelope_old> reader = new SpecificDatumReader<>(DatumEnvelope.getClassSchema(), DatumEnvelope_old.getClassSchema());

        InputStream byteBufferInputStream = new ByteBufferInputStream(Collections.singletonList(buffer));
        DatumEnvelope_old result = reader.read(null, get().directBinaryDecoder(byteBufferInputStream, null));

        Assert.assertEquals(version, 2);
        Assert.assertNotNull(result);



    }

}

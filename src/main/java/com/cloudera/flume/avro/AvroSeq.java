package com.cloudera.flume.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;

public class AvroSeq extends AbstractSource implements PollableSource {

	private Schema schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"SequenceNumber\",\"namespace\":\"com.cloudera.flume.avro\",\"fields\":[{\"name\":\"seq\",\"type\":\"int\"}]}");

	public byte[] toBytes(Object object) throws IOException {
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		Encoder encoder = EncoderFactory.get().binaryEncoder(output, null);
		GenericDatumWriter<Object> datumWriter = null;
		datumWriter = new GenericDatumWriter<Object>(schema);
		datumWriter.write(object, encoder);
		encoder.flush();
		return output.toByteArray();
	}

	private int i;
	private ChannelProcessor channel;
	private Map<String, String> headers;
	private GenericRecord datum;
	
	@Override
	public void start() {
		channel = getChannelProcessor();
		headers = new HashMap<String, String>();
		datum = new GenericData.Record(schema);
		super.start();
	}

	@Override
	public Status process() throws EventDeliveryException {
		Status status = null;
		datum.put("seq", i++);
		try {
			byte [] bytes = toBytes(datum);
			Event event = EventBuilder.withBody(bytes, headers);
			channel.processEvent(event);
			status = Status.READY;
		} catch (IOException e) {
			status = Status.BACKOFF;
			throw new EventDeliveryException(e);
		}
		return status;
	}

}

package com.cloudera.flume.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.lang.SerializationException;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

public class AvroInterceptor implements Interceptor {

	private Schema schema;
	private BinaryDecoder decoder;
	private GenericDatumReader<GenericRecord> reader;
	private GenericRecord record;
	
	public AvroInterceptor(Schema schema) {
		this.schema = schema;
	}
	
	@Override
	public void initialize() {
		reader = new GenericDatumReader<GenericRecord>(schema);
	}

	private GenericRecord toGenericRecord(byte[] bytes) {
		decoder = DecoderFactory.get().binaryDecoder(bytes, decoder);
		try {
			reader = new GenericDatumReader<GenericRecord>(schema);
			record = reader.read(record, decoder);
			return record;
		} catch(IOException e) {
			throw new SerializationException(e);
		}
	}
	
	public byte[] toBytes(GenericRecord record) throws IOException {
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		Encoder encoder = EncoderFactory.get().binaryEncoder(output, null);
		GenericDatumWriter<GenericRecord> datumWriter = null;
		datumWriter = new GenericDatumWriter<GenericRecord>(schema);
		datumWriter.write(record, encoder);
		encoder.flush();
		return output.toByteArray();
	}
	
	@Override
	public Event intercept(Event event) {
		GenericRecord record = toGenericRecord(event.getBody());
		record.put("seq", 2 * (Integer) record.get("seq"));
		try {
			event.setBody(toBytes(record));
			return event;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public List<Event> intercept(List<Event> events) {
		for(Event event : events) {
			intercept(event);
		}
		return events;
	}

	@Override
	public void close() {
		// NOP
	}

}

package com.cloudera.flume.avro;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.lang.SerializationException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.AbstractAvroEventSerializer;
import org.apache.flume.serialization.EventSerializer;

public class AvroSerializer extends AbstractAvroEventSerializer<GenericRecord> {

	private Schema schema;
	private final OutputStream out;

	private AvroSerializer(OutputStream out) {
		this.out = out;
	}

	@Override
	protected Schema getSchema() {
		return schema;
	}

	@Override
	protected OutputStream getOutputStream() {
		return out;
	}

	private BinaryDecoder decoder;
	private GenericDatumReader<GenericRecord> reader;
	private GenericRecord record;
	
	@Override
	public void configure(Context context) {
		schema = new Schema.Parser().parse(context.getString("schema"));
		reader = new GenericDatumReader<GenericRecord>(schema);
		super.configure(context);
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
	
	@Override
	protected GenericRecord convert(Event event) {
		return toGenericRecord(event.getBody());
	}

	public static class Builder implements EventSerializer.Builder {

		@Override
		public EventSerializer build(Context context, OutputStream out) {
			AvroSerializer serializer = new AvroSerializer(out);
			serializer.configure(context);
			return serializer;
		}

	}

}

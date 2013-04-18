package com.cloudera.flume.avro;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.lang.SerializationException;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

public class AvroConsole extends AbstractSink implements Configurable {

	private Schema schema;
	private BinaryDecoder decoder;
	private GenericDatumReader<GenericRecord> reader;
	private GenericRecord record;
	
	private GenericRecord toGenericRecord(byte[] bytes) {
		decoder = DecoderFactory.get().binaryDecoder(bytes, decoder);
		try {
			record = reader.read(record, decoder);
			return record;
		} catch(IOException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public void configure(Context context) {
		schema = new Schema.Parser().parse(context.getString("schema"));
		reader = new GenericDatumReader<GenericRecord>(schema);
	}

	@Override
	public void start() {
		super.start();
	}

	@Override
	public Status process() throws EventDeliveryException {
		Status status = null;
		Channel ch = getChannel();
		Transaction txn = ch.getTransaction();
		txn.begin();
		try {
			Event event = ch.take();
			System.out.println(toGenericRecord(event.getBody()));
			txn.commit();
			status = Status.READY;
		} catch (Throwable t) {
			txn.rollback();
			t.printStackTrace();
			status = Status.BACKOFF;
			if (t instanceof Error) {
				throw (Error)t;
			}
		} finally {
			txn.close();
		}
		return status;
	}

}

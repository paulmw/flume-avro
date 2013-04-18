package com.cloudera.flume.avro;

import org.apache.avro.Schema;
import org.apache.flume.Context;
import org.apache.flume.interceptor.Interceptor;

public class AvroInterceptorBuilder implements Interceptor.Builder {

	private Schema schema;
	
	@Override
	public void configure(Context context) {
		schema = new Schema.Parser().parse(context.getString("schema"));
		
	}

	@Override
	public Interceptor build() {
		return new AvroInterceptor(schema);
	}

}
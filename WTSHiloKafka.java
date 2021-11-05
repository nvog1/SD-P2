



Properties props = new Properties();
		props.put("bootstrap.servers", "broker1:9092,broker2:9092");
		props.put("group.id", "CountryCounter");
		props.put("key.deserializer",
			"org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer",
			"org.apache.kafka.common.serialization.StringDeserializer");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
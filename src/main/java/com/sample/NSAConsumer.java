package com.sample;


import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

public class NSAConsumer {

    private final static String BOOTSTRAP_SERVERS="gbl11767.systems.uk.hsbc:9092";

    public static void main(String[] args) throws Exception
    {
        Properties props = new Properties();

        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG,"G11171avin");
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroDeserializer");
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroDeserializer");

        props.put("security.protocol", "SSL");
        props.put("max.poll.records","100");
        props.put("schema.registry.url","http://gbl11767.systems.uk.mp:8081/api/v1");
        props.put("auto.offset.reset", "earliest");


        final Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList("ITEMPF"));
        while (true) {
            final ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(1000);
            // Decode
            consumerRecords.forEach(record1 -> {
                DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
                System.out.println("DECODE reader: " + reader);
                ByteArrayInputStream is = new ByteArrayInputStream(record1.value());
                System.out.println("DECODE is: " + is);
                try {
                    DataFileStream<GenericRecord> dataFileReader = new DataFileStream<GenericRecord>(is, reader);
                    System.out.println("DECODE dataFileReader: " + dataFileReader);

                    GenericRecord record = null;
                    while (dataFileReader.hasNext()) {
                        record = dataFileReader.next(record);
                        System.out.println(record.getSchema());
                        System.out.println(record.toString());
                    }
                }  catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
    }

}

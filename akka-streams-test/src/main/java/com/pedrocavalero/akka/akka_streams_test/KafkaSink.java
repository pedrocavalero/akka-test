package com.pedrocavalero.akka.akka_streams_test;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import akka.Done;
import akka.actor.ActorSystem;
import akka.kafka.ProducerSettings;
import akka.kafka.javadsl.Producer;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Source;

public class KafkaSink {

	public static void main(String[] args) {
		final ActorSystem system = ActorSystem.create("QuickStart");
		final Materializer materializer = ActorMaterializer.create(system);
		final ProducerSettings<byte[], String> producerSettings = ProducerSettings
		  .create(system, new ByteArraySerializer(), new StringSerializer())
		  .withBootstrapServers("localhost:9092");
		CompletionStage<Done> done =
				  Source.range(1, 100)
				    .map(n -> n.toString()).map(elem -> new ProducerRecord<byte[], String>("test", elem))
				    .runWith(Producer.plainSink(producerSettings), materializer);
	}

}

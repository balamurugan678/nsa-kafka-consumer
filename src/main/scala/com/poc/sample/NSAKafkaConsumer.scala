package com.poc.sample

import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.util.Timeout
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroDeserializer
import org.apache.kafka.clients.consumer.ConsumerConfig

import scala.concurrent.duration._

object NSAKafkaConsumer {

  def main(args: Array[String]): Unit = {

    implicit val system = ActorSystem("NSAKafkaConsumer")
    implicit val materializer = ActorMaterializer()
    implicit val timeout = Timeout(5 seconds)

    val consumerSettings = ConsumerSettings(system, new KafkaAvroDeserializer, new KafkaAvroDeserializer)
      .withBootstrapServers("localhost:9092")
      .withGroupId("G11171avin")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .withProperty("schema.registry.url", "http://sample.systems.uk.maplequad:8081/api/vi")
      .withProperty("security.protocol","SASL_PLAINTEXT")
      .withProperty("sasl.kerberos.service.name","kafka")

    Consumer.committableSource(consumerSettings, Subscriptions.topics("spglobal.stream"))
      .map(msg => {
        println("MSG"+msg.record.value())
        "Hi"
      })
      .runWith(Sink.ignore)
  }


}

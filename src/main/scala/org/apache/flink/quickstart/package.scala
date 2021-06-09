package org.apache.flink

import org.apache.flink.EventParameters.EventParameterRule
import org.apache.flink.Types.{consumerProperties, producerProperties}
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation}
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaConsumerBase, FlinkKafkaProducer011}
import org.apache.flink.streaming.util.serialization.{KeyedDeserializationSchema, KeyedSerializationSchema}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig

import java.nio.charset.StandardCharsets
import java.time.LocalDate
import java.util.{Optional, Properties}

package object Types {

  val consumerProperties = new Properties()
  consumerProperties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:9092")
  consumerProperties.setProperty(FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, "60000")
  consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "validation-broadcast")

  val producerProperties = new Properties()
  producerProperties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:9092")
  producerProperties.setProperty(FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, "60000")
  producerProperties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")

}

package object OrganizationClosureDate {

  case class DTO(code: String, closureDate: Option[LocalDate])

  class KafkaConsumer extends FlinkKafkaConsumer011[DTO](
    "organization-closure-date",
    new DeserializationSchema,
    consumerProperties)

  class DeserializationSchema extends KeyedDeserializationSchema[DTO]() {

    override def deserialize(messageKey: Array[Byte], message: Array[Byte], topic: String, partition: Int, offset: Long): DTO = {
      Option(message)
        .map(new String(_, StandardCharsets.UTF_8))
        .map(DTO(_, Some(LocalDate.now())))
        .orNull
    }

    override def isEndOfStream(dto: DTO): Boolean = false

    override def getProducedType: TypeInformation[DTO] = TypeInformation.of(classOf[DTO])

  }

  class BroadcastStateDescriptor extends MapStateDescriptor[String, DTO](
    "broadcastClosureDateDesc",
    BasicTypeInfo.STRING_TYPE_INFO,
    TypeInformation.of(new TypeHint[DTO] {}))

}

package object EventParameters {

  case class DTO(organization: String, rules: Seq[EventParameterRule])

  case class EventParameterRule(context: String)

  class KafkaConsumer extends FlinkKafkaConsumer011[DTO](
    "organization-parameters",
    new DeserializationSchema,
    consumerProperties)

  class DeserializationSchema extends KeyedDeserializationSchema[DTO]() {

    override def deserialize(messageKey: Array[Byte], message: Array[Byte], topic: String, partition: Int, offset: Long): DTO = {
      Option(message)
        .map(new String(_, StandardCharsets.UTF_8).split("-"))
        .map(arr => DTO(arr(0), Seq(EventParameterRule(arr(1)))))
        .orNull
    }

    override def isEndOfStream(dto: DTO): Boolean = false

    override def getProducedType: TypeInformation[DTO] = TypeInformation.of(classOf[DTO])

  }

  class BroadcastStateDescriptor extends MapStateDescriptor[String, DTO](
    "paramsBroadcastState",
    BasicTypeInfo.STRING_TYPE_INFO,
    TypeInformation.of(new TypeHint[DTO] {}))

}

package object Event {

  case class DTO(organization: String,
                 value: String,
                 parameters: Option[Seq[EventParameterRule]] = None,
                 valid: Boolean = false,
                 error: Option[String] = None)

  class KafkaConsumer extends FlinkKafkaConsumer011[DTO](
    "event",
    new DeserializationSchema,
    consumerProperties)

  class KafkaProducer(topicId: String) extends FlinkKafkaProducer011[DTO](
    topicId,
    new SerializationSchema(topicId),
    producerProperties,
    Optional.empty[FlinkKafkaPartitioner[DTO]]
  )

  class KafkaProducerFailure extends KafkaProducer("output-failure")

  class KafkaProducerSuccess extends KafkaProducer("output-success")

  class DeserializationSchema extends KeyedDeserializationSchema[DTO]() {

    override def deserialize(messageKey: Array[Byte], message: Array[Byte], topic: String, partition: Int, offset: Long): DTO = {
      Option(message)
        .map(new String(_, StandardCharsets.UTF_8).split("-"))
        .map(arr => DTO(arr(0), arr(1)))
        .orNull
    }

    override def isEndOfStream(dto: DTO): Boolean = false

    override def getProducedType: TypeInformation[DTO] = TypeInformation.of(classOf[DTO])

  }

  class SerializationSchema(topicId: String) extends KeyedSerializationSchema[DTO]() {

    override def serializeKey(dto: DTO): Array[Byte] = "".getBytes

    override def serializeValue(dto: DTO): Array[Byte] =
      s"""{"organization":"${dto.organization}","parameters":"${dto.parameters.mkString("|")}","value": "${dto.value}","valid":${dto.valid}""".stripMargin.getBytes(StandardCharsets.UTF_8)

    override def getTargetTopic(dto: DTO): String = topicId

  }

}
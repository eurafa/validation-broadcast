package org.apache.flink.quickstart

import org.apache.flink.Event.{KafkaProducerFailure, KafkaProducerSuccess}
import org.apache.flink.EventParameters.EventParameterRule
import org.apache.flink.streaming.api.scala._
import org.apache.flink.{Event, EventParameters, OrganizationClosureDate}

import java.time.LocalDate

object StreamingJobLoadDefaultValues {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(10)

    val organizationClosureDateSource = new OrganizationClosureDate.KafkaConsumer
    val value1 = env.fromCollection(List(
      OrganizationClosureDate.DTO("2020", Some(LocalDate.now())))
    )
    val organizationClosureDateStream = env
      .addSource(organizationClosureDateSource)
      .union(value1)
    val organizationBroadcastDescriptor = new OrganizationClosureDate.BroadcastStateDescriptor
    val organizationClosureDateBroadcastStream = organizationClosureDateStream.broadcast(organizationBroadcastDescriptor)

    val eventParametersBroadcastDescriptor = new EventParameters.BroadcastStateDescriptor
    val value2 = env.fromCollection(List(
      EventParameters.DTO("2020", Seq(EventParameterRule("cc"))))
    )
    val parametersBroadcastStream = env
      .addSource(new EventParameters.KafkaConsumer)
      .union(value2)
      .broadcast(eventParametersBroadcastDescriptor)

    val outputTag = new OutputTag[Event.DTO]("eventsNotValidated")

    val eventValidatedStream = env
      .addSource(new Event.KafkaConsumer)
      .keyBy(value => value.organization)
      .connect(organizationClosureDateBroadcastStream)
      .process(new EventOrganizationClosureDateBroadcastProcessFunction(organizationBroadcastDescriptor))
      .keyBy(value => value.organization)
      .connect(parametersBroadcastStream)
      .process(new EventOrganizationParameterBroadcastProcessFunction(eventParametersBroadcastDescriptor, outputTag))
      .keyBy(value => value.organization)
      .process(new EventValidationProcessFunction(outputTag))

    eventValidatedStream
      .addSink(new KafkaProducerSuccess)

    eventValidatedStream
      .getSideOutput(outputTag)
      .addSink(new KafkaProducerFailure)

    env.execute("Flink Streaming Scala API Skeleton")
  }

}

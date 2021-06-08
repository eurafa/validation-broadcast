package org.apache.flink.quickstart

import org.apache.flink.Event.{KafkaProducerFailure, KafkaProducerSuccess}
import org.apache.flink.EventParameters.EventParameterRule
import org.apache.flink.streaming.api.scala._
import org.apache.flink.{Event, EventParameters, OrganizationClosureDate}

import java.time.LocalDate

object StreamingJob {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(10)

    val organizationClosureDateSource = new OrganizationClosureDate.KafkaConsumer
    val organizationBroadcastDescriptor = new OrganizationClosureDate.BroadcastDescriptor

    val value1 = env.fromCollection(List(
      OrganizationClosureDate.DTO("1010", Some(LocalDate.now())),
      OrganizationClosureDate.DTO("2020", Some(LocalDate.now())))
    )
    val organizationClosureDateStream = env
      .addSource(organizationClosureDateSource)
      .union(value1)
    val organizationClosureDateBroadcastStream = organizationClosureDateStream.broadcast(organizationBroadcastDescriptor)

    val eventParametersBroadcastDescriptor = new EventParameters.BroadcastDescriptor

    val value2 = env.fromCollection(List(
      EventParameters.DTO("1010", Seq(EventParameterRule("rafa"))),
      EventParameters.DTO("2020", Seq(EventParameterRule("rafael"))))
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
      .process(new EventOrganizationClosureDateBroadcastProcessFunction(organizationBroadcastDescriptor, outputTag))
      .keyBy(value => value.organization)
      .connect(parametersBroadcastStream)
      .process(new EventOrganizationParameterBroadcastProcessFunction(eventParametersBroadcastDescriptor))
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

package org.apache.flink.quickstart

import org.apache.flink.Event.{KafkaProducerFailure, KafkaProducerSuccess}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.{Event, EventParameters, OrganizationClosureDate}

object StreamingJob {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(10)

    val organizationClosureDateStream = env.addSource(new OrganizationClosureDate.KafkaConsumer)
    val organizationBroadcastStateDescriptor = new OrganizationClosureDate.BroadcastStateDescriptor
    val organizationClosureDateBroadcastStream = organizationClosureDateStream.broadcast(organizationBroadcastStateDescriptor)

    val eventParametersBroadcastStateDescriptor = new EventParameters.BroadcastStateDescriptor
    val parametersBroadcastStream = env
      .addSource(new EventParameters.KafkaConsumer)
      .broadcast(eventParametersBroadcastStateDescriptor)

    val outputTag = new OutputTag[Event.DTO]("eventsNotValidated")

    val eventValidatedStream = env
      .addSource(new Event.KafkaConsumer)
      .keyBy(value => value.organization)
      .connect(organizationClosureDateBroadcastStream)
      .process(new EventOrganizationClosureDateBroadcastProcessFunction(organizationBroadcastStateDescriptor))
      .keyBy(value => value.organization)
      .connect(parametersBroadcastStream)
      .process(new EventOrganizationParameterBroadcastProcessFunction(eventParametersBroadcastStateDescriptor, outputTag))
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

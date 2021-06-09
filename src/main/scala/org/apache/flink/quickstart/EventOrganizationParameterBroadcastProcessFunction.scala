package org.apache.flink.quickstart

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.{Collector, OutputTag}
import org.apache.flink.{Event, EventParameters}

class EventOrganizationParameterBroadcastProcessFunction(broadcastStateDescriptor: MapStateDescriptor[String, EventParameters.DTO],
                                                         outputTag: OutputTag[Event.DTO]) extends KeyedBroadcastProcessFunction[String, Event.DTO, EventParameters.DTO, Event.DTO] {

  override def processElement(event: Event.DTO, context: KeyedBroadcastProcessFunction[String, Event.DTO, EventParameters.DTO, Event.DTO]#ReadOnlyContext, collector: Collector[Event.DTO]): Unit = {
    val parameters = context
      .getBroadcastState(broadcastStateDescriptor)
      .get(context.getCurrentKey)

    if (event.error.isDefined) {
      context.output(outputTag, event)
    } else {
      collector.collect(event.copy(parameters = Option(parameters).map(_.rules)))
    }
  }

  override def processBroadcastElement(parameters: EventParameters.DTO, context: KeyedBroadcastProcessFunction[String, Event.DTO, EventParameters.DTO, Event.DTO]#Context, collector: Collector[Event.DTO]): Unit = {
    context
      .getBroadcastState(broadcastStateDescriptor)
      .put(parameters.organization, parameters)
  }

}

package org.apache.flink.quickstart

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.{Event, EventParameters}

class EventOrganizationParameterBroadcastProcessFunction(broadcastStateDescriptor: MapStateDescriptor[String, EventParameters.DTO]) extends KeyedBroadcastProcessFunction[String, Event.DTO, EventParameters.DTO, Event.DTO] {

//  private val state = new MapStateDescriptor[String, DTO](
  //    "paramsBroadcastState",
  //    BasicTypeInfo.STRING_TYPE_INFO,
  //    TypeInformation.of(new TypeHint[DTO] {}))
  //
  //  override def open(parameters: Configuration): Unit = {
  //    getRuntimeContext.getMapState(state).put("1010", DTO("1010", Seq(EventParameterRule("rafael"))))
  //  }
  //
  //  override def close(): Unit = getRuntimeContext.getMapState(state).clear()

  override def processElement(event: Event.DTO, readOnlyContext: KeyedBroadcastProcessFunction[String, Event.DTO, EventParameters.DTO, Event.DTO]#ReadOnlyContext, collector: Collector[Event.DTO]): Unit = {
    val parameters = readOnlyContext
      .getBroadcastState(broadcastStateDescriptor)
      .get(readOnlyContext.getCurrentKey)

    collector.collect(event.copy(parameters = Option(parameters).map(_.rules)))
  }

  override def processBroadcastElement(parameters: EventParameters.DTO, context: KeyedBroadcastProcessFunction[String, Event.DTO, EventParameters.DTO, Event.DTO]#Context, collector: Collector[Event.DTO]): Unit = {
    context
      .getBroadcastState(broadcastStateDescriptor)
      .put(parameters.organization, parameters)
  }

}

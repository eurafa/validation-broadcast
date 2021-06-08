package org.apache.flink.quickstart

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.{Collector, OutputTag}
import org.apache.flink.{Event, OrganizationClosureDate}

class EventOrganizationClosureDateBroadcastProcessFunction(broadcastStateDescriptor: MapStateDescriptor[String, OrganizationClosureDate.DTO],
                                                           outputTag: OutputTag[Event.DTO])
  extends KeyedBroadcastProcessFunction[String, Event.DTO, OrganizationClosureDate.DTO, Event.DTO] {

  //  private val state = new MapStateDescriptor[String, DTO](
  //    "broadcastClosureDateDesc",
  //    BasicTypeInfo.STRING_TYPE_INFO,
  //    TypeInformation.of(new TypeHint[DTO] {}))
  //
  //  override def open(parameters: Configuration): Unit = {
  //    getRuntimeContext.getMapState(state).put("1010", DTO("1010", Some(LocalDate.now())))
  //  }
  //
  //  override def close(): Unit = getRuntimeContext.getMapState(state).clear()

  override def processElement(event: Event.DTO, context: KeyedBroadcastProcessFunction[String, Event.DTO, OrganizationClosureDate.DTO, Event.DTO]#ReadOnlyContext, collector: Collector[Event.DTO]): Unit = {
    val closureDateTuple = context
      .getBroadcastState(broadcastStateDescriptor)
      .get(context.getCurrentKey)

    if (closureDateTuple != null && closureDateTuple.closureDate.isDefined) {
      collector.collect(event)
    } else {
      val error = s"Event with value ${event.value} was discarded because organization ${event.organization} is not configured"
      context.output(outputTag, event.copy(error = Some(error)))
    }
  }

  override def processBroadcastElement(closureDateTuple: OrganizationClosureDate.DTO, context: KeyedBroadcastProcessFunction[String, Event.DTO, OrganizationClosureDate.DTO, Event.DTO]#Context, collector: Collector[Event.DTO]): Unit = {
    context
      .getBroadcastState(broadcastStateDescriptor)
      .put(closureDateTuple.code, closureDateTuple)
  }

}

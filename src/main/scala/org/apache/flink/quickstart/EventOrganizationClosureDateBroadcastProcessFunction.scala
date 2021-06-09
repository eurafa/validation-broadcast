package org.apache.flink.quickstart

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.{Collector, OutputTag}
import org.apache.flink.{Event, OrganizationClosureDate}

class EventOrganizationClosureDateBroadcastProcessFunction(broadcastStateDescriptor: MapStateDescriptor[String, OrganizationClosureDate.DTO])
  extends KeyedBroadcastProcessFunction[String, Event.DTO, OrganizationClosureDate.DTO, Event.DTO] {

  override def processElement(event: Event.DTO, context: KeyedBroadcastProcessFunction[String, Event.DTO, OrganizationClosureDate.DTO, Event.DTO]#ReadOnlyContext, collector: Collector[Event.DTO]): Unit = {
    val closureDateTuple = context
      .getBroadcastState(broadcastStateDescriptor)
      .get(context.getCurrentKey)

    if (Option(closureDateTuple).flatMap(_.closureDate).isDefined) {
      collector.collect(event)
    } else {
      val error = s"Event with value ${event.value} was discarded because organization ${event.organization} is not configured"
      collector.collect(event.copy(error = Some(error)))
    }
  }

  override def processBroadcastElement(closureDateTuple: OrganizationClosureDate.DTO, context: KeyedBroadcastProcessFunction[String, Event.DTO, OrganizationClosureDate.DTO, Event.DTO]#Context, collector: Collector[Event.DTO]): Unit = {
    context
      .getBroadcastState(broadcastStateDescriptor)
      .put(closureDateTuple.code, closureDateTuple)
  }

}

package org.apache.flink.quickstart

import org.apache.flink.Event
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.{Collector, OutputTag}

class EventValidationProcessFunction(outputTag: OutputTag[Event.DTO]) extends KeyedProcessFunction[String, Event.DTO, Event.DTO] {

  override def processElement(event: Event.DTO, context: KeyedProcessFunction[String, Event.DTO, Event.DTO]#Context, collector: Collector[Event.DTO]): Unit = {
    if (event.parameters.isDefined && !event.parameters.get.isEmpty) {
      if (event.parameters.get.map(_.context).contains(event.value)) {
        collector.collect(event.copy(valid = true))
      } else {
        context.output(outputTag, event)
      }
    } else {
      val error = s"Event with value ${event.value} was discarded because ${event.organization} parameters does not match with ${event.parameters.mkString("|")}"
      context.output(outputTag, event.copy(error = Some(error)))
    }
  }

}

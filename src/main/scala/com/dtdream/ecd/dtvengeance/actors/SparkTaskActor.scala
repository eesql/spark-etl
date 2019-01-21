package com.dtdream.ecd.dtvengeance.actors

import akka.actor.{Actor, ActorLogging}
import com.dtdream.ecd.dtvengeance.etl.Routes
import com.dtdream.ecd.dtvengeance.utils.AkkaMessages.TaskMessage


class SparkTaskActor extends Actor with ActorLogging {

  override def preStart(): Unit = {}


  override def receive = {
    case msg:TaskMessage =>

      val task = Routes.routes.get(msg.name)
      //TODO: add businessDate process using msg.businessDate

      task match {
        case Some(t) => t.task.process(t.config)
        case None => println("did not work!")
      }
  }

  override def postStop(): Unit = super.postStop()

}

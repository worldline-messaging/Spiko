package com.worldline.mts.fifo


private[fifo] object FifoProtocol {
	sealed trait Command
	sealed trait Request extends Command
	sealed trait Response extends Command
	
	final case class CommandSuccess() extends Response
	final case class CommandFailure(errorCode:Int) extends Response
	final case class CreateQueue(name: String, maxSize: Int = -1) extends Request
	final case class DropQueue(name: String) extends Request
	final case class AddMessage(queue: String, message: Array[Byte]) extends Request
	final case class PollMessage(queue: String) extends Request
	final case class PeekMessage(queue: String) extends Request
	final case class GetSize(queue: String) extends Request
}

package multipaxos

import com.google.protobuf.ByteString
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel

data class TotallyOrderedMessage<T>(
    val sequenceNo: Int,
    val message: T,
)

abstract class AtomicBroadcast<T>(
    protected val learner: LearnerService,
    public val biSerializer: ByteStringBiSerializer<T>,
    receiveCapacityBufferSize: Int = 100,
) {
    private var `sequence#` = 0

    private val chan = Channel<TotallyOrderedMessage<T>>(receiveCapacityBufferSize)

    public val stream: ReceiveChannel<TotallyOrderedMessage<T>>
        get() = chan

    init {
        learner.observers += { seq, serialized ->
            // Note: This ignores empty byte strings
            if (serialized.size() == 0) {
                return@`+=`
            }

            for (msg in _deliver(serialized)) {
                `sequence#`++
                chan.send(TotallyOrderedMessage(`sequence#`, msg))
            }

        }
    }

    public suspend fun send(msg: T) {
        this._send(biSerializer(msg))
    }

    public suspend fun receive(): TotallyOrderedMessage<T> = stream.receive()

    // ToDo for Students:
    // This function should send the message to a ordered / sequencer
    // The orderer / sequencer should invoke a MultiPaxos instance
    //
    // Note that you need to check for the delivery of the message, since the
    // orderer / sequencer crash. In such case, be sure to retransmit the message
    // to the new sequencer (also make sure it not received twice)
    //
    // Do not send an empty byte string, it will be ignored by MultiPaxos
    abstract suspend fun _send(byteString: ByteString)

    // ToDo for Students:
    // This function should reconstruct the object(s) for delivery from the
    // binary string that was returned from a MultiPaxos Instance
    //
    // Do not send an empty byte string, it will be ignored by MultiPaxos,
    // and you will not receive it here
    abstract fun _deliver(byteString: ByteString): List<T>
}

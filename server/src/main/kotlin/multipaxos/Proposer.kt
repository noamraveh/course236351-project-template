package multipaxos

import com.google.protobuf.ByteString
import cs236351.multipaxos.*
import io.grpc.ManagedChannel
import io.grpc.StatusException
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.SendChannel
import kotlin.coroutines.CoroutineContext


import cs236351.multipaxos.MultiPaxosAcceptorServiceGrpcKt.MultiPaxosAcceptorServiceCoroutineStub as AcceptorGrpcStub
import cs236351.multipaxos.MultiPaxosLearnerServiceGrpcKt.MultiPaxosLearnerServiceCoroutineStub as LearnerGrpcStub

typealias ID = Int

class Proposer(
    private val id: ID,
    acceptors: Map<ID, ManagedChannel>,
    private val thisLearner: LearnerService,
    private val omegaFD: OmegaFailureDetector<Int>,
    private val scope: CoroutineScope,
    private val context: CoroutineContext = paxosThread,
    proposalCapacityBufferSize: Int = 10,
) {
    private val acceptors: Map<ID, AcceptorGrpcStub> = (acceptors.mapValues { (_, v) ->
        AcceptorGrpcStub(v)
    })

    private val leaderWaiter = object {
        var chan = Channel<Unit>(1)

        init {
            omegaFD.addWatcher { chan.send(Unit) }
        }

        suspend fun waitUntilLeader() {
            do {
                if (omegaFD.leader == id) return
                chan.receive()
            } while (true)
        }
    }

    private val quorumWaiter: QuorumWaiter<ID, AcceptorGrpcStub> =
        MajorityQuorumWaiter(this.acceptors, scope, context)

    private val acceptorsSize = acceptors.size

    private val proposalsStream = Channel<ByteString>(proposalCapacityBufferSize)
    public val proposalSendStream: SendChannel<ByteString> = proposalsStream
    public suspend fun addProposal(proposal: ByteString) = proposalSendStream.send(proposal)

    public fun start() = scope.launch(context) {
        for (proposal in proposalsStream) {
            val instanceNo = thisLearner.lastInstance.get() + 1
            Instance(instanceNo, proposal).run()
        }
    }

/*    suspend fun doLeader1(iNo: Int, propsal: ByteString) {
        var r = RoundNo(0, id)
        while (true) {
            r++
            val prepareMsg = prepare {
                roundNo = r.toProto()
                instanceNo = iNo
                value = propsal
            }
            //println("$iNo\n$prepareMsg")
            val promises = preparePromiseStage(prepareMsg)
            val (ok, v) = extract(promises)
            //println("$iNo\n$promises")
            if (!ok) {
                println(promises)
                print("NO Promise")
                continue
            }
            val accepts = acceptAcceptedStage(r, v, iNo)
            //println("$iNo\n$accepts")
            val ok2 =
                accepts.filterNotNull().filter { it.ack == Ack.YES }.size > acceptorsSize / 2.0
            if (!ok2) {
                continue
            }
            val commitMsg = commit {
                if (v != null) {
                    value = v
                }
                instanceNo = iNo
            }
//            learners.map {
//                scope.async(paxosThread) {
//                    try {
//                        it.doCommit(commitMsg)
//                    } catch (e: StatusException) {
//                        println(e)
//                        null
//                    }
//                }
//            }.awaitAll()
            thisLearner.doCommit(commitMsg)
            return
        }
    }*/

/*

    private suspend fun acceptAcceptedStage(
        r: RoundNo,
        v: ByteString?,
        iNo: Int,
    ): List<Accepted?> {
        val acceptMsg = accept {
            roundNo = r.toProto()
            if (v != null) {
                value = v
            }
            instanceNo = iNo
        }

        val accepts = acceptors.map {
            scope.async(paxosThread) {
                try {
                    it.doAccept(acceptMsg)
                } catch (e: StatusException) {
                    println(e)
                    null
                }
            }
        }.awaitAll()
        return accepts
    }
*/

/*    private suspend fun preparePromiseStage(prepareMsg: Prepare) = acceptors.map {
        scope.async(paxosThread) {
            try {
                it.doPrepare(prepareMsg)
            } catch (e: StatusException) {
                println(e)
                null
            }
        }
    }.awaitAll()
*/

/*    private fun extract(promises: List<Promise?>): Pair<Boolean, ByteString?> {
        var max = RoundNo(0, 0)
        var v: ByteString? = null
        var count = 0

        for (promise in promises.filterNotNull()) {
            if (promise.ack == Ack.YES) count++
            if (max < RoundNo(promise.roundNo)) {
                max = RoundNo(promise.roundNo)
                v = promise.value
            }
        }
        return Pair(count > acceptorsSize / 2.0, v)
    }
    */

    private inner class Instance(
        val instanceNo: Int,
        var value: ByteString,
    ) {
        internal suspend fun run() {
            while (true) {
                val success = doRound()
                if (success) {
                    return
                }
            }
        }

        private var roundNo = RoundNo(1, id)
        private suspend fun doRound(): Boolean {
            roundNo++
            var (ok, v) = preparePromise()
            if (!ok) return false
            v?.let { value = it }

            ok = acceptAccepted()
            if (!ok) return false

            commit()
            return true
        }


        private suspend fun preparePromise(): Pair<Boolean, ByteString> {
            val prepareMsg = prepare {
                roundNo = this@Instance.roundNo.toProto()
                instanceNo = this@Instance.instanceNo
                value = this@Instance.value
            }.also {
                println("Proposer [$instanceNo, $roundNo]" +
                        "\tPrepare: value=\"${it.value?.toStringUtf8() ?: "null"}\"\n===")
            }
            val (ok, results) = quorumWaiter.waitQuorum({ (_, it) -> it.ack == Ack.YES }) {
                try {
                    this.doPrepare(prepareMsg)
                        .also {
                            "Proposer [$instanceNo, $roundNo]" +
                                    println("\tPromise: ${it.ack} value=\"${
                                        it.value?.let { if (it.size() == 0) it.toStringUtf8() else "null" }
                                    }\"\n\t\t lastgoodround=${RoundNo(it.goodRoundNo)}\n===")
                        }
                } catch (e: StatusException) {
                    null
                }
            }
            val promises = results.map { it.second }.filterNotNull()
            return Pair(ok, if (ok) maxByRoundNo(promises) else EMPTY_BYTE_STRING)
            //println("$iNo\n$promises")
        }

        private suspend fun acceptAccepted(): Boolean {
            val acceptMsg = accept {
                roundNo = this@Instance.roundNo.toProto()
                value = this@Instance.value
                instanceNo = this@Instance.instanceNo
            }.also {
                println("Proposer [$instanceNo, $roundNo]" +
                        "\tAccept: value=\"${it.value?.let { if (it.size() == 0) it.toStringUtf8() else "null" }}\"\n===")
            }
            val (ok, _) = quorumWaiter.waitQuorum({ (_, it) -> it.ack == Ack.YES }) {
                try {
                    this.doAccept(acceptMsg).also {
                        "Proposer [$instanceNo, $roundNo]" +
                                println("\tAccepted: ${it.ack}\n===")
                    }
                } catch (e: StatusException) {
                    null
                }
            }
            return ok
        }

        private suspend fun commit() {
            val commitMsg = commit {
                value = this@Instance.value
                instanceNo = this@Instance.instanceNo
            }
            thisLearner.doCommit(commitMsg)
        }
    }
}

private fun maxByRoundNo(promises: List<Promise>): ByteString {
    var maxRoundNo = RoundNo(0, 0)
    var v: ByteString = EMPTY_BYTE_STRING

    for (promise in promises) {
        val roundNo = RoundNo(promise.roundNo)
        if (maxRoundNo < roundNo) {
            maxRoundNo = roundNo
            v = promise.value
        }
    }
    return v
}


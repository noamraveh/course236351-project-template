package zookeeper.kotlin

import kotlinx.coroutines.*
import org.apache.zookeeper.ZooKeeper
import org.apache.zookeeper.data.Stat
import java.util.concurrent.Executors
import org.apache.zookeeper.Watcher

typealias Path = String
typealias ZName = String
typealias ZChildren = List<ZName>




class ZookeeperKtClient(private val zk: ZooKeeper) : ZooKeeperKt {
    override val namespace: Path
        get() = "/"

    override suspend fun create(op: CreateOperation): Pair<Path, Stat> {
        val path = applyNamespace(op.path, op.flags.isSequential)
        val mode = op.flags.zkCreateMode
        val stat = Stat()
        var createdPath: Path = ""

        if (op.flags.hasTTL) {
            val ttl = op.flags.TTL
            createdPath = zkThreadContext {
                catchKeeperExceptions(op.handlers) {
                    zk.create(path, op.data, op.acl, mode, stat, ttl)!!
                }?: "EMPTY"
            }
        } else {
            createdPath = zkThreadContext {
                catchKeeperExceptions(op.handlers) {
                    println("creating $path")
                    val p= zk.create(path, op.data, op.acl, mode, stat)
                    println("created $p")
                    p!!
                } ?: "EMPTY"
            }
        }
        return Pair(createdPath, stat)
    }

    override suspend fun getChildren(op: GetChildrenOperation): Pair<ZChildren, Stat> {
        val path = applyNamespace(op.path)
        val stat = Stat()
        val watcher: Watcher? = op.watchers.all?.toZKWatcher()
        val childrenList = zkThreadContext {
            catchKeeperExceptions(op.handlers) {
                zk.getChildren(path, watcher, stat)
            }
        }
        return Pair(childrenList!!.map { it!! }, stat)
    }

    override suspend fun delete(op: DeleteOperation) {
        val path = applyNamespace(op.path)
        val version: Int = op.version ?: -1
        zkThreadContext {
            catchKeeperExceptions(op.handlers) {
                zk.delete(path, version)
            }
        }
    }

    override suspend fun exists(op: CheckExistenceOperation): Pair<Boolean, Stat?> {
        val path = applyNamespace(op.path)
        val watcher: Watcher? = op.watchers.all?.toZKWatcher()
        val stat = zkThreadContext {
            catchKeeperExceptions(op.handlers) {
                zk.exists(path, watcher)
            }
        }
        return Pair(stat != null, stat)
    }

}


private val zkAPIContext = Executors.newSingleThreadExecutor().asCoroutineDispatcher()

private suspend fun <T> zkThreadContext(block: () -> T): T = coroutineScope {
    async(zkAPIContext) { block() }.await()
}


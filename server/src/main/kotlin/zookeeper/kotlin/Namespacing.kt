package zookeeper.kotlin

import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.data.Stat
import zookeeper.kotlin.createflags.Persistent

interface ZooKeeperNamespacer {
    val namespace: Path
    suspend fun usingNamespace(namespace: Path): ZooKeeperKt
    fun applyNamespace(path: Path, isSequential: Boolean = false): Path =
        ZKPaths.fixForNamespace(namespace, path, isSequential)!!
}

internal class NamespaceDecorator(
    private val zk: ZooKeeperKt,
    private val subnamespace: Path,
) : ZooKeeperKt {

    override val namespace: Path = ZKPaths.fixForNamespace(zk.namespace, subnamespace)!!

    override suspend fun getChildren(op: GetChildrenOperation): Pair<ZChildren, Stat> =
        zk.getChildren(op.apply {
            path = ZKPaths.fixForNamespace(subnamespace, path)!!
        })

    override suspend fun delete(op: DeleteOperation) =
        zk.delete(op.apply {
            path = ZKPaths.fixForNamespace(subnamespace, path)!!
        })

    override suspend fun exists(op: CheckExistenceOperation): Pair<Boolean, Stat?> =
        zk.exists(op.apply {
            path = ZKPaths.fixForNamespace(subnamespace, path)!!
        })

    override suspend fun create(op: CreateOperation) = zk.create(op.apply {
        path = ZKPaths.fixForNamespace(subnamespace, path)!!
    })

    override suspend fun usingNamespace(namespace: Path) = make(this, namespace)

    companion object {
        suspend fun make(zk: ZooKeeperKt, namespace: Path): NamespaceDecorator {
            zk.create(namespace) {
                flags = Persistent
                ignore(KeeperException.Code.NODEEXISTS)
            }
            return NamespaceDecorator(zk, namespace)
        }
    }
}
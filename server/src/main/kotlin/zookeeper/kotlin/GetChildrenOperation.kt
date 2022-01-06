package zookeeper.kotlin

import org.apache.zookeeper.data.Stat

data class GetChildrenOperation(
    var path: Path,
    private val keeperExceptionCatcher: KeeperExceptionCatcher = KeeperExceptionCatcherImpl(),
) : KeeperExceptionCatcher by keeperExceptionCatcher, ZooKeeperWatchable {
    override val watchers: WatcherList = WatchersListImpl()
}

inline fun getChildrenOperation(
    path: Path,
    block: GetChildrenOperation.() -> Unit,
): GetChildrenOperation = GetChildrenOperation(path).apply(block)


interface ZooKeeperChildrenGetter {
    suspend fun getChildren(op: GetChildrenOperation): Pair<ZChildren, Stat>

    suspend fun getChildren(path: Path, block: GetChildrenOperation.() -> Unit = {}) =
        getChildren(getChildrenOperation(path, block))
}
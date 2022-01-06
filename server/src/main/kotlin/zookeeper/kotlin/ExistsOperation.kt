package zookeeper.kotlin

import org.apache.zookeeper.data.Stat

data class CheckExistenceOperation(
    var path: Path,
    private val keeperExceptionCatcher: KeeperExceptionCatcher = KeeperExceptionCatcherImpl(),
) : KeeperExceptionCatcher by keeperExceptionCatcher, ZooKeeperWatchable {
    override val watchers: WatcherList = WatchersListImpl()
}

inline fun checkExistenceOperation(
    path: Path,
    block: CheckExistenceOperation.() -> Unit,
): CheckExistenceOperation = CheckExistenceOperation(path).apply(block)


interface ZooKeeperExistenceChecker {
    suspend fun exists(op: CheckExistenceOperation): Pair<Boolean, Stat?>

    suspend fun exists(path: Path, block: CheckExistenceOperation.() -> Unit = {}) =
        exists(checkExistenceOperation(path, block))
}
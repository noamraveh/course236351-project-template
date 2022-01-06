package zookeeper.kotlin

data class DeleteOperation(
    var path: Path,
    var version: Int? = null,
    private val keeperExceptionCatcher: KeeperExceptionCatcher = KeeperExceptionCatcherImpl(),
) : KeeperExceptionCatcher by keeperExceptionCatcher

inline fun deleteOperation(
    path: Path,
    block: DeleteOperation.() -> Unit,
): DeleteOperation = DeleteOperation(path).apply(block)

interface ZooKeeperDeletor {
    suspend fun delete(op: DeleteOperation)
    suspend fun delete(path: Path = "", block: DeleteOperation.() -> Unit = {}) =
        delete(deleteOperation(path, block))
}


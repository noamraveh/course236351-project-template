package zookeeper.kotlin

import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.ZooDefs
import org.apache.zookeeper.data.Stat

internal enum class BaseType { None, PERSISTENT, EPHEMERAL, CONTAINER }

sealed class CreateFlagsType {
    val hasTTL: Boolean get() = baseType == BaseType.PERSISTENT && _ttl != null
    val TTL: Long get() = _ttl!!
    val zkCreateMode: CreateMode get() = converToCreateMode()
    infix fun and(other: CreateFlagsType) = other.combineWith(this)


    protected abstract fun combineWith(b: CreateFlagsType): CreateFlagsType
    internal abstract val baseType: BaseType
    internal abstract val isSequential: Boolean
    internal abstract val _ttl: Long?


    private fun converToCreateMode() = when {
        //@formatter:off
        baseType == BaseType.CONTAINER  && !isSequential && !hasTTL -> CreateMode.CONTAINER
        baseType == BaseType.PERSISTENT && !isSequential && !hasTTL -> CreateMode.PERSISTENT
        baseType == BaseType.PERSISTENT &&  isSequential && !hasTTL -> CreateMode.PERSISTENT_SEQUENTIAL
        baseType == BaseType.PERSISTENT && !isSequential &&  hasTTL -> CreateMode.PERSISTENT_WITH_TTL
        baseType == BaseType.PERSISTENT &&  isSequential &&  hasTTL -> CreateMode.PERSISTENT_SEQUENTIAL_WITH_TTL
        baseType == BaseType.EPHEMERAL  && !isSequential && !hasTTL -> CreateMode.EPHEMERAL
        baseType == BaseType.EPHEMERAL  &&  isSequential && !hasTTL -> CreateMode.EPHEMERAL_SEQUENTIAL
        //@formatter:on
        else -> throw IllegalStateException("Invalid combination for create mode")
    }

    internal inline fun assertNot(assertion: Boolean, msg: CreateFlagsType.() -> String) {
        if (assertion) throw IllegalStateException(msg())
    }

    open class CreateFlagsBase
    internal constructor(
        override val baseType: BaseType,
        override val isSequential: Boolean = false,
        override val _ttl: Long? = null,
    ) : CreateFlagsType() {
        override fun combineWith(b: CreateFlagsType): CreateFlagsType = throw NotImplementedError()
    }
}

object CreateFlags {
    internal object None : CreateFlagsType.CreateFlagsBase(BaseType.None)

    object Persistent : CreateFlagsType.CreateFlagsBase(BaseType.PERSISTENT) {
        infix fun withTTL(ttl: Long) = this and TTL(ttl)
    }

    object Ephemeral : CreateFlagsType.CreateFlagsBase(BaseType.EPHEMERAL)
    object Container : CreateFlagsType.CreateFlagsBase(BaseType.CONTAINER)

    object Sequential : CreateFlagsType.CreateFlagsBase(BaseType.None, isSequential = true) {
        override fun combineWith(b: CreateFlagsType): CreateFlagsType {
            b.assertNot(baseType == BaseType.CONTAINER) {
                "Container cannot be sequential"
            }
            return CreateFlagsBase(b.baseType, isSequential = this.isSequential, _ttl = b._ttl)
        }
    }

    class TTL(val ttl: Long) : CreateFlagsType.CreateFlagsBase(BaseType.None, _ttl = ttl) {
        override fun combineWith(b: CreateFlagsType): CreateFlagsType {
            b.assertNot(baseType in setOf(BaseType.CONTAINER, BaseType.EPHEMERAL)) {
                "Ephemeral or Container cannot have a ttl"
            }
            return CreateFlagsBase(BaseType.PERSISTENT,
                isSequential = b.isSequential,
                _ttl = ttl)
        }
    }
}

data class CreateOperation(
    var path: Path,
    var flags: CreateFlagsType = CreateFlags.None,
    var data: ByteArray = ByteArray(0),
    var acl: List<org.apache.zookeeper.data.ACL> = ZooDefs.Ids.OPEN_ACL_UNSAFE,
    private val keeperExceptionCatcher: KeeperExceptionCatcher = KeeperExceptionCatcherImpl(),
) : KeeperExceptionCatcher by keeperExceptionCatcher

inline fun createOperation(
    path: Path,
    block: CreateOperation.() -> Unit,
): CreateOperation = CreateOperation(path).apply(block)

interface ZooKeeperCreator {
    suspend fun create(op: CreateOperation): Pair<Path, Stat>
    suspend fun create(path: Path = "", block: CreateOperation.() -> Unit = {}): Pair<Path, Stat> =
        create(createOperation(path, block))
}


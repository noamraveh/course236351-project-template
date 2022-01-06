package zookeeper.kotlin

import org.apache.zookeeper.KeeperException

typealias KeeperExceptionHandler = (KeeperException) -> Unit

interface KeeperExceptionHandlers {
    operator fun get(code: KeeperException.Code): KeeperExceptionHandler
    operator fun contains(code: KeeperException.Code): Boolean
    val finally: () -> Unit
}

internal class KeeperExceptionHandlersMapAdapter(
    val map: KeeperExceptionHandlerMap,
    override val finally: () -> Unit,
) :
    KeeperExceptionHandlers {
    override fun get(code: KeeperException.Code) = map[code]!!
    override fun contains(code: KeeperException.Code) = code in map
}

interface KeeperExceptionCatcher {
    fun catch(code: KeeperException.Code, handler: KeeperExceptionHandler)
    fun ignore(code: KeeperException.Code) = catch(code, {})
    var finally: () -> Unit
    val handlers: KeeperExceptionHandlers
}

typealias KeeperExceptionHandlerMap = Map<KeeperException.Code, KeeperExceptionHandler>
typealias KeeperExceptionHandlerMutableMap = MutableMap<KeeperException.Code,
        KeeperExceptionHandler>

class KeeperExceptionCatcherImpl : KeeperExceptionCatcher {
    val handlersMap: KeeperExceptionHandlerMutableMap = mutableMapOf()
    override fun catch(code: KeeperException.Code, handler: KeeperExceptionHandler) {
        handlersMap.putIfAbsent(code, handler)
    }

    override var finally: () -> Unit = {}

    override val handlers: KeeperExceptionHandlers
        get() = KeeperExceptionHandlersMapAdapter(handlersMap.toMap(), finally)
}

inline fun <T> catchKeeperExceptions(
    handlers: KeeperExceptionHandlers,
    block: () -> T?,
): T? = try {
    block()!!
} catch (e: KeeperException) {
    if (e.code() in handlers) {
        handlers[e.code()](e)
    } else {
        throw e
    }
    null
} finally {
    handlers.finally?.invoke()
}
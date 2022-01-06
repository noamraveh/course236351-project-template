package zookeeper.kotlin

import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.zookeeper.Watcher as ZKWatcher
import org.apache.zookeeper.Watcher.Event.EventType as ZKEventType
import org.apache.zookeeper.Watcher.Event.KeeperState as ZKKeeperState
import org.apache.zookeeper.WatchedEvent as ZKWatchedEvent

typealias Watcher = suspend (path: Path, type: ZKEventType, state: ZKKeeperState) -> Unit

interface WatcherList {
    operator fun plusAssign(watcher: Watcher?)
    val all: List<Watcher>?
}

class WatchersListImpl : WatcherList {
    private val list: MutableList<Watcher> = mutableListOf()
    override fun plusAssign(watcher: Watcher?) {
        watcher?.let { list += it }
    }


    override val all: List<Watcher>?
        get() = if (list.size > 0) list.toList() else null
}

interface ZooKeeperWatchable {
    val watchers: WatcherList
}

internal fun List<Watcher>.toZKWatcher(): ZKWatcher = ZKWatcher {
    val event = it!!
    val path: Path = event.path!!
    val type: ZKEventType = event.type!!
    val state: ZKKeeperState = event.state!!

    runBlocking {
        for (watcher in this@toZKWatcher.map { it!! }) {
            launch {
                watcher(path, type, state)
            }
        }
    }
}
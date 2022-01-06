package zookeeper.kotlin


interface ZooKeeperKt
    : ZooKeeperCreator, ZooKeeperNamespacer, ZooKeeperChildrenGetter,
    ZooKeeperDeletor, ZooKeeperExistenceChecker {
    override suspend fun usingNamespace(namespace: Path): ZooKeeperKt =
        NamespaceDecorator.make(this, namespace)
}

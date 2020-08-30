

基于Zookeeper实现简易版配置中心

> 要求实现以下功能：
>
> 创建一个Web项目，将数据库连接信息交给Zookeeper配置中心管理，即：当项目Web项目启动时，从Zookeeper进行MySQL配置参数的拉取
>
> 要求项目通过数据库连接池访问MySQL（连接池可以自由选择熟悉的）
>
> 当Zookeeper配置信息变化后Web项目自动感知，正确释放之前连接池，创建新的连接池


1. 启动 `SpringBoot` 项目
> 启动时，从 `Zookeeper` 拉取配置

2. 




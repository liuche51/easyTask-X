# easyTask-X

* 一个方便触发一次性或周期性任务执行的分布式架构组件，支持海量,高并发,高可用的任务处理
* A distributed architecture component that facilitates the triggering of one-time or periodic task execution, supporting massive, highly concurrent, and highly available task processing

## 使用场景(Usage scenarios)

* 乘坐网约车结单后30分钟若顾客未评价，则系统将默认提交一条评价信息
* 银行充值接口，要求5分钟后才可以查询到结果成功OR失败
* 会员登录系统30秒后自动发送一条登录短信通知
* 每个登录用户每隔10秒统计一次其某活动中获得的积分

## 特性(Features)

![Architecture](https://images.cnblogs.com/cnblogs_com/liuche/1811577/o_200722062611QQ%E5%9B%BE%E7%89%8720200722142544.png)
* 高可用：因为我们是分布式master-slave集群，每个任务多有多个备份数据，节点宕机，集群自动重新选举和恢复。所以可靠性非常高
* 秒级触发：我们是采用时钟秒级分片的数据结构，支持秒级触发任务。不早也不迟
* 分布式：独立的分布式中间件，类似kafka等架构
* 高并发：支持多线程同时提交任务，支持多线程同时执行任务
* 数据一致性：提供三种由弱到强的一致性任务提交模式。兼顾性能和较强的数据一致性能力。
* 海量任务：节点可以存储非常多的任务，只要内存和磁盘足够。触发效率也是极高。需要配置好分派任务线程池和执行任务线程池大小即可
* 开源：组件完全在GitHub上开源。任何人都可以随意使用，在不侵犯著作权情况下
* 易使用：提供开发的客户端Java SDK，简单易用。服务端部署也简单，仅依赖一个第三方的zookeeper。

## 总体架构(Architecture)

![Architecture](https://images.cnblogs.com/cnblogs_com/liuche/2095303/o_220116063130_QQ%E5%9B%BE%E7%89%8720220116142408.png)
　　整体采用分布式设计，leader-follower/master-slave风格。集群中每一个节点都是master，同时也可能是其他某个节点的slave。每个master都有若干个slave。master上提交的新任务都会被salve异步同步过去，删除任务同时也会强制删除salve中的备份任务。
* Leader：集群中心控制节点。主管集群所有节点的注册、心跳及master/slave选举等。
* follower：集群普通节点。接受存储任务信息，管理任务CRUD，对接客户端任务。
* master：负责接受和推送客户端任务，管理节点任务。同步slave的任务信息。每个节点都是一个master
* slave：负责和master上的任务信息保持同步，作为备份。以便在master宕机后，可以接管其工作，成为任务的新master。
* client：以sdk方式引入到业务微服务中。负责用户和集群之间信息传递的门户。
* zookeeper：第三方中间件。用于集群的leader选举，保证集群分区容错性和leader宕机后集群稳定过度到新leader。

## 架构之环形队列(AnnularQueue)

![Architecture](https://images.cnblogs.com/cnblogs_com/liuche/1811577/o_200722062635%E5%9B%BE%E7%89%872.png)
　　环形队列在之前单机版的easyTask中也讲过，原理都是类似的。客户端提交任务，服务端先将任务进行持久化，再添加上环形队列这个数据结构中去，等待时间片轮询的到来。不同的是这里的持久化机制，改成了分布式存储了。不仅master自己存储起来，还要同步存储到其slave中去。删除一个任务也是类似的过程。

　　任务添加时会计算其触发所属的时间分片槽，等环形队列的始终秒针到达时会判断任务是否可以被执行了。如果可以执行了，则分派任务线程池将其丢入执行任务线程池等待执行。只要执行任务线程池线程数足够，任务将立即得到执行。


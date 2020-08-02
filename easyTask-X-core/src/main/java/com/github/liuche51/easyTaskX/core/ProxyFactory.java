package com.github.liuche51.easyTaskX.core;

import com.github.liuche51.easyTaskX.cluster.ClusterService;
import com.github.liuche51.easyTaskX.dto.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

class ProxyFactory {
    private static Logger log = LoggerFactory.getLogger(ProxyFactory.class);
    private Task target;
    public ProxyFactory(Task target) {
        this.target = target;
    }

    public Object getProxyInstance() {
        return Proxy.newProxyInstance(
                target.getClass().getClassLoader(),
                target.getClass().getInterfaces(),
                new InvocationHandler() {
                    @Override
                    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                        String id=target.getTaskExt().getId();
                        log.debug("任务:{} 代理执行开始", id);
                        try {
                            return method.invoke(target, args);
                        } catch (Exception e) {
                            log.error("target proxy method execute exception！task.id="+id, e);
                            throw e;
                        }finally {
                            log.debug("任务:{} 代理执行结束", id);
                            if (target.getTaskType().equals(TaskType.ONECE)){
                                boolean ret = ClusterService.deleteTask(id);
                                if (ret)
                                {
                                    log.debug("任务:{} 执行完成，已从持久化记录中删除", id);
                                }
                            }
                        }
                    }
                }
        );
    }

}

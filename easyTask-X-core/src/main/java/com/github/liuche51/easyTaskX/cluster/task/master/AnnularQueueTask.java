package com.github.liuche51.easyTaskX.cluster.task.master;


import com.github.liuche51.easyTaskX.cluster.follow.BrokerService;
import com.github.liuche51.easyTaskX.cluster.follow.ClientService;
import com.github.liuche51.easyTaskX.cluster.task.TimerTask;
import com.github.liuche51.easyTaskX.dto.InnerTask;
import com.github.liuche51.easyTaskX.dto.Slice;
import com.github.liuche51.easyTaskX.enume.ImmediatelyType;
import com.github.liuche51.easyTaskX.enume.TaskType;
import com.github.liuche51.easyTaskX.util.LogUtil;

import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 任务执行器
 * 单例模式
 */
public class AnnularQueueTask extends TimerTask {
    /**
     * 时间分片。
     */
    private Slice[] slices = new Slice[60];
    private static AnnularQueueTask singleton = null;

    public Slice[] getSlices() {
        return slices;
    }

    public static AnnularQueueTask getInstance() {
        if (singleton == null) {
            synchronized (AnnularQueueTask.class) {
                if (singleton == null) {
                    singleton = new AnnularQueueTask();
                }
            }
        }
        return singleton;
    }

    private AnnularQueueTask() {
        for (int i = 0; i < slices.length; i++) {
            slices[i] = new Slice();
        }
    }

    @Override
    public void run() {
        while (!isExit()) {
            int lastSecond = 0;
            while (true) {
                int second = ZonedDateTime.now().getSecond();
                long currenttime = System.currentTimeMillis() + 1000l;//当前时间，因为计算时有一秒钟内的精度问题，所以判断时当前时间需多补上一秒。这样才不会导致某些任务无法得到及时的执行
                if (second == lastSecond) {
                    try {
                        Thread.sleep(500l);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    continue;
                }
                Slice slice = slices[second];
                //slice.getList().size()数量多时，会非常耗时。生产下需要关闭此处
                // LogUtil.debug("已执行时间分片:{}，任务数量:{}", second, slice.getList() == null ? 0 : slice.getList().size());
                lastSecond = second;
                BrokerService.getConfig().getDispatchs().submit(new Runnable() {
                    public void run() {
                        ConcurrentHashMap<String, InnerTask> list = slice.getList();
                        List<InnerTask> periodSchedules = new LinkedList<>();
                        Iterator<Map.Entry<String, InnerTask>> items = list.entrySet().iterator();
                        while (items.hasNext()) {
                            Map.Entry<String, InnerTask> item = items.next();
                            InnerTask innerTask = item.getValue();
                            //判断任务是否可以本次执行
                            if (currenttime >= innerTask.getExecuteTime()) {
                                BrokerService.getConfig().getWorkers().submit(new Runnable() {
                                    @Override
                                    public void run() {
                                        BrokerService.notifyClientExecuteNewTask(innerTask);
                                    }
                                });
                                if (TaskType.PERIOD.equals(innerTask.getTaskType()))//周期任务需要重新提交新任务
                                    periodSchedules.add(innerTask);
                                list.remove(item.getKey());
                                LogUtil.debug("工作任务:{} 已提交执行。所属分片:{}", innerTask.getId(), second);
                            }
                            //因为列表是已经按截止执行时间排好序的，可以节省后面元素的过期判断
                            else break;
                        }
                        submitNewPeriodSchedule(periodSchedules);
                    }
                });
            }
        }
    }

    /**
     * 批量创建新周期任务
     *
     * @param list
     */
    private void submitNewPeriodSchedule(List<InnerTask> list) {
        for (InnerTask schedule : list) {
            try {
                schedule.setExecuteTime(InnerTask.getNextExcuteTimeStamp(schedule.getPeriod(), schedule.getUnit()));
                int slice = AddSlice(schedule);
                LogUtil.debug("已重新提交周期任务:{}，所属分片:{}，线程ID:{}", schedule.getId(), slice, Thread.currentThread().getId());
            } catch (Exception e) {
                LogUtil.error("submitNewPeriodSchedule exception！", e);
            }
        }
    }

    /**
     * 将任务添加到时间分片中去。
     *
     * @param innerTask
     * @return
     */
    private int AddSlice(InnerTask innerTask) {
        ZonedDateTime time = ZonedDateTime.ofInstant(new Timestamp(innerTask.getExecuteTime()).toInstant(), ZoneId.systemDefault());
        int second = time.getSecond();
        Slice slice = slices[second];
        ConcurrentHashMap<String, InnerTask> list2 = slice.getList();
        list2.put(innerTask.getId(), innerTask);
        LogUtil.debug("已添加类型:{}任务:{}，所属分片:{} 预计执行时间:{} 线程ID:{}", innerTask.getTaskType().name(), innerTask.getId(), time.getSecond(), time.toLocalTime(), Thread.currentThread().getId());
        return second;
    }

    /**
     * 提交任务到时间轮分片
     * 提交到分片前需要做的一些逻辑判断
     *
     * @param innerTask
     * @throws Exception
     */
    public void submitAddSlice(InnerTask innerTask) throws Exception {
        //分布式立即执行的任务，第一次不走时间分片环形队列，直接提交执行。
        if (innerTask.getImmediatelyType().equals(ImmediatelyType.DISTRIB)) {
            LogUtil.debug("分布式立即执行类工作任务:{}已提交代理执行", innerTask.getId());
            BrokerService.getConfig().getWorkers().submit(new Runnable() {
                @Override
                public void run() {
                    BrokerService.notifyClientExecuteNewTask(innerTask);
                }
            });
            //如果是一次性任务，则不用继续提交到时间分片中了
            if (innerTask.getTaskType().equals(TaskType.ONECE)) {
                return;
            }
            //前面只处理了周期任务非立即执行的情况。这里处理立即执行的情况下。需要重新设置下一个执行周期
            else if (innerTask.getTaskType().equals(TaskType.PERIOD)) {
                innerTask.setExecuteTime(InnerTask.getNextExcuteTimeStamp(innerTask.getPeriod(), innerTask.getUnit()));
            }
        }
        AddSlice(innerTask);
    }

    /**
     * 清空所有任务
     */
    public void clearTask() {
        for (Slice s : slices) {
            s.getList().clear();
        }
    }

    /**
     * 变更任务的所属Broker。
     * 原Broker宕机时
     *
     * @param newBroker
     * @param oldBroker
     */
    public void changeBroker(String newBroker, String oldBroker) {
        for (int i = 0; i < this.slices.length; i++) {
            Slice s = this.slices[i];
            Iterator<Map.Entry<String, InnerTask>> items = s.getList().entrySet().iterator();
            while (items.hasNext()) {
                Map.Entry<String, InnerTask> item = items.next();
                InnerTask task = item.getValue();
                if (oldBroker.equals(task.getBroker()))
                    task.setBroker(newBroker);
            }
        }
    }
}

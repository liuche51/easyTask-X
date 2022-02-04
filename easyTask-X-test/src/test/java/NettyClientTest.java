import com.github.liuche51.easyTaskX.cluster.EasyTaskConfig;
import com.github.liuche51.easyTaskX.cluster.follow.BrokerService;
import com.github.liuche51.easyTaskX.dto.BaseNode;
import com.github.liuche51.easyTaskX.dto.proto.Dto;
import com.github.liuche51.easyTaskX.dto.proto.NodeDto;
import com.github.liuche51.easyTaskX.dto.proto.ResultDto;
import com.github.liuche51.easyTaskX.dto.proto.ScheduleDto;
import com.github.liuche51.easyTaskX.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTaskX.netty.client.NettyClient;
import com.github.liuche51.easyTaskX.netty.client.NettyConnectionFactory;
import com.github.liuche51.easyTaskX.netty.client.NettyMsgService;
import com.github.liuche51.easyTaskX.util.StringConstant;
import com.github.liuche51.easyTaskX.util.Util;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;

public class NettyClientTest {
    @Test
    public void sendSyncMsg() throws Exception {
        EasyTaskConfig config=new EasyTaskConfig();
        config.getAdvanceConfig().setTimeOut(30);
        config.getAdvanceConfig().setNettyPoolSize(2);
        //ClusterService.setConfig(config);
        try {
            while (true){
                ScheduleDto.Schedule.Builder builder=ScheduleDto.Schedule.newBuilder();
                String id=String.valueOf(System.currentTimeMillis());
                builder.setId(id).setClassPath("com.github.liuche51.easyTask.test.task.CusTask1").setExecuteTime(1586078809995l)
                        .setTaskType("PERIOD").setPeriod(30).setUnit("SECONDS").setSource("127.0.0.1:2020")
                        .setParam("birthday#;1986-1-1&;threadid#;1&;name#;Jack&;age#;32&");
                Dto.Frame.Builder builder1=Dto.Frame.newBuilder();
                builder1.setIdentity(Util.generateIdentityId());
                builder1.setInterfaceName(NettyInterfaceEnum.MasterNotifySlaveTranTrySaveTask).setBodyBytes(builder.build().toByteString());
                System.out.println("发送任务:"+id);
                NettyClient client=NettyConnectionFactory.getInstance().getConnection("127.0.0.1",2021);
                Object msg= NettyMsgService.sendSyncMsg(client,builder1.build());
                Dto.Frame frame= (Dto.Frame) msg;
                String ret=frame.getBody();
                System.out.println("服务器返回:"+ret);
                Thread.sleep(500000);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Test
    public void sendSyncMsgWithList(){
        try {
            while (true){
                ScheduleDto.Schedule.Builder builder0=ScheduleDto.Schedule.newBuilder();
                String id=String.valueOf(System.currentTimeMillis());
                builder0.setId(id).setClassPath("com.github.liuche51.easyTask.test.task.CusTask1").setExecuteTime(1586078809995l)
                        .setTaskType("PERIOD").setPeriod(30).setUnit("SECONDS").setSource("127.0.0.1:2020")
                        .setParam("birthday#;1986-1-1&;threadid#;1&;name#;Jack&;age#;32&");
                ScheduleDto.Schedule.Builder builder1=ScheduleDto.Schedule.newBuilder();
                String id1=String.valueOf(System.currentTimeMillis()+1);
                builder1.setId(id1).setClassPath("com.github.liuche51.easyTask.test.task.CusTask1").setExecuteTime(1586078809995l)
                        .setTaskType("PERIOD").setPeriod(30).setUnit("SECONDS").setSource("127.0.0.1:2020")
                        .setParam("birthday#;1986-1-1&;threadid#;1&;name#;Jack&;age#;32&");
                ScheduleDto.ScheduleList.Builder builder3=ScheduleDto.ScheduleList.newBuilder();
                builder3.addSchedules(builder0.build());
                builder3.addSchedules(builder1.build());
                Dto.Frame.Builder builder4=Dto.Frame.newBuilder();
                builder4.setIdentity(StringConstant.EMPTY);
                builder4.setInterfaceName(NettyInterfaceEnum.MasterSyncDataToNewSlave).setBodyBytes(builder3.build().toByteString());
                System.out.println("发送任务:"+id);
                NettyClient client=NettyConnectionFactory.getInstance().getConnection("127.0.0.1",2021);
                Object msg= NettyMsgService.sendSyncMsg(client,builder4.build());
                Dto.Frame frame= (Dto.Frame) msg;
                String ret=frame.getBody();
                System.out.println("服务器返回:"+ret);
                Thread.sleep(5000);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Test
    public void sendASyncMsg(){
        try {
            EasyTaskConfig config=new EasyTaskConfig();
            config.getAdvanceConfig().setTimeOut(30);
            config.getAdvanceConfig().setNettyPoolSize(2);
            BrokerService.setConfig(config);
            while (true){
                ScheduleDto.Schedule.Builder builder=ScheduleDto.Schedule.newBuilder();
                String id=String.valueOf(System.currentTimeMillis());
                builder.setId(id).setClassPath("com.github.liuche51.easyTask.test.task.CusTask1").setExecuteTime(1586078809995l)
                        .setTaskType("PERIOD").setPeriod(30).setUnit("SECONDS").setSource("127.0.0.1")
                        .setParam("birthday#;1986-1-1&;threadid#;1&;name#;Jack&;age#;32&");
                Dto.Frame.Builder builder1=Dto.Frame.newBuilder();
                builder1.setIdentity(StringConstant.EMPTY);
                builder1.setInterfaceName(NettyInterfaceEnum.MasterNotifySlaveTranTrySaveTask).setBodyBytes(builder.build().toByteString());
                System.out.println("发送任务:"+id);
                NettyClient client=NettyConnectionFactory.getInstance().getConnection("127.0.0.1",2021);
                ChannelFuture future= NettyMsgService.sendASyncMsg(client,builder1.build());
                //这种异步消息回调方法，没法获取服务器返回的结果信息，只能知道是否完成异步通信。如果正常通信了(消息发出去了)则
                //会进入isSuccess()方法，即便接受方发生异常了也不影响，连接失败，则直接抛出异常。超时可能会进入isDone()方法
                future.addListener(new GenericFutureListener<Future<? super Void>>() {
                    @Override
                    public void operationComplete(Future<? super Void> future) throws Exception {
                        if(future.isSuccess()){
                            System.out.println("future.isSuccess()");
                        }else if(future.isDone()){
                            System.out.println("future.isDone()");
                        }else {
                            System.out.println("else");
                        }

                    }
                });
                Thread.sleep(5000);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Test
    public void getList() throws Exception {
        EasyTaskConfig config=new EasyTaskConfig();
        BrokerService.setConfig(config);
        Dto.Frame.Builder builder = Dto.Frame.newBuilder();
        builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.FollowRequestLeaderSendRegedit).setSource("127.0.0.1:2021")
        .setBody(StringConstant.BROKER);
        Dto.Frame frame = NettyMsgService.sendSyncMsg(new BaseNode("127.0.0.1",2021).getClient(), builder.build());
        ResultDto.Result result = ResultDto.Result.parseFrom(frame.getBodyBytes());
        if (StringConstant.TRUE.equals(result.getResult())) {
            NodeDto.Node node=NodeDto.Node.parseFrom(result.getBodyBytes());
            NodeDto.NodeList clientNodes=node.getClients();
            ConcurrentHashMap<String, BaseNode> clients=new ConcurrentHashMap<>();
            clientNodes.getNodesList().forEach(x->{
                clients.put(x.getHost()+":"+x.getPort(),new BaseNode(x.getHost(),x.getPort()));
            });
            NodeDto.NodeList followNodes=node.getClients();
            ConcurrentHashMap<String,BaseNode> follows=new ConcurrentHashMap<>();
            followNodes.getNodesList().forEach(x->{
                follows.put(x.getHost()+":"+x.getPort(),new BaseNode(x.getHost(),x.getPort()));
            });
        }
    }
}

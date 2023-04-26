package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }
    //private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();
    private IVoucherOrderService proxy;
    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }
    String queneName = "stream.orders";
         private class VoucherOrderHandler implements Runnable{
        @Override
        public void run() {
            while(true){
                try {
                    //获取消息队列中的订单消息 xreadgroup group g1 c1 count 1 block 2000 streams stream.orders
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queneName, ReadOffset.lastConsumed())
                    );
                    //判断消息获取是否成功
                    if(list == null||list.isEmpty()){
                        //如果没有消息，说明没有消息，下一次循环
                        continue;
                    }
                    //如果有消息
                    //解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    //获取订单
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    handleVoucherOrder(voucherOrder);
                    //ACK确认 SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queneName,"g1",record.getId());
                } catch (Exception e) {
                   log.error("处理订单异常",e);
                   HandlePendingList();
                }
            }
        }
    }

    private void HandlePendingList() {
        while(true){
            try {
                //获取pending-list中的订单消息 xreadgroup group g1 c1 count 1 block 2000 streams stream.orders
                List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                        Consumer.from("g1", "c1"),
                        StreamReadOptions.empty().count(1),
                        StreamOffset.create(queneName, ReadOffset.from("0"))
                );
                //判断消息获取是否成功
                if(list == null||list.isEmpty()){
                    //如果没有消息，说明没有消息，结束循环
                    break;
                }
                //如果有消息
                //解析消息中的订单信息
                MapRecord<String, Object, Object> record = list.get(0);
                Map<Object, Object> values = record.getValue();
                //获取订单
                VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                handleVoucherOrder(voucherOrder);
                //ACK确认 SACK stream.orders g1 id
                stringRedisTemplate.opsForStream().acknowledge(queneName,"g1",record.getId());
            } catch (Exception e) {
                log.error("pending-list异常",e);
                try {
                    Thread.sleep(20);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
    }
  /*  private class VoucherOrderHandler implements Runnable{
        @Override
        public void run() {
            while(true){
                try {
                    //获取队列中的订单消息
                    VoucherOrder voucherOrder = orderTasks.take();
                    //获取订单
                    handleVoucherOrder(voucherOrder);
                } catch (Exception e) {
                   log.error("处理订单异常",e);
                }
            }
        }
    }*/

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        //获取用户
        Long userId = voucherOrder.getUserId();
        //创建锁对象
        RLock lock= redissonClient.getLock("lock:order:" + userId);
        boolean tryLock = lock.tryLock();
        //获取锁
        //boolean tryLock = lock.tryLock(1200);
        if(!tryLock){
            //获取锁失败，返回错误或重试
           log.error("不允许重复下单");
        }

        try {
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            //释放锁
            lock.unlock();
        }

    }
    @Override
    public Result setKillVoucher(Long voucherId) {
        long orderId = redisIdWorker.nextId("order");
        //获取用户
        Long userId = UserHolder.getUser().getId();
        //执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(),String.valueOf(orderId)
        );
        int r = result.intValue();
        //判断结果是否为0
        if(r != 0){
            //不为0，没有购买资格
            return Result.fail(r == 1?"库存不足":"不能重复下单");
        }
        //为0，有购买资格，把下单信息保存到阻塞队列
        //获取代理对象(事务)
        proxy =(IVoucherOrderService) AopContext.currentProxy();
        //返回订单id
        return Result.ok(orderId);
    }
  /*  @Override
    public Result setKillVoucher(Long voucherId) {
        //获取用户
        Long userId = UserHolder.getUser().getId();
        //执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString()
        );
        int r = result.intValue();
        //判断结果是否为0
        if(r != 0){
            //不为0，没有购买资格
            return Result.fail(r == 1?"库存不足":"不能重复下单");
        }
        //为0，有购买资格，把下单信息保存到阻塞队列
        long id = redisIdWorker.nextId("order");
        //创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        //订单ID
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        //用户ID
        voucherOrder.setUserId(id);
        //代金券ID
        voucherOrder.setVoucherId(voucherId);
        //TODO  保存阻塞队列
        orderTasks.add(voucherOrder);
        //获取代理对象(事务)
        proxy =(IVoucherOrderService) AopContext.currentProxy();
        //返回订单id
        return Result.ok(orderId);
    }*/
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        //一人一单
        Long id = voucherOrder.getUserId();
        //查询订单
        int count = query().eq("user_id", id).eq("voucher_id", voucherOrder.getVoucherId()).count();
        if (count > 0) {
            //用户已经购买过了
            log.error("用户已经购买过了！");
            return;
        }
        //扣减库存
        boolean update = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId())
                .gt("stock", 0)
                .update();
        if (!update) {
            //扣减失败
            log.error("库存不足！");
            return ;
        }

        save(voucherOrder);
    }
   /* @Override
    public Result setKillVoucher(Long voucherId) {
        //查询优惠券
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        //判断秒杀是否开始
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            //尚未开始
            return Result.fail("秒杀尚未开始");
        }
        //判断秒杀是否结束
        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
            //已经结束
            return Result.fail("秒杀已经结束");
        }
        //判断库存是否充足
        if (voucher.getStock() < 1) {
            //库存不足
            return Result.fail("库存不足");
        }
        Long userId = UserHolder.getUser().getId();
        //创建锁对象
        //SimpleRedisLock lock = new SimpleRedisLock("orders"+userId,stringRedisTemplate);
        RLock lock= redissonClient.getLock("lock:order:" + userId);
        boolean tryLock = lock.tryLock();
        //获取锁
        //boolean tryLock = lock.tryLock(1200);
        if(!tryLock){
            //获取锁失败，返回错误或重试
            return Result.fail("不允许重复下单");
        }

        try {
            //获取代理对象(事务)
            IVoucherOrderService proxy =(IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        } finally {
            //释放锁
            lock.unlock();
        }

    }*/
  /*  @Transactional
    public Result createVoucherOrder(Long voucherId) {
        //一人一单
        Long id = UserHolder.getUser().getId();
        //查询订单
        int count = query().eq("user_id", id).eq("voucher_id", voucherId).count();
        if (count > 0) {
            return Result.fail("用户已经购买过一次");
        }
        //扣减库存
        boolean update = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId)
                .gt("stock", 0)
                .update();
        if (!update) {
            //扣减失败
            return Result.fail("库存不足");
        }
        //创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        //订单ID
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        //用户ID
        voucherOrder.setUserId(id);
        //代金券ID
        voucherOrder.setVoucherId(voucherId);
        super.save(voucherOrder);
        //返回订单ID
        return Result.ok(orderId);
    }*/
}

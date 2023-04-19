package com.hmdp.utils;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;

import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

@Component
@Slf4j
public class CacheClient {
    private final StringRedisTemplate stringRedisTemplate;
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }
    public void set(String key , Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),time,unit);
    }
    public void setWithLogicExpire(String key , Object value, Long time, TimeUnit unit){
        //设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }
    public <R,ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID , R> dbFallBack,Long time, TimeUnit unit){
        String key =keyPrefix + id;
        String json= stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);

        if (StrUtil.isNotBlank(json)){
            return  BeanUtil.toBean(json, type);
        }
        if(json != null){
            return null;
        }
        //不存在，根据id查询数据库
        R r = dbFallBack.apply(id);
        //不存在，返回错误
        if(r == null){
            //将空值写入redis
            stringRedisTemplate.opsForValue().set(key ,"",CACHE_NULL_TTL , TimeUnit.MINUTES);
            return null;
        }
        //存在，返回redis
        set(key,r,time,unit);
        return r;
    }
    public <R,ID> R queryWithLogicExpire(String keyPrefix ,ID id,Class<R> type,Function<ID ,R> dbFollback,Long time, TimeUnit unit) {
        String key =keyPrefix +id;
        //从redis查询商铺缓存
        String json= stringRedisTemplate.opsForValue().get(key);
        //判断是否存在
        if (StrUtil.isBlank(json)) {
            return null;
        }
        if(json != null){
            return null;
        }
        //命中，需要反序列化json为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();
        //判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())){
            //未过期，直接返回店铺信息
            return r;
        }
        //已过期，需要缓存重建
        //获取互斥锁
        boolean isLock = tryLock(LOCK_SHOP_KEY + id);
        //判断是否获取锁成功
        if(isLock) {
            //获取成功，开启一个线程执行重建
            json= stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
            //判断是否存在
            if (StrUtil.isNotBlank(json)) {
                //存在直接返回
               return BeanUtil.toBean(json, type);
            }
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    R r1 = dbFollback.apply(id);
                    this.setWithLogicExpire(key,r1,time,unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    //释放锁
                    unLock(LOCK_SHOP_KEY + id);
                }
            });
        }
        //返回过期商铺信息
        return r;
    }
    private boolean tryLock(String key){
        Boolean aBoolean = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(aBoolean);
    }
    private void unLock(String key){
        stringRedisTemplate.delete(key);
    }
}

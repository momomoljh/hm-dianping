package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TYPE_KEY;
import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TYPE_TTL;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Override
    public Result listShopType() {
        List<String> shopTypeJson = stringRedisTemplate.opsForList().range(CACHE_SHOP_TYPE_KEY, 0, -1);
        if(!shopTypeJson.isEmpty()){
            List<ShopType> shopTypes = shopTypeJson.stream().map(shopType ->
                    BeanUtil.toBean(shopType, ShopType.class)
            ).collect(Collectors.toList());
            return Result.ok(shopTypes);
        }
        List<ShopType> shopTypes =  lambdaQuery().orderByAsc(ShopType::getSort).list();
        //不存在，返回错误
        if(shopTypes.isEmpty()){
            //将空值写入redis
            return Result.fail("店铺分类不存在！");
        }
        List<String> shopTypeCache = shopTypes.stream().map(shopType ->
            JSONUtil.toJsonStr(shopType)
        ).collect(Collectors.toList());
        stringRedisTemplate.opsForList().rightPushAll(CACHE_SHOP_TYPE_KEY,shopTypeCache);
        stringRedisTemplate.expire(CACHE_SHOP_TYPE_KEY,CACHE_SHOP_TYPE_TTL, TimeUnit.MINUTES);
        return Result.ok(shopTypes);
    }
}

---
--- Generated by EmmyLua(https://github.com/EmmyLua)
--- Created by 86133.
--- DateTime: 2023/4/25 12:00
---
--1.参数列表
--1.1优惠券id
local voucherId = ARGV[1]
--1.2用户id
local userId = ARGV[2]

--数据key
--库存key
local stockKey = 'seckill:stock:' .. voucherId
--订单key
local orderKey = 'seckill:order:' .. voucherId

--脚本业务
--判断库存是否充足
if (tonumber(redis.call('get',stockKey)) <= 0) then
        --库存不足
    return 1
end
    --判断用户是否下单 SISMEMBER orderKey userId
if(redis.call('sismember',orderKey,userId) == 1) then
    --存在说明使重复下单 返回2
    return 2
end
    --扣库存 incrby stockKey -1
redis.call('incrby',stockKey,-1)
    --保存用户 sadd orderKey userId
redis.call('sadd',orderKey,userId)
return 0
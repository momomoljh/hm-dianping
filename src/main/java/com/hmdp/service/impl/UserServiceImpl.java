package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.UserHolder;
import javafx.scene.input.DataFormat;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.jdbc.Null;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;
import static com.hmdp.utils.SystemConstants.USER_NICK_NAME_PREFIX;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Override
    public Result sengCode(String phone, HttpSession session) {
        //检验手机号
        if(RegexUtils.isPhoneInvalid(phone)){
            //如果不符合，返回错误信息
            return Result.fail("手机号格式错误！");
        }
        //如果符合，生成验证码
        String code = RandomUtil.randomNumbers(6);

        //保存验证码到redis
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY + phone,code,LOGIN_CODE_TTL, TimeUnit.MINUTES);
        //发送验证码
        log.debug("发送短信验证码成功，验证码是：{}",code);
        //返回ok
        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        //检验手机号
        String phone = loginForm.getPhone();
        if(RegexUtils.isPhoneInvalid(phone)){
            //如果不符合，返回错误信息
            return Result.fail("手机号格式错误！");
        }
        //从redis获取验证码并检验
        String code = loginForm.getCode();
        String cacheCode= stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY+ phone);
        if(cacheCode == null ||!cacheCode.equals(code)){
            //不一致，报错
            return Result.fail("验证码错误！");
        }
        //一致，根据手机号查询用户
        LambdaQueryWrapper<User> queryWrapper = new LambdaQueryWrapper();
        queryWrapper.eq(User::getPhone,phone);
        User user = this.getOne(queryWrapper);
        //判断用户是否存在
        if(user == null){
            //不存在，创建新用户并保存
            user = createWithUser(phone);
        }
        //TODO 7 保存用户信息到redis中
        //TODO 7.1 随机生成token作为登录令牌
        String token = UUID.randomUUID().toString(true);
        //TODO 7.2 将user对象转换了Hash
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO,new HashMap<>(),
                CopyOptions.create()
                        .setIgnoreNullValue(true)
                        .setFieldValueEditor((filedName,filedValue)->filedValue.toString()));
        //7.3 存储
        String tokenKey = LOGIN_USER_KEY + token;
        stringRedisTemplate.opsForHash().putAll(tokenKey,userMap);
        // 设置token有效期
        stringRedisTemplate.expire(tokenKey,LOGIN_USER_TTL,TimeUnit.MINUTES);
        //8 TODO 返回token
        return Result.ok(token);
    }

    @Override
    public Result sign() {
        //获取当前用户
        UserDTO user = UserHolder.getUser();
        //获取当前日期
        LocalDateTime now = LocalDateTime.now();
        //拼接key
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key =  USER_SIGN_KEY + user +keySuffix;
        //获取今天是本月的第几天
        int dayOfMonth = now.getDayOfMonth();
        //写入Redis
        stringRedisTemplate.opsForValue().setBit(key,dayOfMonth - 1,true);
        return Result.ok();
    }

    @Override
    public Result signCount() {
        //获取当前用户
        UserDTO user = UserHolder.getUser();
        //获取当前日期
        LocalDateTime now = LocalDateTime.now();
        //拼接key
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key =  USER_SIGN_KEY + user +keySuffix;
        //获取今天是本月的第几天
        int dayOfMonth = now.getDayOfMonth();
        //获取本月截止今天为止所有的签到记录，返回一个十进制数字 BITFIELD KEY GET U28 0
        List<Long> result = stringRedisTemplate.opsForValue().bitField(key,
                BitFieldSubCommands.create()
                        .get(BitFieldSubCommands.BitFieldType.unsigned(dayOfMonth)).valueAt(0));
        if(result == null || result.isEmpty()){
            return Result.ok(0);
        }
        Long number = result.get(0);
        if(number == null ||number == 0){
            return Result.ok(0);
        }
        //循环遍历数字与1做与运算，得到最后一个bit位
        int count = 0;
        while (true) {
            //判断是否为0
            if ((number & 1) == 0) {
                //为0，结束
                break;
            } else {
                //不为0，计数器加一
                count++;
            }
            //把数字右移一位
            number>>>=1;
        }
        return Result.ok(count);
    }

    private User createWithUser(String phone) {
        //创建用户
        User user = new User();
        user.setPhone(phone);
        user.setNickName(USER_NICK_NAME_PREFIX + RandomUtil.randomNumbers(10));
        //保存用户
        this.save(user);
        return user;
    }
}

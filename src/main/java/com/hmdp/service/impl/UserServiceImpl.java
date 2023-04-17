package com.hmdp.service.impl;

import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.jdbc.Null;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpSession;

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

    @Override
    public Result sengCode(String phone, HttpSession session) {
        //检验手机号
        if(RegexUtils.isPhoneInvalid(phone)){
            //如果不符合，返回错误信息
            return Result.fail("手机号格式错误！");
        }
        //如果符合，生成验证码
        String code = RandomUtil.randomNumbers(6);

        //保存验证码到session
        session.setAttribute("code",code);
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
        //检验验证码
        String code = loginForm.getCode();
        Object cacheCode = session.getAttribute("code");
        if(cacheCode == null ||!cacheCode.toString().equals(code)){
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
        //保存用户信息到session中
        session.setAttribute("user",user);
        return Result.ok();
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

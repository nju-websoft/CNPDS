package nju.websoft.chinaopendataportal.Config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import nju.websoft.chinaopendataportal.Util.RequestLimitInterceptor;
import nju.websoft.chinaopendataportal.Util.UserAgentInterceptor;

@Configuration
public class WebConfig implements WebMvcConfigurer {

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(new UserAgentInterceptor());
        registry.addInterceptor(new RequestLimitInterceptor());
    }
}

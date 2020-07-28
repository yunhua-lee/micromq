package io.micromq.util;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import javax.servlet.http.HttpServletRequest;

public final class SpringUtils implements ApplicationContextAware {
    private static volatile ApplicationContext applicationContext;

    public static String getRealURL(HttpServletRequest request){
        return request.getRequestURL() + "?" + request.getQueryString();
    }

    @Override
    public synchronized void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        if( SpringUtils.applicationContext == null) {
            SpringUtils.applicationContext = applicationContext;
        }
    }

    public static Object getBean(String name){
        return getApplicationContext().getBean(name);
    }

    public static <T> T getBean(Class<T> clazz){
        return getApplicationContext().getBean(clazz);
    }

    public static <T> T getBean(String name,Class<T> clazz){
        return getApplicationContext().getBean(name, clazz);
    }

    private static ApplicationContext getApplicationContext(){
        return applicationContext;
    }
}

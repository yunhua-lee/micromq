package io.micromq.util;

import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ThreadFactory;

public final class SysUtils {
    public static ThreadFactory createThreadFactory(String name) {
        Validate.notBlank(name);

        return new BasicThreadFactory.Builder().namingPattern("micromq-thread-" + name + "-%d").daemon(true)
                .priority(Thread.MAX_PRIORITY).build();
    }

    public static String getHostName() {
        try {
            String hostname = InetAddress.getLocalHost().getHostName();
            return hostname;
        } catch (UnknownHostException e) {

            return "unknown";
        }
    }

    public static String getRootPath() {
        return SysUtils.class.getClassLoader().getResource("").getPath();
    }

    public static String getAbsolutePath(String path) {
        org.apache.commons.lang.Validate.notEmpty(path);

        if (path.startsWith("/")) {
            return getRootPath() + path.substring(1);
        } else {
            return getRootPath() + path;
        }
    }
}

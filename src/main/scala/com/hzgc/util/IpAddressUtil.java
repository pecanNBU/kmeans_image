package com.hzgc.util;

import org.apache.log4j.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * 获取IP、hostName工具类
 */
public class IpAddressUtil {

    private static Logger LOG = Logger.getLogger(IpAddressUtil.class);

    private static InetAddress netAddress = getInetAddress();

    private static InetAddress getInetAddress() {
        try {
            return InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            LOG.warn("unknown host!");
        }
        return null;
    }

    public static String getHostIp() {
        if (null == netAddress) {
            return null;
        }
        return netAddress.getHostAddress();
    }

    public static String getHostName() {
        if (null == netAddress) {
            return null;
        }
        return netAddress.getHostName();
    }
}



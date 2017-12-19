package com.hzgc.util;

import org.apache.log4j.Logger;

class StringUtil {
    private static Logger LOG = Logger.getLogger(StringUtil.class);
    static boolean strIsRight(String str) {
        return null != str && str.length() > 0;
    }
}

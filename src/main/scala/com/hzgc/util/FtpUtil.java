package com.hzgc.util;

import org.apache.log4j.Logger;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class FtpUtil implements Serializable {
    private static Logger LOG = Logger.getLogger(FtpUtil.class);

    private static Properties properties = new Properties();
    private static int ftpServerPort;

    static {
        FileInputStream in = null;
        try {
            in = new FileInputStream(FileUtil.loadResourceFile("ftpAddress.properties"));
            properties.load(in);
            ftpServerPort = Integer.parseInt(properties.getProperty("port"));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    static ByteArrayOutputStream inputStreamCacher(InputStream is) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        int len;
        try {
            while ((len = is.read(buffer)) > -1) {
                baos.write(buffer, 0, len);
            }
            baos.flush();
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
        } finally {
            try {
                baos.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            if(is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return baos;
    }
}

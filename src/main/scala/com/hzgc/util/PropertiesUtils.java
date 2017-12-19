package com.hzgc.util;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;

public class PropertiesUtils implements Serializable {
    public static Properties getProperties() {
        Properties ps = new Properties();
        try {
            InputStream is = new FileInputStream(FileUtil.loadResourceFile("kmeansClustering.properties"));
            ps.load(is);
        } catch (Exception e) {
            System.out.println(e);
        }
        return ps;
    }


}

package com.rock.flink.launcher.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.File;
import java.util.Map;

/**
 * @author cuishilei
 * @date 2019/9/26
 */
public class YarnConfLoader {

    /**
     * 从 yarn 的配置文件路径加载配置文件
     *
     * @param yarnConfDir hadoop配置文件路径 默认应该是 $HADOOP_HOME/etc/hadoop
     * @return org.apache.hadoop.yarn.conf.YarnConfiguration
     * @author cuishilei
     * @date 2019/9/27
     */
    public static YarnConfiguration getYarnConf(String yarnConfDir) {
        YarnConfiguration yarnConf = new YarnConfiguration();
        try {
            File dir = new File(yarnConfDir);
            if (dir.exists() && dir.isDirectory()) {
                File[] xmlFileList = dir.listFiles((dir1, name) -> name.endsWith(".xml"));
                if (xmlFileList != null) {
                    for (File xmlFile : xmlFileList) {
                        yarnConf.addResource(xmlFile.toURI().toURL());
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        haYarnConf(yarnConf);
        return yarnConf;
    }

    /**
     * deal yarn HA conf
     */
    private static Configuration haYarnConf(Configuration yarnConf) {
        for (Map.Entry<String, String> entry : yarnConf) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key.startsWith("yarn.resourcemanager.hostname.")) {
                String rm = key.substring("yarn.resourcemanager.hostname.".length());
                String addressKey = "yarn.resourcemanager.address." + rm;
                if (yarnConf.get(addressKey) == null) {
                    yarnConf.set(addressKey, value + ":" + YarnConfiguration.DEFAULT_RM_PORT);
                }
            }
        }
        return yarnConf;
    }
}

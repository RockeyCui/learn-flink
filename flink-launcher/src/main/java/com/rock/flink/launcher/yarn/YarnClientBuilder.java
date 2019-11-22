package com.rock.flink.launcher.yarn;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * @author cuishilei
 * @date 2019/9/26
 */
public class YarnClientBuilder {

    /**
     * yarn 客户端
     */
    private YarnClient yarnClient;

    /**
     * yarn 配置文件
     */
    private YarnConfiguration yarnConf;

    private String yarConfDirPath;

    public YarnClientBuilder(String yarConfDirPath) {
        this.yarConfDirPath = yarConfDirPath;
        yarnConf = YarnConfLoader.getYarnConf(yarConfDirPath);
        if (StringUtils.isEmpty(yarConfDirPath)) {
            throw new RuntimeException("yarn conf path is required");
        }
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(yarnConf);
        yarnClient.start();
    }

    public synchronized YarnClient getYarnClient() {
        return yarnClient;
    }

    public YarnConfiguration getYarnConf() {
        return yarnConf;
    }
}

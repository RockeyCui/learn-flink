package com.rock.flink.launcher.yarnsession;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.yarn.AbstractYarnClusterDescriptor;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author cuishilei
 * @date 2019/9/27
 */
public class YarnsessionClusterBuilder {

    private String flinkConfDir;

    private YarnClient yarnClient;

    private YarnConfiguration yarnConf;

    private String yarnConfDir;

    public YarnsessionClusterBuilder(String flinkConfDir, YarnClient yarnClient, YarnConfiguration yarnConf, String yarnConfDir) {
        this.flinkConfDir = flinkConfDir;
        this.yarnClient = yarnClient;
        this.yarnConf = yarnConf;
        this.yarnConfDir = yarnConfDir;
    }

    public ClusterClient createClusterClient() throws Exception {
        Configuration flinkConf = GlobalConfiguration.loadConfiguration(flinkConfDir);
        flinkConf.setString(ConfigConstants.PATH_HADOOP_CONFIG, yarnConfDir);
        FileSystem.initialize(flinkConf, null);
        ApplicationId applicationId = getYarnClusterApplicationId(yarnClient);
        System.out.println("applicationId=" + applicationId.toString());
        AbstractYarnClusterDescriptor clusterDescriptor = new YarnClusterDescriptor(flinkConf, yarnConf, ".", yarnClient, false);
        return clusterDescriptor.retrieve(applicationId);
    }

    private static ApplicationId getYarnClusterApplicationId(YarnClient yarnClient) throws Exception {
        ApplicationId applicationId = null;
        Set<String> set = new HashSet<>();
        set.add("Apache Flink");
        EnumSet<YarnApplicationState> enumSet = EnumSet.noneOf(YarnApplicationState.class);
        enumSet.add(YarnApplicationState.RUNNING);
        List<ApplicationReport> reportList = yarnClient.getApplications(set, enumSet);

        int maxMemory = -1;
        int maxCores = -1;
        for (ApplicationReport report : reportList) {
            //寻找正在运行的 flink session
            if (!report.getName().startsWith("Flink session cluster")) {
                continue;
            }
            if (!report.getYarnApplicationState().equals(YarnApplicationState.RUNNING)) {
                continue;
            }
            //多个 session 寻找最大配置的 session
            int thisMemory = report.getApplicationResourceUsageReport().getNeededResources().getMemory();
            int thisCores = report.getApplicationResourceUsageReport().getNeededResources().getVirtualCores();
            if (thisMemory > maxMemory || thisMemory == maxMemory && thisCores > maxCores) {
                maxMemory = thisMemory;
                maxCores = thisCores;
                applicationId = report.getApplicationId();
            }
        }
        if (StringUtils.isEmpty(applicationId.toString())) {
            throw new RuntimeException("No flink session found on yarn cluster.");
        }
        return applicationId;
    }
}

package com.rock.flink.launcher;

import com.rock.flink.launcher.perjob.PerJobClusterBuilder;
import com.rock.flink.launcher.stand.StandaloneClusterBuilder;
import com.rock.flink.launcher.yarn.YarnClientBuilder;
import com.rock.flink.launcher.yarnsession.YarnsessionClusterBuilder;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.StandaloneClusterDescriptor;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.yarn.AbstractYarnClusterDescriptor;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.File;

/**
 * @author cuishilei
 * @date 2019/9/26
 */
public class LauncherMain {

    public static void main(String[] args) throws Exception {
        String jarPath = args[0];
        String flinkConfDir = args[1];
        String yarnConfDir = args[2];
        String flinkJarPath = args[3];
        String deployType = args[4];

        //args 为提交给 jar 包的参数
        JobGraph jobGraph = getJobGraph(flinkConfDir, jarPath, args);

        Configuration newConf = new Configuration();

        YarnClientBuilder yarnClientBuilder = new YarnClientBuilder(yarnConfDir);

        YarnConfiguration yarnConf = yarnClientBuilder.getYarnConf();
        YarnClient yarnClient = yarnClientBuilder.getYarnClient();

        if (deployType.equalsIgnoreCase("per")) {
            //yarn cluster 描述
            AbstractYarnClusterDescriptor clusterDescriptor = new PerJobClusterBuilder(flinkJarPath, null, yarnClient, yarnConf).createClusterDescriptor();
            //flink 任务启动配置信息
            ClusterSpecification clusterSpecification = new ClusterSpecification.ClusterSpecificationBuilder()
                    .setMasterMemoryMB(1024)
                    .setTaskManagerMemoryMB(1024)
                    .setNumberTaskManagers(1)
                    .setSlotsPerTaskManager(8)
                    .createClusterSpecification();
            ClusterClient<ApplicationId> clusterClient = clusterDescriptor.deployJobCluster(clusterSpecification, jobGraph, true);
            ApplicationId applicationId = clusterClient.getClusterId();
            String flinkJobId = jobGraph.getJobID().toString();
            System.out.println(String.format("deploy per_job with appId: %s, jobId: %s", applicationId, flinkJobId));
            clusterClient.shutdown();
        } else if (deployType.equalsIgnoreCase("yarnsession")) {
            ClusterClient clusterClient = new YarnsessionClusterBuilder(flinkConfDir, yarnClient, yarnConf, yarnConfDir).createClusterClient();
            //JobSubmissionResult res = clusterClient.submitJob(jobGraph, null);
            clusterClient.submitJob(jobGraph, null);
            clusterClient.shutdown();
        } else {
            StandaloneClusterDescriptor clusterDescriptor = new StandaloneClusterBuilder(flinkConfDir).createClusterClient();
            ClusterSpecification clusterSpecification = (new ClusterSpecification.ClusterSpecificationBuilder()).createClusterSpecification();

            RestClusterClient<StandaloneClusterId> retrieve = clusterDescriptor.retrieve(StandaloneClusterId.getInstance());
            PackagedProgram program = new PackagedProgram(new File(jarPath), args);
            JobSubmissionResult res = retrieve.submitJob(jobGraph, null);
            System.out.println(String.format("deploy per_job with jobId: %s", res.getJobID()));
            retrieve.shutdown();
        }
    }

    /**
     * 获得提交给 JobManager 的数据结构
     *
     * @param flinkConfDir flink 的 conf 目录
     * @param jarPath      要执行的 job jar 文件
     * @param args         提交给 job jar 的参数
     * @return org.apache.flink.runtime.jobgraph.JobGraph
     * @author cuishilei
     * @date 2019/9/27
     */
    private static JobGraph getJobGraph(String flinkConfDir, String jarPath, String[] args) throws ProgramInvocationException {
        PackagedProgram program = new PackagedProgram(new File(jarPath), args);
        Configuration config = GlobalConfiguration.loadConfiguration(flinkConfDir);
        return PackagedProgramUtils.createJobGraph(program, config, 8);
    }
}

package com.rock.flink.launcher.stand;

import org.apache.flink.client.deployment.StandaloneClusterDescriptor;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.MiniClusterClient;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.util.LeaderConnectionInfo;

import java.net.InetSocketAddress;

/**
 * @author cuishilei
 * @date 2019/9/27
 */
public class StandaloneClusterBuilder {

    private String flinkConfDir;

    public StandaloneClusterBuilder(String flinkConfDir) {
        this.flinkConfDir = flinkConfDir;
    }

    public StandaloneClusterDescriptor createClusterClient() throws Exception {
        Configuration config = GlobalConfiguration.loadConfiguration(flinkConfDir);

       /* MiniClusterConfiguration.Builder configBuilder = new MiniClusterConfiguration.Builder();
        configBuilder.setConfiguration(config);
        MiniCluster miniCluster = new MiniCluster(configBuilder.build());
        MiniClusterClient clusterClient = new MiniClusterClient(config, miniCluster);
        LeaderConnectionInfo connectionInfo = clusterClient.getClusterConnectionInfo();
        InetSocketAddress address = AkkaUtils.getInetSocketAddressFromAkkaURL(connectionInfo.getAddress());
        config.setString(JobManagerOptions.ADDRESS, address.getAddress().getHostName());
        config.setInteger(JobManagerOptions.PORT, address.getPort());
        clusterClient.setDetached(true);*/

        StandaloneClusterDescriptor descriptor = new StandaloneClusterDescriptor(config);
        return descriptor;
    }
}

package com.rock.flink.launcher.perjob;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.yarn.AbstractYarnClusterDescriptor;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.File;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author cuishilei
 * @date 2019/9/27
 */
public class PerJobClusterBuilder {
    private String shipJarPath;

    private Properties confProp;

    private YarnClient yarnClient;

    private YarnConfiguration yarnConf;

    public PerJobClusterBuilder(String shipJarPath, Properties confProp, YarnClient yarnClient, YarnConfiguration yarnConf) {
        this.shipJarPath = shipJarPath;
        this.confProp = confProp;
        this.yarnClient = yarnClient;
        this.yarnConf = yarnConf;
    }

    public AbstractYarnClusterDescriptor createClusterDescriptor() throws MalformedURLException {
        Configuration newConf = new Configuration();
        if (confProp != null) {
            for (Map.Entry<Object, Object> objectObjectEntry : confProp.entrySet()) {
                newConf.setString(objectObjectEntry.getKey().toString(), objectObjectEntry.getValue().toString());
            }
        }
        AbstractYarnClusterDescriptor clusterDescriptor = getClusterDescriptor(newConf, yarnConf);
        addShipFiles(clusterDescriptor);
        return clusterDescriptor;
    }

    private void addShipFiles(AbstractYarnClusterDescriptor clusterDescriptor) throws MalformedURLException {
        if (StringUtils.isNotBlank(shipJarPath)) {
            if (!new File(shipJarPath).exists()) {
                throw new RuntimeException("The Flink jar path is not exist");
            }
        }
        if (shipJarPath != null) {
            List<File> files = new ArrayList<>();
            File[] jars = new File(shipJarPath).listFiles();
            for (File file : jars) {
                if (file.toURI().toURL().toString().contains("flink-dist")) {
                    clusterDescriptor.setLocalJarPath(new Path(file.toURI().toURL().toString()));
                } else {
                    files.add(file);
                }
            }
            clusterDescriptor.addShipFiles(files);
        }
    }

    private AbstractYarnClusterDescriptor getClusterDescriptor(Configuration configuration, YarnConfiguration yarnConfiguration) {
        return new YarnClusterDescriptor(configuration, yarnConfiguration, ".", yarnClient, false);
    }
}

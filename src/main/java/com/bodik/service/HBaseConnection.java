package com.bodik.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.Logger;

public class HBaseConnection {

	private static Configuration config;

	static {
		PropertyLoader loader = new PropertyLoader();
		String ipDB = loader.getIpDB();
		config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", ipDB);
		config.set("hbase.zookeeper.property.clientPort", loader.getPortDB());
		config.set("zookeeper.znode.parent", "/hbase-unsecure");
		config.set("hbase.master", ipDB + ":60000");
		config.set("hbase.cluster.distributed", "true");
		try {
			HBaseAdmin.checkHBaseAvailable(config);
		} catch (MasterNotRunningException e) {
			Logger.getLogger(HBaseConnection.class).error(
					"HBase is not running!", e);
		} catch (Exception e) {
			Logger.getLogger(HBaseConnection.class).error(
					"Could not connect to the table!", e);
		}
	}

	public static Configuration getConf() {
		return config;
	}

}
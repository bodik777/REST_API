package com.bodik.model;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.Logger;

public class ConnectionToHBase {

	private static String table;
	private String ipDB, portDB;
	private static Configuration config;

	public ConnectionToHBase() {
		LoadPropertiesFromFile loader = new LoadPropertiesFromFile();
		table = loader.getTable();
		ipDB = loader.getIpDB();
		portDB = loader.getPortDB();
		config = loader.getConfig();

		config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", ipDB);
		config.set("hbase.zookeeper.property.clientPort", portDB);
		config.set("zookeeper.znode.parent", "/hbase-unsecure");
		config.set("hbase.master", ipDB + ":60000");
		config.set("hbase.cluster.distributed", "true");
		try {
			HBaseAdmin.checkHBaseAvailable(config);
		} catch (MasterNotRunningException e) {
			Logger.getLogger(ConnectionToHBase.class)
					.error("HBase is not running!", e);
		} catch (Exception e) {
			Logger.getLogger(ConnectionToHBase.class).error(
					"Could not connect to the table!", e);
		}
	}

	public Configuration getConf() {
		return config;
	}

	public String getTable() {
		return table;
	}

}
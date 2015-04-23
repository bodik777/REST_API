package com.bodik.model;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

public class LoadPropertiesFromFile {
	private String table = null;
	private String ipDB = null, portDB = null;
	private Configuration config = null;

	public LoadPropertiesFromFile() {
		Properties props = new Properties();
		try {
			File file = new File("D:/config.ini");
			if (!file.exists()) {
				file.createNewFile();
				FileWriter fileWrite = new FileWriter(file);
				fileWrite.append("ipDB = localhost\r\n");
				fileWrite.flush();
				fileWrite.append("portDB = 2181\r\n");
				fileWrite.flush();
				fileWrite.append("table = examplå");
				fileWrite.flush();
				fileWrite.close();
			}
			props.load(new FileInputStream(file));
			ipDB = props.getProperty("ipDB", "localhost");
			portDB = props.getProperty("portDB", "2181");
			table = props.getProperty("table", "example");

		} catch (IOException e) {
			Logger.getLogger(ConnectionToHBase.class).error(
					"Failed to load configuration file!", e);
		}
	}

	public String getTable() {
		return table;
	}

	public String getIpDB() {
		return ipDB;
	}

	public String getPortDB() {
		return portDB;
	}

	public Configuration getConfig() {
		return config;
	}
}

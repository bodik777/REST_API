package com.bodik.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;

public class PropertyLoader {
	private String ipDB = null;
	private String portDB = null;

	public PropertyLoader() {
		Properties props = new Properties();
		try {
			File file = new File("D:/config.ini");
			props.load(new FileInputStream(file));
			this.ipDB = props.getProperty("ipDB", "localhost");
			this.portDB = props.getProperty("portDB", "2181");
		} catch (IOException e) {
			Logger.getLogger(HBaseConnection.class).error(
					"Failed to load configuration file!", e);
		}
	}

	public String getIpDB() {
		return this.ipDB;
	}

	public String getPortDB() {
		return this.portDB;
	}

}

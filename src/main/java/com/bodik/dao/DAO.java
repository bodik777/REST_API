package com.bodik.dao;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.bodik.service.HBaseConnection;

public class DAO {
	protected Table tables = null;
	protected Connection connection;

	protected DAO() {
		try {
			connection = ConnectionFactory.createConnection(HBaseConnection
					.getConf());
		} catch (IOException e) {
			Logger.getLogger(ItemTransactionDao.class).error(
					"Could not connect to the table!", e);
		}
	}

	protected Scan getScaner(String startRow,
			String stopRow, Long minStamp, Long maxStamp) {
		Scan s= null;
		try {
			s = new Scan();
			if (minStamp != null && maxStamp != null) {
				s.setTimeRange(minStamp, maxStamp);
			} else if (minStamp == null && maxStamp != null) {
				s.setTimeRange(0L, maxStamp);
			} else if (maxStamp == null && minStamp != null) {
				s.setTimeRange(minStamp, 9223372036854775807L);
			}
			if (startRow != null) {
				s.setStartRow(Bytes.toBytes(startRow));
			}
			if (stopRow != null) {
				s.setStopRow(Bytes.toBytes(stopRow));
			}
		} catch (IOException e) {
			Logger.getLogger(ItemTransactionDao.class).error("Failed to extract data!", e);
		}
		return s;
	}
	
	protected Long getMaxTimestamp(Result rr) {
		Long times = 0L;
		for (Cell cell : rr.rawCells()) {
			if (times < cell.getTimestamp()) {
				times = cell.getTimestamp();
			}
		}
		return times;
	}
}

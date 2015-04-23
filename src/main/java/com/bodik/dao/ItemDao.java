package com.bodik.dao;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.bodik.model.ConnectionToHBase;
import com.bodik.model.Item;

public class ItemDao {

	Configuration config;
	Table tables = null;

	public ItemDao() {
		ConnectionToHBase conn = new ConnectionToHBase();
		config = conn.getConf();
		try {
			Connection connection = ConnectionFactory.createConnection(config);
			tables = connection.getTable(TableName.valueOf(conn.getTable()));
		} catch (IOException e) {
			Logger.getLogger(ItemDao.class).error(
					"Could not connect to the table!", e);
		}
	}

	public ArrayList<Item> getAll() {
		ArrayList<Item> rows = new ArrayList<Item>();
		ResultScanner scanner = null;
		try {
			Scan s = new Scan();
			s.addColumn(Bytes.toBytes("data"), Bytes.toBytes("City"));
			s.addColumn(Bytes.toBytes("data"), Bytes.toBytes("Price"));
			scanner = tables.getScanner(s);
			for (Result rr : scanner) {
				rows.add(new Item(Bytes.toString(rr.getValue(
						Bytes.toBytes("data"), Bytes.toBytes("City"))), Bytes
						.toString(rr.getValue(Bytes.toBytes("data"),
								Bytes.toBytes("Price"))), getMaxTimestamp(rr)));
			}
		} catch (IOException e) {
			Logger.getLogger(ItemDao.class).error("Failed to extract data!", e);
		} finally {
			scanner.close();
		}
		return rows;
	}

	public Item getById(String id) {
		Item item = null;
		try {
			Get query = new Get(id.getBytes());
			Result res = tables.get(query);
			if (!res.isEmpty()) {
				item = new Item(Bytes.toString(res.getValue(
						Bytes.toBytes("data"), Bytes.toBytes("City"))),
						Bytes.toString(res.getValue(Bytes.toBytes("data"),
								Bytes.toBytes("Price"))), getMaxTimestamp(res));
			}
		} catch (IOException e) {
			Logger.getLogger(ItemDao.class).error("Failed to extract data!", e);
		}
		return item;
	}

	private Long getMaxTimestamp(Result rr) {
		Long times = 0L;
		for (Cell cell : rr.rawCells()) {
			if (times < cell.getTimestamp()) {
				times = cell.getTimestamp();
			}
		}
		return times;
	}

}
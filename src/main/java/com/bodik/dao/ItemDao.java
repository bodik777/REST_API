package com.bodik.dao;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.bodik.model.Item;
import com.bodik.service.ConnectionToHBase;

public class ItemDao {

	private Table tables = null;
	private final String TABLE_NAME = "tableHBaseSales";

	public ItemDao() {
		try {
			Connection connection = ConnectionFactory
					.createConnection(ConnectionToHBase.getConf());
			tables = connection.getTable(TableName.valueOf(TABLE_NAME));
		} catch (IOException e) {
			Logger.getLogger(ItemDao.class).error(
					"Could not connect to the table!", e);
		}
	}

	public ArrayList<Item> getAll(String startRow, String stopRow,
			Long minStamp, Long maxStamp, String fCity, String fPrice) {
		ArrayList<Item> rows = new ArrayList<Item>();
		try {
			Scan s = new Scan();
			s.addColumn(Bytes.toBytes("data"), Bytes.toBytes("City"));
			s.addColumn(Bytes.toBytes("data"), Bytes.toBytes("Price"));

			if (minStamp != null && maxStamp != null) {
				s.setTimeRange(minStamp, maxStamp);
			} else if (minStamp == null && maxStamp != null) {
				s.setTimeRange(0L, maxStamp);
			} else if (maxStamp == null && minStamp != null) {
				s.setTimeRange(minStamp, 1429886358562L);
			}
			if (startRow != null) {
				s.setStartRow(Bytes.toBytes(startRow));
			}
			if (stopRow != null) {
				s.setStopRow(Bytes.toBytes(stopRow));
			}
			FilterList flMaster = new FilterList(
					FilterList.Operator.MUST_PASS_ALL);
			if (fCity != null) {
				Filter filterCity = new SingleColumnValueFilter(
						Bytes.toBytes("data"), Bytes.toBytes("City"),
						CompareOp.EQUAL, new SubstringComparator(fCity));
				flMaster.addFilter(filterCity);
			}
			if (fPrice != null) {
				Filter filterPrice = new SingleColumnValueFilter(
						Bytes.toBytes("data"), Bytes.toBytes("Price"),
						CompareOp.EQUAL, new SubstringComparator(fPrice));
				flMaster.addFilter(filterPrice);
			}
			if (flMaster.hasFilterRow()) {
				s.setFilter(flMaster);
			}

			ResultScanner scanner = tables.getScanner(s);
			for (Result rr : scanner) {
				rows.add(new Item(Bytes.toString(rr.getRow()), Bytes
						.toString(rr.getValue(Bytes.toBytes("data"),
								Bytes.toBytes("City"))),
						Bytes.toString(rr.getValue(Bytes.toBytes("data"),
								Bytes.toBytes("Price"))), getMaxTimestamp(rr)));
			}
			scanner.close();
		} catch (IOException e) {
			Logger.getLogger(ItemDao.class).error("Failed to extract data!", e);
		}
		return rows;
	}

	public Item getById(String id) {
		Item item = null;
		try {
			Get query = new Get(id.getBytes());
			Result res = tables.get(query);
			if (!res.isEmpty()) {
				item = new Item(Bytes.toString(res.getRow()),
						Bytes.toString(res.getValue(Bytes.toBytes("data"),
								Bytes.toBytes("City"))), Bytes.toString(res
								.getValue(Bytes.toBytes("data"),
										Bytes.toBytes("Price"))),
						getMaxTimestamp(res));
			}
		} catch (IOException e) {
			Logger.getLogger(ItemDao.class).error("Failed to extract data!", e);
		}
		return item;
	}

	public void putToTable(Item item) {
		Put p = new Put(Bytes.toBytes(item.getRow()));
		try {
			p.addImmutable(Bytes.toBytes("data"), Bytes.toBytes("City"),
					Bytes.toBytes(item.getCity()));
			p.addImmutable(Bytes.toBytes("data"), Bytes.toBytes("Price"),
					Bytes.toBytes(item.getPrice()));
			tables.put(p);
		} catch (IOException e) {
			Logger.getLogger(ItemDao.class).error("Error adding entry!", e);
		}
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
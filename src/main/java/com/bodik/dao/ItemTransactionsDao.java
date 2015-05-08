package com.bodik.dao;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.bodik.model.ItemTransaction;

public class ItemTransactionsDao extends DAO {
	private final String TABLE_NAME = "tableHBaseSales";
	private final String COLUMN_FAMILY = "data";

	public ItemTransactionsDao() {
		super();
		try {
			tables = super.connection.getTable(TableName.valueOf(TABLE_NAME));
		} catch (IOException e) {
			Logger.getLogger(ItemTransactionsDao.class).error(
					"Could not connect to the table!", e);
		}
	}

	public ArrayList<ItemTransaction> getAll(String startRow, String stopRow,
			Long minStamp, Long maxStamp, String fCountry, String fProduct) {
		ArrayList<ItemTransaction> rows = new ArrayList<ItemTransaction>();
		try {
			Scan s = getScaner(startRow, stopRow, minStamp, maxStamp);
			s.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("Country"));
			s.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("Product"));

			ArrayList<String> colList = new ArrayList<String>();
			ArrayList<String> valList = new ArrayList<String>();
			colList.add("Country");
			valList.add(fCountry);
			colList.add("Product");
			valList.add(fProduct);
			FilterList flMaster = getFilter(COLUMN_FAMILY, colList, valList);
			if (flMaster.hasFilterRow()) {
				s.setFilter(flMaster);
			}

			ResultScanner scanner = tables.getScanner(s);
			for (Result rr : scanner) {
				rows.add(new ItemTransaction(Bytes.toString(rr.getRow()), Bytes
						.toString(rr.getValue(Bytes.toBytes(COLUMN_FAMILY),
								Bytes.toBytes("Country"))), Bytes.toString(rr
						.getValue(Bytes.toBytes(COLUMN_FAMILY),
								Bytes.toBytes("Product"))), getMaxTimestamp(rr)));
			}
			scanner.close();
		} catch (IOException e) {
			Logger.getLogger(ItemTransactionsDao.class).error(
					"Failed to extract data!", e);
		}
		return rows;
	}

	public ItemTransaction getById(String id) {
		ItemTransaction item = null;
		try {
			Get query = new Get(id.getBytes());
			Result res = tables.get(query);
			if (!res.isEmpty()) {
				item = new ItemTransaction(Bytes.toString(res.getRow()),
						Bytes.toString(res.getValue(
								Bytes.toBytes(COLUMN_FAMILY),
								Bytes.toBytes("Country"))), Bytes.toString(res
								.getValue(Bytes.toBytes(COLUMN_FAMILY),
										Bytes.toBytes("Product"))),
						getMaxTimestamp(res));
			}
		} catch (IOException e) {
			Logger.getLogger(ItemTransactionsDao.class).error(
					"Failed to extract data!", e);
		}
		return item;
	}

	public void putToTable(ItemTransaction item) {
		Put p = new Put(Bytes.toBytes(item.getRow()));
		try {
			p.addImmutable(Bytes.toBytes(COLUMN_FAMILY),
					Bytes.toBytes("Country"), Bytes.toBytes(item.getCountry()));
			p.addImmutable(Bytes.toBytes(COLUMN_FAMILY),
					Bytes.toBytes("Product"), Bytes.toBytes(item.getProduct()));
			tables.put(p);
		} catch (IOException e) {
			Logger.getLogger(ItemTransactionsDao.class).error(
					"Error adding entry!", e);
		}
	}

}

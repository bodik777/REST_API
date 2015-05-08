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

import com.bodik.model.ItemSales;

public class ItemSalesDao extends DAO {
	private final String TABLE_NAME = "tableHBaseSales";
	private final String COLUMN_FAMILY = "data";

	public ItemSalesDao() {
		super();
		try {
			tables = super.connection.getTable(TableName.valueOf(TABLE_NAME));
		} catch (IOException e) {
			Logger.getLogger(ItemSalesDao.class).error(
					"Could not connect to the table!", e);
		}
	}

	public ArrayList<ItemSales> getAll(String startRow, String stopRow,
			Long minStamp, Long maxStamp, String fCity, String fPrice) {
		ArrayList<ItemSales> rows = new ArrayList<ItemSales>();
		try {
			Scan s = getScaner(startRow, stopRow, minStamp, maxStamp);
			s.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("City"));
			s.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("Price"));

			ArrayList<String> colList = new ArrayList<String>();
			ArrayList<String> valList = new ArrayList<String>();
			colList.add("City");
			valList.add(fCity);
			colList.add("Price");
			valList.add(fPrice);
			FilterList flMaster = getFilter(COLUMN_FAMILY, colList, valList);
			if (flMaster.hasFilterRow()) {
				s.setFilter(flMaster);
			}

			ResultScanner scanner = tables.getScanner(s);
			for (Result rr : scanner) {
				rows.add(new ItemSales(Bytes.toString(rr.getRow()), Bytes
						.toString(rr.getValue(Bytes.toBytes(COLUMN_FAMILY),
								Bytes.toBytes("City"))), Bytes.toString(rr
						.getValue(Bytes.toBytes(COLUMN_FAMILY),
								Bytes.toBytes("Price"))), getMaxTimestamp(rr)));
			}
			scanner.close();
		} catch (IOException e) {
			Logger.getLogger(ItemSalesDao.class).error(
					"Failed to extract data!", e);
		}
		return rows;
	}

	public ItemSales getById(String id) {
		ItemSales item = null;
		try {
			Get query = new Get(id.getBytes());
			Result res = tables.get(query);
			if (!res.isEmpty()) {
				item = new ItemSales(Bytes.toString(res.getRow()),
						Bytes.toString(res.getValue(
								Bytes.toBytes(COLUMN_FAMILY),
								Bytes.toBytes("City"))), Bytes.toString(res
								.getValue(Bytes.toBytes(COLUMN_FAMILY),
										Bytes.toBytes("Price"))),
						getMaxTimestamp(res));
			}
		} catch (IOException e) {
			Logger.getLogger(ItemSalesDao.class).error(
					"Failed to extract data!", e);
		}
		return item;
	}

	public void putToTable(ItemSales item) {
		Put p = new Put(Bytes.toBytes(item.getRow()));
		try {
			p.addImmutable(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("City"),
					Bytes.toBytes(item.getCity()));
			p.addImmutable(Bytes.toBytes(COLUMN_FAMILY),
					Bytes.toBytes("Price"), Bytes.toBytes(item.getPrice()));
			tables.put(p);
		} catch (IOException e) {
			Logger.getLogger(ItemSalesDao.class)
					.error("Error adding entry!", e);
		}
	}

}
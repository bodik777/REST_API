package com.bodik.tests;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.jboss.resteasy.client.ClientRequest;
import org.jboss.resteasy.client.ClientResponse;
import org.junit.Test;

import com.bodik.model.Item;
import com.bodik.service.ConnectionToHBase;

@SuppressWarnings("deprecation")
public class ItemDaoTest {
	private final String ROOT_URL = "http://localhost:8080/REST_API/items";
	private final String TABLE_NAME = "tableHBaseSales";

	@Test
	public void testGetAll() {
		try {
			ClientRequest request = new ClientRequest(ROOT_URL);
			request.accept("application/json");
			ClientResponse<String> response = request.get(String.class);

			BufferedReader br = new BufferedReader(new InputStreamReader(
					new ByteArrayInputStream(response.getEntity().getBytes())));
			String temp;
			String res = "";
			System.out.println("Request - getAll: Status: "
					+ response.getStatus() + "; Output ->");
			while ((temp = br.readLine()) != null) {
				res += temp;
			}
			System.out.println(res);

			assertTrue(response.getStatus() == 200);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Test
	public void testGetAllParams() {
		try {
			ClientRequest request = new ClientRequest(
					ROOT_URL
							+ "?startRow=1&stopRow=999&minStamp=0&maxStamp=1429886358562&fCity=Basildon&fPrice=1200");
			request.accept("application/json");
			ClientResponse<String> response = request.get(String.class);

			BufferedReader br = new BufferedReader(new InputStreamReader(
					new ByteArrayInputStream(response.getEntity().getBytes())));
			String temp;
			String res = "";
			System.out.println("Request - getAllParams: Status: "
					+ response.getStatus() + "; Output ->");
			while ((temp = br.readLine()) != null) {
				res += temp;
			}
			System.out.println(res);

			assertTrue(response.getStatus() == 200);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Test
	public void testGetById() {
		try {
			ClientRequest request = new ClientRequest(ROOT_URL + "/1");
			request.accept("application/json");
			ClientResponse<String> response = request.get(String.class);

			BufferedReader br = new BufferedReader(new InputStreamReader(
					new ByteArrayInputStream(response.getEntity().getBytes())));
			String temp;
			String res = "";
			int status = response.getStatus();
			System.out.println("Request - getById: Status: " + status
					+ "; Output ->");
			while ((temp = br.readLine()) != null) {
				res += temp;
			}
			System.out.println(res);
			assertTrue(status == 200);
			assertEquals(
					res,
					"{\"row\":\"1\",\"city\":\"Basildon\",\"price\":\"1200\",\"timestamp\":1429886358462}");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testPutToTable() {
		Item item = new Item("testRow", "testCity", "999999999");
		Connection connection;
		Table tables = null;
		try {
			connection = ConnectionFactory.createConnection(ConnectionToHBase
					.getConf());
			tables = connection.getTable(TableName.valueOf(TABLE_NAME));
		} catch (IOException e) {
			e.printStackTrace();
		}
		Put p = new Put(Bytes.toBytes(item.getRow()));
		try {
			p.addImmutable(Bytes.toBytes("data"), Bytes.toBytes("City"),
					Bytes.toBytes(item.getCity()));
			p.addImmutable(Bytes.toBytes("data"), Bytes.toBytes("Price"),
					Bytes.toBytes(item.getPrice()));
			tables.put(p);
			assertTrue("Adding success!", true);
		} catch (IOException e) {
			e.printStackTrace();
		}
		List<Delete> list = new ArrayList<Delete>();
		Delete del = new Delete(Bytes.toBytes("testRow"));
		list.add(del);
		try {
			tables.delete(list);
			assertTrue("Deleting success!", true);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}

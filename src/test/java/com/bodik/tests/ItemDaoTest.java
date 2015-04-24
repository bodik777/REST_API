package com.bodik.tests;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;

import org.jboss.resteasy.client.ClientRequest;
import org.jboss.resteasy.client.ClientResponse;
import org.junit.Test;

@SuppressWarnings("deprecation")
public class ItemDaoTest {
	final String ROOT_URL = "http://localhost:8080/REST_API/items/";

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
			System.out.println("Request - getById: Status: "
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
			ClientRequest request = new ClientRequest(ROOT_URL + "1230774660s");
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
			assertEquals(res,
					"{\"city\":\"Holte\",\"price\":\"1200\",\"timestamp\":1428655187781}");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}

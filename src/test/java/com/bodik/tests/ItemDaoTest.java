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
			String output;
			System.out.println("Request - getAll: Status: "
					+ response.getStatus() + "; Output ->");
			while ((output = br.readLine()) != null) {
				System.out.println(output);
			}

			assertTrue(response.getStatus() == 200);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Test
	public void testGetById() {
		try {
			ClientRequest request = new ClientRequest(ROOT_URL + "1230774660");
			request.accept("application/json");
			ClientResponse<String> response = request.get(String.class);

			BufferedReader br = new BufferedReader(new InputStreamReader(
					new ByteArrayInputStream(response.getEntity().getBytes())));
			String output;
			System.out.println("Request - getById: Status: "
					+ response.getStatus() + "; Output ->");
			while ((output = br.readLine()) != null) {
				System.out.println(output);
			}

			assertTrue(response.getStatus() == 200);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}

package com.bodik.resources;

import java.util.ArrayList;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.bodik.dao.ItemDao;
import com.bodik.model.Item;

@Path("items")
public class ItemsProduction {

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public ArrayList<Item> items() {
		return new ItemDao().getAll();
	}

	@GET
	@Path("{id}")
	@Produces(MediaType.APPLICATION_JSON)
	public Object item(@PathParam("id") String id) {
		Item item = new ItemDao().getById(id);
		if (item == null) {
			return Response.status(404).build();
		}
		return item;
	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response createRow(Item item) {
		if (item.getRow() == null) {
			return Response.status(400).build();
		}
		new ItemDao().putToTabe(item);
		return Response.status(200).build();
	}

}

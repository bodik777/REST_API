package com.bodik.resources;

import java.util.ArrayList;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.bodik.dao.ItemTransactionsDao;
import com.bodik.model.ItemTransaction;

@Path("itemsTransactions")
public class ItemsTransactionProduction {
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public ArrayList<ItemTransaction> getAll(
			@QueryParam("startRow") String startRow,
			@QueryParam("stopRow") String stopRow,
			@QueryParam("minStamp") Long minStamp,
			@QueryParam("maxStamp") Long maxStamp,
			@QueryParam("fCountry") String fCountry,
			@QueryParam("fProduct") String fProduct) {
		return new ItemTransactionsDao().getAll(startRow, stopRow, minStamp, maxStamp,
				fCountry, fProduct);
	}

	@GET
	@Path("{id}")
	@Produces(MediaType.APPLICATION_JSON)
	public Object getById(@PathParam("id") String id) {
		ItemTransaction item = new ItemTransactionsDao().getById(id);
		if (item == null) {
			return Response.status(404).build();
		}
		return item;
	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response createRow(ItemTransaction item) {
		if (item.getRow() == null) {
			return Response.status(400).build();
		}
		new ItemTransactionsDao().putToTable(item);
		return Response.status(200).build();
	}

}

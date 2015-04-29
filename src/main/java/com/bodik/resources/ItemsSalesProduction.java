package com.bodik.resources;

import java.util.ArrayList;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.bodik.dao.ItemSalesDao;
import com.bodik.model.ItemSales;

@Path("itemsSales")
public class ItemsSalesProduction {

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public ArrayList<ItemSales> getAll(@QueryParam("startRow") String startRow,
			@QueryParam("stopRow") String stopRow,
			@QueryParam("minStamp") Long minStamp,
			@QueryParam("maxStamp") Long maxStamp,
			@DefaultValue("") @QueryParam("fCity") String fCity,
			@DefaultValue("") @QueryParam("fPrice") String fPrice) {
		return new ItemSalesDao().getAll(startRow, stopRow, minStamp, maxStamp,
				fCity, fPrice);
	}

	@GET
	@Path("{id}")
	@Produces(MediaType.APPLICATION_JSON)
	public Object getById(@PathParam("id") String id) {
		ItemSales item = new ItemSalesDao().getById(id);
		if (item == null) {
			return Response.status(404).build();
		}
		return item;
	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response createRowSales(ItemSales item) {
		if (item.getRow() == null) {
			return Response.status(400).build();
		}
		new ItemSalesDao().putToTable(item);
		return Response.status(200).build();
	}

}

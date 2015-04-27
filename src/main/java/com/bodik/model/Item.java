package com.bodik.model;

public class Item {
	private String row;
	private String city;
	private String price;
	private Long timestamp;

	public Item() {
	}

	public Item(String row, String city, String price, Long timestamp) {
		this.row = row;
		this.city = city;
		this.price = price;
		this.timestamp = timestamp;
	}

	public Item(String row, String city, String price) {
		this.row = row;
		this.city = city;
		this.price = price;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getPrice() {
		return price;
	}

	public void setPrice(String price) {
		this.price = price;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	public String getRow() {
		return row;
	}

	public void setRow(String row) {
		this.row = row;
	}
}
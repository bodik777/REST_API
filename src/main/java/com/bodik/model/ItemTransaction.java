package com.bodik.model;

public class ItemTransaction {
	private String row;
	private String country;
	private String product;
	private Long timestamp;

	public ItemTransaction() {
	}

	public ItemTransaction(String row, String country, String product,
			Long timestamp) {
		this.row = row;
		this.country = country;
		this.product = product;
		this.timestamp = timestamp;
	}

	public ItemTransaction(String row, String country, String product) {
		this.row = row;
		this.country = country;
		this.product = product;
	}

	public String getRow() {
		return row;
	}

	public void setRow(String row) {
		this.row = row;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getProduct() {
		return product;
	}

	public void setProduct(String product) {
		this.product = product;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}
}
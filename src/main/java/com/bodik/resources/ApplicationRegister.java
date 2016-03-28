package com.bodik.resources;

import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;

@ApplicationPath("")
public class ApplicationRegister extends Application {
	private Set<Object> singletons = new HashSet<Object>();
	private Set<Class<?>> empty = new HashSet<Class<?>>();

	public ApplicationRegister() {
		// ADD YOUR RESTFUL RESOURCES HERE
		this.singletons.add(new ItemsSalesProduction());
		this.singletons.add(new ItemsTransactionProduction());
	}

	public Set<Class<?>> getClasses() {
		return this.empty;
	}

	public Set<Object> getSingletons() {
		return this.singletons;
	}
}
package org.domainobject.foo;

import org.domainobject.persister.DomainObject;

public class Supplier extends DomainObject {

	private int id;
	private String name;
	private String telephone1;
	private String email1;
	private String url1;


	public int getId()
	{
		return id;
	}


	public void setId(int id)
	{
		this.id = id;
	}


	public String getName()
	{
		return name;
	}


	public void setName(String name)
	{
		this.name = name;
	}


	public String getTelephone1()
	{
		return telephone1;
	}


	public void setTelephone1(String telephone1)
	{
		this.telephone1 = telephone1;
	}


	public String getEmail1()
	{
		return email1;
	}


	public void setEmail1(String email1)
	{
		this.email1 = email1;
	}


	public String getUrl1()
	{
		return url1;
	}


	public void setUrl1(String url1)
	{
		this.url1 = url1;
	}

}

package org.domainobject.orm.core;

import java.sql.Connection;

class AbstractEntity {

	private String name;
	private String schema;
	private Column[] cols;
	private Column[] pk;

	public String getName()
	{
		// TODO Auto-generated method stub
		return null;
	}

	public String getFrom()
	{
		// TODO Auto-generated method stub
		return null;
	}

	public Connection getConnection()
	{
		// TODO Auto-generated method stub
		return null;
	}

	public Column[] getColumns()
	{
		// TODO Auto-generated method stub
		return null;
	}

	public Column[] getPrimaryKeyColumns()
	{
		// TODO Auto-generated method stub
		return null;
	}

	public void setPrimaryKeyColumns(Column[] columns)
	{
		// TODO Auto-generated method stub

	}

	public Column[] getForeignKeyColumns(Entity parent)
	{
		// TODO Auto-generated method stub
		return null;
	}

	public void setForeignKeyColumns(Entity parent, Column[] columns)
	{
		// TODO Auto-generated method stub

	}

}

package org.domainobject.core;

import java.sql.Connection;

abstract class AbstractEntity implements Entity {

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

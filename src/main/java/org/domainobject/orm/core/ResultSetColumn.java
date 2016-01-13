package org.domainobject.orm.core;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.domainobject.orm.exception.DomainObjectSQLException;

public class ResultSetColumn implements Column {

	private final ResultSetMetaData rsmd;
	private final int columnIndex;


	public ResultSetColumn(ResultSetMetaData rsmd, int columnIndex)
	{
		this.rsmd = rsmd;
		this.columnIndex = columnIndex;
	}


	public Entity getEntity()
	{
		return null;
	}


	public String getName()
	{
		try {
			return rsmd.getColumnLabel(columnIndex);
		}
		catch (SQLException e) {
			throw new DomainObjectSQLException(e);
		}
	}


	public int getDataType()
	{
		try {
			return rsmd.getColumnType(columnIndex);
		}
		catch (SQLException e) {
			return 0;
		}
	}


	public int getColumnSize()
	{
		try {
			return rsmd.getPrecision(columnIndex);
		}
		catch (SQLException e) {
			return 0;
		}
	}


	public int getOrdinalPosition()
	{
		return columnIndex;
	}


	public boolean isNullable()
	{
		return true;
	}


	public boolean isAutoIncrement()
	{
		return false;
	}

}

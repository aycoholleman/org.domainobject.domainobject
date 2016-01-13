package org.domainobject.core;


import java.sql.ResultSetMetaData;
import java.sql.SQLException;


public class NestedQueryColumn implements Column {

	private final Entity entity;
	private final String name;
	private final int dataType;
	private final int columnSize;
	private final int ordinalPosition;

	// Not relevant for columns in a QueryEntity; we're never going to INSERT into
	// that type of entity.
	private final boolean isNullable = false;
	private final boolean isAutoIncrement = false;

	public NestedQueryColumn(Entity entity, ResultSetMetaData rsmd, int columnIndex) throws SQLException
	{
		this.entity = entity;
		name = rsmd.getColumnLabel(columnIndex);
		dataType = rsmd.getColumnType(columnIndex);
		columnSize = rsmd.getPrecision(columnIndex);
		ordinalPosition = columnIndex;
	}
	
	public Entity getEntity()
	{
		return entity;
	}

	public String getName()
	{
		return name;
	}

	public int getDataType()
	{
		return dataType;
	}

	public int getColumnSize()
	{
		return columnSize;
	}

	public int getOrdinalPosition()
	{
		return ordinalPosition;
	}

	public boolean isNullable()
	{
		return isNullable;
	}

	public boolean isAutoIncrement()
	{
		return isAutoIncrement;
	}

	@Override
	public boolean equals(Object obj)
	{
		if(this == obj) {
			return true;
		}
		if (obj == null || !(obj instanceof NestedQueryColumn)) {
			return false;
		}
		NestedQueryColumn other = (NestedQueryColumn) obj;
		return name.equals(other.name) && entity.equals(other.entity);
	}
	
	@Override
	public int hashCode() {
		int hash = NestedQueryColumn.class.hashCode();
		hash = (hash * 31) + entity.hashCode();
		hash = (hash * 31) + name.hashCode();
		return hash;
	}
}

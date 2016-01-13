package org.domainobject.core;

import java.sql.ResultSet;
import java.sql.SQLException;

class StaticColumn implements Column {

	private static enum ATTRIBUTE
	{
		COLUMN_NAME(4), DATA_TYPE(5), COLUMN_SIZE(7), ORDINAL_POSITION(17), IS_NULLABLE(18), IS_AUTOINCREMENT(23);

		private final int columnIndex;


		ATTRIBUTE(int columnIndex)
		{
			this.columnIndex = columnIndex;
		}
	}

	private final Entity entity;
	private final String name;
	private final int dataType;
	private final int columnSize;
	private final int ordinalPosition;
	private final boolean isNullable;
	private final boolean isAutoIncrement;


	public StaticColumn(Entity entity, ResultSet rs) throws SQLException
	{
		this.entity = entity;
		name = rs.getString(ATTRIBUTE.COLUMN_NAME.columnIndex);
		dataType = rs.getInt(ATTRIBUTE.DATA_TYPE.columnIndex);
		columnSize = rs.getInt(ATTRIBUTE.COLUMN_SIZE.columnIndex);
		ordinalPosition = rs.getInt(ATTRIBUTE.ORDINAL_POSITION.columnIndex);
		isNullable = rs.getString(ATTRIBUTE.IS_NULLABLE.columnIndex).equals("YES");
		if (rs.getMetaData().getColumnCount() >= ATTRIBUTE.IS_AUTOINCREMENT.columnIndex) {
			isAutoIncrement = rs.getString(ATTRIBUTE.IS_AUTOINCREMENT.columnIndex).equals("YES");
		}
		else {
			isAutoIncrement = false;
		}
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
		if (this == obj) {
			return true;
		}
		if (obj == null || !(obj instanceof StaticColumn)) {
			return false;
		}
		StaticColumn other = (StaticColumn) obj;
		return (name.equals(other.name) && entity.equals(other.entity));
	}


	@Override
	public int hashCode()
	{
		StringBuilder sb = new StringBuilder(64);
		sb.append(entity.getConnection()).append("&&&");
		sb.append(entity).append("&&&");
		sb.append(name);
		return sb.toString().hashCode();
	}

}

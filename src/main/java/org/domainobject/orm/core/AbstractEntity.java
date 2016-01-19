package org.domainobject.orm.core;

import java.util.HashMap;
import java.util.Map;

public class AbstractEntity {

	private String name;
	private String schema;
	private Column[] columns;
	private Column[] primaryKey;
	private Map<AbstractEntity, Column[]> parents;
	private Map<AbstractEntity, Column[]> children;

	public String getName()
	{
		return name;
	}

	public void setName(String name)
	{
		this.name = name;
	}

	public String getSchema()
	{
		return schema;
	}

	public void setSchema(String schema)
	{
		this.schema = schema;
	}

	public Column[] getColumns()
	{
		return columns;
	}

	public void setColumns(Column[] columns)
	{
		this.columns = columns;
	}

	public Column[] getPrimaryKey()
	{
		return primaryKey;
	}

	public void setPrimaryKey(Column[] primaryKey)
	{
		this.primaryKey = primaryKey;
	}

	public void addParent(AbstractEntity parent, Column[] foreignKey)
	{
		if (parents == null)
			parents = new HashMap<>();

	}

}

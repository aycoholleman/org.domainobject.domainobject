package org.domainobject.orm.core;

import static java.sql.Types.BIGINT;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.INTEGER;
import static java.sql.Types.NUMERIC;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.TINYINT;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.domainobject.orm.exception.DomainObjectSQLException;
import org.domainobject.orm.exception.MetaDataAssemblyException;
import org.domainobject.util.ArrayUtil;

class StaticEntity implements Entity {

	protected final Connection connection;
	protected final String name;
	protected final String schema;
	protected final Column[] columns;

	public StaticEntity(String name, Connection conn)
	{
		this.name = name;
		this.connection = conn;
		try {
			this.schema = conn.getCatalog();
		}
		catch (SQLException e) {
			throw new DomainObjectSQLException(e);
		}
		if (this.schema == null) {
			throw new MetaDataAssemblyException(
					"Cannot determine schema of entity (java.sql.Connection.getCatalog() returned null)");
		}
		this.columns = loadColumns();
	}

	public StaticEntity(String name, String schema, Connection conn)
	{
		this.name = name;
		this.connection = conn;
		this.schema = schema;
		this.columns = loadColumns();
	}

	public String getName()
	{
		return name;
	}

	public String getSchema()
	{
		return schema;
	}

	public Connection getConnection()
	{
		return connection;
	}

	private String from;

	public String getFrom()
	{
		if (from == null) {
			try {
				String quote = connection.getMetaData().getIdentifierQuoteString();
				// @formatter:off
				from =  new StringBuilder()
							.append(quote)
							.append(schema)
							.append(quote)
							.append('.')
							.append(quote)
							.append(name)
							.append(quote)
							.toString();
				// @formatter:on
			}
			catch (SQLException e) {
				throw new DomainObjectSQLException(e);
			}
		}
		return from;
	}

	public Column[] getColumns()
	{
		return columns;
	}

	private Column[] primaryKeyColumns;

	public Column[] getPrimaryKeyColumns()
	{
		if (primaryKeyColumns == null) {
			try {
				DatabaseMetaData dbmd = connection.getMetaData();
				ResultSet rs = dbmd.getPrimaryKeys(schema, null, name);
				TreeMap<Short, Column> treeMap = new TreeMap<Short, Column>();
				while (rs.next()) {
					treeMap.put(rs.getShort(5), findColumn(rs.getString(4)));
				}
				primaryKeyColumns = treeMap.values().toArray(new Column[treeMap.size()]);
			}
			catch (SQLException e) {
				throw new DomainObjectSQLException(e);
			}
		}
		return primaryKeyColumns;
	}

	public void setPrimaryKeyColumns(Column[] columns)
	{
		this.primaryKeyColumns = columns;
	}

	private Column[] generatedKeyColumns;

	public Column[] getGeneratedKeyColumns()
	{
		if (generatedKeyColumns == null) {
			Column c = getAutoIncrementColumn();
			if (c != null) {
				this.generatedKeyColumns = new Column[] { c };
			}
			else {
				if (getPrimaryKeyColumns().length != 1) {
					this.generatedKeyColumns = new Column[0];
				}
				else {
					int datatype = this.primaryKeyColumns[0].getDataType();
					if (ArrayUtil.in(datatype, INTEGER, NUMERIC, BIGINT, DECIMAL, DOUBLE, SMALLINT,
							TINYINT)) {
						this.generatedKeyColumns = new Column[] { this.primaryKeyColumns[0] };
					}
					else {
						this.generatedKeyColumns = new Column[0];
					}
				}
			}
		}
		return this.generatedKeyColumns;
	}

	public void setGeneratedKeyColumns(Column[] columns)
	{
		this.generatedKeyColumns = columns;
	}

	private Column autoIncrementColumn;

	public Column getAutoIncrementColumn()
	{
		if (autoIncrementColumn == null) {
			for (Column column : columns) {
				if (column.isAutoIncrement()) {
					autoIncrementColumn = column;
					break;
				}
			}
		}
		return autoIncrementColumn;
	}

	private final Map<Entity, Column[]> foreignKeys = new HashMap<Entity, Column[]>(4);

	public Column[] getForeignKeyColumns(Entity parent)
	{
		Column[] fk = foreignKeys.get(parent);
		if (fk == null) {
			if (!(parent instanceof StaticEntity)) {
				String msg = "Cannot establish foreign key to non-static entity \""
						+ parent.getName() + "\" (" + parent.getClass() + ")";
				throw new MetaDataAssemblyException(msg);
			}
			StaticEntity te = (StaticEntity) parent;
			TreeMap<Short, Column> treeMap = new TreeMap<Short, Column>();
			try {
				DatabaseMetaData dbmd = connection.getMetaData();
				ResultSet rs = dbmd.getCrossReference(te.schema, null, te.name, schema, null, name);
				while (rs.next()) {
					treeMap.put(rs.getShort(5), findColumn(rs.getString(4)));
				}
			}
			catch (SQLException e) {
				throw new DomainObjectSQLException(e);
			}
			fk = treeMap.values().toArray(new Column[treeMap.size()]);
			foreignKeys.put(parent, fk);
		}
		return fk;
	}

	public void setForeignKeyColumns(Entity parent, Column[] columns)
	{
		foreignKeys.put(parent, columns);
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj) {
			return true;
		}
		if (obj == null || !(obj instanceof StaticEntity)) {
			return false;
		}
		StaticEntity other = (StaticEntity) obj;
		return name.equals(other.name) && schema.equals(other.schema)
				&& connection.equals(other.connection);
	}

	public int hashCode()
	{
		int hash = StaticEntity.class.hashCode();
		hash = (hash * 31) + name.hashCode();
		hash = (hash * 31) + schema.hashCode();
		hash = (hash * 31) + connection.hashCode();
		return hash;
	}

	private Column[] loadColumns()
	{
		try {
			List<Column> list = new ArrayList<Column>();
			DatabaseMetaData dbmd = connection.getMetaData();
			ResultSet rs = dbmd.getColumns(null, null, name, null);
			while (rs.next()) {
				list.add(new StaticColumn(this, rs));
			}
			return list.toArray(new StaticColumn[list.size()]);
		}
		catch (SQLException e) {
			throw new DomainObjectSQLException(e);
		}
	}

	private Column findColumn(String name)
	{
		for (Column column : columns) {
			if (column.getName().equals(name)) {
				return column;
			}
		}
		return null;
	}

}

package org.domainobject.orm.core;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.domainobject.orm.exception.DomainObjectSQLException;

public class InformationSchema {

	protected final Connection conn;

	public InformationSchema(Connection conn)
	{
		this.conn = conn;
	}

	public String getDatabaseSchema()
	{
		try {
			return conn.getCatalog();
		}
		catch (SQLException e) {
			throw new DomainObjectSQLException(e);
		}
	}

	/**
	 * Counts the number of database entities with the specified name and within
	 * specified database schema (may be {@code null}).
	 * 
	 * @param name
	 * @param schema
	 * @return
	 */
	public int countEntities(String name, String schema)
	{
		try {
			DatabaseMetaData dbmd = conn.getMetaData();
			ResultSet rs = dbmd.getTables(schema, null, name, null);
			int result = 0;
			while (rs.next()) {
				result++;
			}
			return result;
		}
		catch (SQLException e) {
			throw new DomainObjectSQLException(e);
		}

	}

	public Column[] getColumns(String name, String schema)
	{
		try {
			List<Column> list = new ArrayList<>();
			DatabaseMetaData dbmd = conn.getMetaData();
			ResultSet rs = dbmd.getColumns(null, null, name, null);
			while (rs.next())
				list.add(new StaticColumn(rs));
			return list.toArray(new StaticColumn[list.size()]);
		}
		catch (SQLException e) {
			throw new DomainObjectSQLException(e);
		}
	}

	// public Column[] getPrimaryKeyColumns(String name, String schema)
	// {
	// try {
	// DatabaseMetaData dbmd = conn.getMetaData();
	// ResultSet rs = dbmd.getPrimaryKeys(schema, null, name);
	// TreeMap<Short, Column> treeMap = new TreeMap<>();
	// while (rs.next()) {
	// treeMap.put(rs.getShort(5), findColumn(rs.getString(4)));
	// }
	// primaryKeyColumns = treeMap.values().toArray(new Column[treeMap.size()]);
	// }
	// catch (SQLException e) {
	// throw new DomainObjectSQLException(e);
	// }
	// }

	public String getFullyQualifiedName(String name, String schema)
	{
		try {
			String quote = conn.getMetaData().getIdentifierQuoteString();
			// @formatter:off
			return new StringBuilder(50)
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
}

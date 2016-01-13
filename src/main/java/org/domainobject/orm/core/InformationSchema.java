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

	public String getCatalog()
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

}

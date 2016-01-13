package org.domainobject.orm.core;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import org.domainobject.orm.exception.DomainObjectSQLException;

public class DatabaseInfo {

	static DatabaseInfo create(Connection connection)
	{
		try {
			DatabaseMetaData dbmd = connection.getMetaData();
			DatabaseInfo dbi = new DatabaseInfo();
			dbi.vendor = dbmd.getDatabaseProductName();
			dbi.majorVersion = dbmd.getDatabaseMajorVersion();
			dbi.minorVersion = dbmd.getDatabaseMinorVersion();
			return dbi;
		}
		catch (SQLException e) {
			throw new DomainObjectSQLException(e);
		}
	}

	private String vendor;
	private int majorVersion;
	private int minorVersion;

	private DatabaseInfo()
	{
	}

	public String getVendor()
	{
		return vendor;
	}

	public boolean isHSQL()
	{
		return vendor.equals("HSQL Database Engine");
	}

	public boolean isMySQL()
	{
		return vendor.equals("MySQL");
	}

	public boolean isOracle()
	{
		return vendor.equals("Oracle");
	}

	public int getMajorVersion()
	{
		return majorVersion;
	}

	public int getMinorVersion()
	{
		return minorVersion;
	}

}

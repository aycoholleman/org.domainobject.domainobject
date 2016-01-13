package org.domainobject.core;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.domainobject.exception.DomainObjectSQLException;

/**
 * A {@code NestedQueryEntity} allows you to map a persistent class to a "live"
 * query. For example, you could have a {@code DepartmentAverages} class that
 * reports on the average age and salary of the employees per department:
 * 
 * <pre>
 * class DepartmentAverages {
 * 	String departmentName;
 * 	double averageAge;
 * 	double averageSalary;
 * }
 * </pre>
 * 
 * You could then either define a database view that prepares this data for you
 * (in which case you would use a {@link ViewEntity}), but you can also map the
 * class directly to a SQL query, e.g.:
 * 
 * <pre>
 * SELECT B.NAME AS DEPARTMENT_NAME, AVG(A.AGE) AS AVERAGE_AGE, AVG(A.SALARY) AS AVERAGE_SALARY
 * FROM EMPLOYEE B, DEPARTMENT A
 * WHERE B.DEPARTMENT_ID = A.ID
 * GROUP BY B.NAME
 * </pre>
 * 
 * All operations on a DepartmentAverages object are then translated into SQL
 * that has the above query as a nested query in the FROM clause, e.g.
 * 
 * <pre>
 * SELECT A.AVERAGE_AGE
 * FROM
 * (
 * 	SELECT B.NAME AS DEPARTMENT_NAME, AVG(A.AGE) AS AVERAGE_AGE, AVG(A.SALARY) AS AVERAGE_SALARY
 * 	FROM EMPLOYEE B, DEPARTMENT A
 * 	WHERE B.DEPARTMENT_ID = A.ID
 * 	GROUP BY B.NAME
 * ) AS A
 * WHERE A.DEPARTMENT_NAME = 'SALES'
 * </pre>
 * 
 * Obviously, only SELECT type operations will be supported. Also, your RDBMS
 * may anyhow not support nested queries in the FROM clause.
 * 
 */
public abstract class NestedQueryEntity implements Entity {

	protected final String name;
	protected final Connection connection;

	private Column[] columns;


	public NestedQueryEntity(String name, Connection conn)
	{
		this.name = name;
		this.connection = conn;
	}


	public String getName()
	{
		return name;
	}


	public Connection getConnection()
	{
		return connection;
	}


	/**
	 * Returns the query string enclosed between parentheses.
	 */
	public String getFrom()
	{
		return new StringBuilder(64).append('(').append(getQueryString()).append(')').toString();
	}


	public Column[] getColumns()
	{
		if (columns == null) {
			try {
				String sql = getQueryStringForOneRow();
				Statement stmt = connection.createStatement();
				ResultSet rs = stmt.executeQuery(sql);
				ResultSetMetaData rsmd = rs.getMetaData();
				columns = new NestedQueryColumn[rsmd.getColumnCount()];
				for (int i = 1; i <= rsmd.getColumnCount(); ++i) {
					columns[i - 1] = new NestedQueryColumn(this, rsmd, i);
				}
			}
			catch (SQLException e) {
				throw new DomainObjectSQLException(e);
			}
		}

		return columns;
	}

	private Column[] primaryKeyColumns;


	public Column[] getPrimaryKeyColumns()
	{
		return primaryKeyColumns;
	}


	public void setPrimaryKeyColumns(Column[] columns)
	{
		this.primaryKeyColumns = columns;
	}


	public Column[] getGeneratedKeyColumns()
	{
		throw new UnsupportedOperationException();
	}


	public void setGeneratedKeyColumns(Column[] columns)
	{
		throw new UnsupportedOperationException();
	}

	private final Map<Entity, Column[]> foreignKeys = new HashMap<Entity, Column[]>(4);


	public Column[] getForeignKeyColumns(Entity parent)
	{
		return foreignKeys.get(parent);
	}


	public void setForeignKeyColumns(Entity parent, Column[] columns)
	{
		foreignKeys.put(parent, columns);
	}


	/**
	 * Get the SQL to appear as a nested query within the FROM clause.
	 * 
	 * @return The SQL query.
	 */
	abstract protected String getQueryString();


	/**
	 * Get the SQL that gives you back one arbitrary row from the query. This
	 * SQL is executed once to retrieve metadata about the columns in the query.
	 * 
	 * @return The SQL query to be executed
	 */
	abstract protected String getQueryStringForOneRow();

}

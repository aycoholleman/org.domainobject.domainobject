package org.domainobject.util;

import org.domainobject.core.Condition;
import org.domainobject.util.ArrayUtil;


/**
 * {@code SQLString} is a collection of commonly used SQL keywords, fragments
 * and phrases. It does not pretend to be exhaustive and it is not part of some
 * SQL parsing facility. The strings enumerated here are generally related to
 * the overall structure of a SQL statement. The {@link Condition} class
 * contains SQL strings that are more targeted at WHERE clauses.
 */
public enum SQLString
{

	//@formatter:off
	SELECT,
	INSERT_INTO("INSERT INTO"),
	DELETE_FROM("DELETE FROM"),
	UPDATE,
	FROM,
	AS,
	SET,
	WHERE,
	AND,
	OR,
	ORDER_BY("ORDER BY");
	//@formatter:on

	private final String sql;


	/**
	 * Returns a delimited version of the SQL string so it can be directly
	 * embedded within a SQL query. The whitespace character is used as
	 * delimiter.
	 * 
	 * @return The SQL keyword/phrase surrounded by whitespace.
	 */
	public String delimited()
	{
		if (ArrayUtil.in(this, SELECT, INSERT_INTO, DELETE_FROM, UPDATE)) {
			return new StringBuilder(sql.length() + 1).append(sql).append(' ').toString();
		}
		return new StringBuilder(sql.length() + 2).append(' ').append(sql).append(' ').toString();
	}


	/**
	 * Return the SQLString keyword
	 */
	public String toString()
	{
		return sql;
	}


	SQLString()
	{
		this.sql = this.name();
	}


	SQLString(String sql)
	{
		this.sql = sql;
	}

}

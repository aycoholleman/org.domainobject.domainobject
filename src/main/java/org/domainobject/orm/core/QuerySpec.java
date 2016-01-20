package org.domainobject.orm.core;

import static org.domainobject.orm.core.Condition.EQUALS;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.domainobject.orm.exception.DomainObjectException;
import org.domainobject.orm.exception.QuerySpecException;
import org.domainobject.orm.exception.UnresolvableQuerySpecException;
import org.domainobject.orm.persister.StandardPersister;
import org.domainobject.orm.util.SQLString;
import org.domainobject.orm.util.Util;

/**
 * <p>
 * A QuerySpec object enables you to fine-tune or extend the SQLString issued by
 * the various methods in {@link StandardPersister}. You could, for example, use
 * a QuerySpec object to add an ORDER BY clause. Contrary to all other classes
 * in domainobject, which go out of their way to let you think in terms of
 * objects and properties rather than in tables and columns, a QuerySpec object
 * also accepts raw SQLString. Thus the QuerySpec class also provides you with a
 * last chance to inject SQLString-centric constraints and expressions like:
 * 
 * <pre>
 * INSTR(UPPER(LAST_NAME),'Smith') > 0
 * </pre>
 * 
 * </p>
 * 
 * <p>
 * Method names starting with "sql" only accept raw SQLString, i.e.
 * <i>column</i> names and SQLString expressions. Method names without the "sql"
 * prefix only accept <i>property</i> names (presumed to belong to a
 * _Persistable object) and thus allow you to stay within the object-oriented
 * realm. The SQLString-centric methods give you more flexibility (anything
 * SQLString goes), but make you that bit more reliant on the underlying
 * database.
 * </p>
 * 
 * <p>
 * Finally, note that the {@link QueryGenerator SQLString adapter}, which is the
 * ultimate consumer of QuerySpec objects, may ignore certain properties of the
 * QuerySpec object. For example, it will ignore the ORDER BY clause in a
 * QuerySpec object when generating an UPDATE or DELETE statement.
 * </p>
 * 
 * @see Condition
 * 
 */
public class QuerySpec {

	/**
	 * Recommended table alias. If possible, {@link Query} generators should use
	 * this alias for the table corresponding to the type of objects being
	 * updated or returned by a {@link Query}.
	 */
	public static final String TARGET_TABLE_ALIAS = "T1";

	/**
	 * Recommended table alias. If possible and applicable, {@link Query}
	 * generators should use this alias for the table from which you navigate to
	 * the target table (the table being updated or selected from). For example:
	 * 
	 * <pre>
	 * StandardPersister persister = new StandardPersister(...);
	 * ...
	 * List<Employee> employees = persister.loadChildren(department, queryspec);
	 * </pre>
	 * 
	 * Here you navigate from departments to employees. Thus the EMPLOYEE table
	 * in the underlying SQL should get the TARGET_TABLE_ALIAS and the
	 * DEPARTMENT table should get the SOURCE_TABLE_ALIAS.
	 */
	public static final String SOURCE_TABLE_ALIAS = "T2";

	/**
	 * Recommended table alias.
	 */
	public static final String TABLE_ALIAS_3 = "T3";

	/**
	 * Recommended table alias.
	 */
	public static final String TABLE_ALIAS_4 = "T4";

	/**
	 * Recommended table alias.
	 */
	public static final String TABLE_ALIAS_5 = "T5";

	/**
	 * An immutable, empty QuerySpec object. Attempting to change its state will
	 * cause a {@link DomainObjectException} to be thrown.
	 */
	public static final QuerySpec EMPTY = new QuerySpec();

	private static final String ERR_TOO_MANY_CLAUSES = "You must either specify a SELECT clause or an INSERT clause or an UPDATE clause";

	// SELECT
	@SuppressWarnings("rawtypes")
	private List select;

	// INSERT
	private List<Object> insert;

	// UPDATE
	@SuppressWarnings("rawtypes")
	private List update;

	// WHERE
	@SuppressWarnings("rawtypes")
	private List where;

	// ORDER BY
	@SuppressWarnings("rawtypes")
	private List orderBy;

	// PARAMETER BINDINGS
	private Map<String, Object> bindings;

	private int offset;
	private int maxRecords;

	// BEGIN NESTED CLASSES
	// @formatter:off
	
	private static class OOSelect {
		private final String property;
		private OOSelect(final String property) { this.property = property;}
		public boolean equals(Object obj)
		{
			if (this == obj) return true;
			if (obj == null) return false;
			return property.equals(((OOSelect) obj).property);
		}
		public int hashCode() { return property.hashCode(); }
	}

	private static class OOSort {
		private final String property;
		private final boolean isAscending;
		private OOSort(final String property, final boolean isAscending)
		{
			this.property = property;
			this.isAscending = isAscending;
		}
		public boolean equals(Object obj)
		{
			if (this == obj) return true;
			if (obj == null) return false;
			OOSort other = (OOSort) obj;
			return property.equals(other.property) && isAscending == other.isAscending;
		}
		public int hashCode()
		{
			return new StringBuilder().append(property).append('&').append(isAscending ? '1' : '0').toString().hashCode();
		}
	}

	// @formatter:on
	// END NESTED CLASSES

	/**
	 * Add one or more more fields to the SELECT clause. The should belong to
	 * the persistent class corresponding to the table you are SELECTing data
	 * from.
	 * 
	 * @param fields
	 *            One or more property names.
	 * 
	 * @return this QuerySpec instance.
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public QuerySpec select(String... fields)
	{
		if (fields.length != 0) {
			checkThisIsNotEmpty();
			if (select == null) {
				select = new ArrayList(fields.length);
			}
			for (String field : fields) {
				select.add(new OOSelect(field));
			}
		}
		return this;
	}


	/**
	 * Add a raw SQL expression to the SELECT clause. Don't include the SELECT
	 * keyword itself. Although you can pass plain column names to this method,
	 * this method is especially meant to add more complex SQL expressions to
	 * the SELECT clause (e.g. expressions involving SQL functions).
	 * 
	 * @param sql
	 *            One or more SQLString expressions to be added to the SELECT
	 *            clause.
	 * 
	 * @return This QuerySpec instance.
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public QuerySpec sqlSelect(String... sql)
	{
		checkThisIsNotEmpty();
		if (sql == null || sql.length == 0) {
			throw new QuerySpecException("At least one raw SQL expression must be provided");
		}
		if (select == null) {
			select = new ArrayList();
		}
		select.addAll(Arrays.asList(sql));
		return this;
	}


	public QuerySpec insert(String field, Object value)
	{
		checkThisIsNotEmpty();
		if (select != null || update != null) {
			throw new QuerySpecException(ERR_TOO_MANY_CLAUSES);
		}
		if (insert == null) {
			insert = new ArrayList<Object>();
		}
		insert.add(new Condition(field, EQUALS, value));
		return this;
	}


	public QuerySpec insert(String field)
	{
		checkThisIsNotEmpty();
		if (select != null || update != null) {
			throw new QuerySpecException(ERR_TOO_MANY_CLAUSES);
		}
		if (insert == null) {
			insert = new ArrayList<Object>();
		}
		insert.add(new Condition(field));
		return this;
	}


	public QuerySpec sqlInsert(String column, String sqlExpression)
	{
		checkThisIsNotEmpty();
		if (select != null || update != null) {
			throw new QuerySpecException(ERR_TOO_MANY_CLAUSES);
		}
		if (insert == null) {
			insert = new ArrayList<Object>();
		}
		insert.add(new String[] { column, sqlExpression });
		return this;
	}


	@SuppressWarnings({ "rawtypes", "unchecked" })
	public QuerySpec sqlUpdate(String... sql)
	{
		checkThisIsNotEmpty();
		if (sql == null || sql.length == 0) {
			throw new QuerySpecException("At least one raw SQL expression must be provided");
		}
		if (update == null) {
			update = new ArrayList();
		}
		update.addAll(Arrays.asList(sql));
		return this;
	}


	@SuppressWarnings({ "unchecked", "rawtypes" })
	public QuerySpec update(String... fields)
	{
		checkThisIsNotEmpty();
		if (update == null) {
			update = new ArrayList();
		}
		for (String field : fields) {
			update.add(new Condition(field));
		}
		return this;
	}


	@SuppressWarnings({ "unchecked", "rawtypes" })
	public QuerySpec update(String field, Object value)
	{
		checkThisIsNotEmpty();
		if (update == null) {
			update = new ArrayList();
		}
		update.add(new Condition(field, EQUALS, value));
		return this;
	}


	public QuerySpec update(String[] fields, Object[] values)
	{
		if (fields.length != values.length) {
			throw new QuerySpecException("Number of fields must match number of values");
		}
		for (int i = 0; i < fields.length; ++i) {
			update(fields[i], values[i]);
		}
		return this;
	}


	/**
	 * Extract a SELECT clause from this QuerySpec.
	 * 
	 * @param metadata
	 *            The metadata object through which to translate property names
	 *            into column names. You may pass null if you somehow know that
	 *            the SELECT clause was constructed solely from calls to
	 *            {@link #sqlSelect(String...)} and never to
	 *            {@link #select(String...)}.
	 * @param tableAlias
	 *            A table prefix for columns resulting from calls to
	 *            {@link #select(String...)}. So you would get something like
	 *            <code>table_alias.column_name</code>. You may pass null if you
	 *            don't need fully qualified column names.
	 * 
	 * @return The SELECT clause.
	 * 
	 * @throws SQLException
	 */
	public String getSelectClause(MetaData<?> metadata, String tableAlias) throws SQLException
	{

		StringBuilder sb = new StringBuilder(SQLString.SELECT.delimited());

		if (select == null) {
			if (tableAlias != null) {
				sb.append(tableAlias).append('.');
			}
			return sb.append('*').toString();
		}

		int i = 0;

		for (Object element : select) {

			if (i++ > 0) {
				sb.append(',');
			}

			if (element.getClass() == OOSelect.class) {
				if (metadata == null) {
					throw new UnresolvableQuerySpecException();
				}
				OOSelect s = (OOSelect) element;
				if (tableAlias != null) {
					sb.append(tableAlias).append('.');
				}
				sb.append(Util.getQuotedColumnForField(metadata, s.property));
			}
			else {
				sb.append((String) element);
			}
		}

		return sb.toString();

	}


	public boolean isSelectStar()
	{
		return select == null;
	}


	public String getUpdateClause(MetaData<?> metadata, String tableAlias)
	{
		if (update == null) {
			return null;
		}
		StringBuilder sb = new StringBuilder(64);
		for (Object obj : update) {
			String expression;
			if (obj instanceof Condition) {
				Condition condition = (Condition) obj;
				expression = condition.resolveSelf(metadata, tableAlias);
			}
			else {
				expression = (String) obj;
			}
			if (sb.length() != 0) {
				sb.append(',');
			}
			sb.append(expression);
		}
		return sb.toString();
	}


	/**
	 * Add a raw SQLString condition to the WHERE clause. Never start with the
	 * WHERE keyword itself.
	 * 
	 * @param sql
	 *            The WHERE clause or an extra constraint within the WHERE
	 *            clause.
	 * @return This QuerySpec instance
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public QuerySpec sqlWhere(String sql)
	{
		checkThisIsNotEmpty();
		if (where == null) {
			where = new ArrayList(4);
		}
		where.add(sql);
		return this;
	}


	/**
	 * Create one or more {@link Condition}s to be added to the WHERE clause.
	 * The <code>Condition</code>s are created from the properties that you pass
	 * to this method. See {@link Condition#Condition(String)}.
	 * 
	 * @param fields
	 *            One or more property names.
	 * 
	 * @return this QuerySpec instance.
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public QuerySpec where(String... fields)
	{
		if (fields.length != 0) {
			checkThisIsNotEmpty();
			if (where == null) {
				where = new ArrayList();
			}
			for (String field : fields) {
				where.add(new Condition(field));
			}
		}
		return this;
	}


	@SuppressWarnings({ "rawtypes", "unchecked" })
	public QuerySpec where(String field, Object value)
	{
		checkThisIsNotEmpty();
		if (where == null) {
			where = new ArrayList();
		}
		where.add(new Condition(field, EQUALS, value));
		return this;
	}


	public QuerySpec where(String[] fields, Object[] values)
	{
		if (fields.length != values.length) {
			throw new QuerySpecException("Number of fields must match number of values");
		}
		for (int i = 0; i < fields.length; ++i) {
			where(fields[i], values[i]);
		}
		return this;
	}


	/**
	 * Add one or more {@link Condition}s to the WHERE clause.
	 * 
	 * @param conditions
	 *            One or more <code>Condition</code>s.
	 * 
	 * @return this QuerySpec instance.
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public QuerySpec where(Condition... conditions)
	{

		checkThisIsNotEmpty();

		if (conditions.length != 0) {

			if (where == null) {
				where = new ArrayList(conditions.length);
			}

			for (Condition condition : conditions) {
				where.add(condition);
			}

		}

		return this;
	}


	/**
	 * Extract WHERE clause from this QuerySpec. The WHERE clause will start
	 * with the WHERE keyword (rather than with AND or OR).
	 * 
	 * @param persistable
	 *            The _Persistable object for which to generate the WHERE
	 *            clause. You may pass null if you somehow know that the WHERE
	 *            clause was constructed solely from calls to
	 *            {@link #sqlWhere(String)} and never to
	 *            {@link #where(String...)} or {@link #where(Condition...)}.
	 * @param tableAlias
	 *            A table prefix to use in conditions resulting from
	 *            {@link #where(String...)} or {@link #where(Condition...)}. You
	 *            may pass null if you don't need fully qualified column names.
	 * 
	 * @return The WHERE clause.
	 * 
	 * @throws SQLException
	 * @throws IllegalAccessException
	 */
	public String getWhereClause(MetaData<?> metadata, String tableAlias)
	{
		return getWhereClause(metadata, tableAlias, SQLString.WHERE);
	}


	/**
	 * Extract WHERE clause from this QuerySpec.
	 * 
	 * @param persistable
	 *            The _Persistable object for which to generate the WHERE
	 *            clause. You may pass null if you somehow know that the WHERE
	 *            clause was constructed solely from calls to
	 *            {@link #sqlWhere(String)} and never to
	 *            {@link #where(String...)} or {@link #where(Condition...)}.
	 * @param tableAlias
	 *            A table prefix to use in conditions resulting from
	 *            {@link #where(String...)} or {@link #where(Condition...)}. You
	 *            may pass null if you don't need fully qualified column names.
	 * @param keyword
	 *            The SQLString keyword to start the WHERE clause with. Must be
	 *            one of {@link SQLString#WHERE}, {@link SQLString#AND} or
	 *            {@link SQLString#OR}.
	 * 
	 * @return The WHERE clause
	 * 
	 * @throws SQLException
	 * @throws IllegalAccessException
	 */
	public String getWhereClause(MetaData<?> metadata, String tableAlias, SQLString keyword)
	{
		if (where == null) {
			return "";
		}

		StringBuilder sb = new StringBuilder(64);

		int i = 0;

		for (Object obj : where) {
			if (i++ > 0) {
				sb.append(SQLString.AND);
			}
			else {
				sb.append(keyword);
			}
			if (obj instanceof Condition) {
				Condition condition = (Condition) obj;
				sb.append(condition.resolve(metadata, tableAlias));
			}
			else {
				sb.append('(').append((String) obj).append(')');
			}
		}

		return sb.toString();

	}


	List<Condition> getConditionsInUpdateClause()
	{
		if (update == null) {
			return null;
		}
		List<Condition> conditions = new ArrayList<Condition>(update.size());
		for (Object obj : update) {
			if (obj instanceof Condition) {
				conditions.add((Condition) obj);
			}
		}
		return conditions;
	}


	List<Condition> getConditionsInWhereClause()
	{
		if (where == null) {
			return null;
		}
		List<Condition> conditions = new ArrayList<Condition>(where.size());
		for (Object obj : where) {
			if (obj instanceof Condition) {
				conditions.add((Condition) obj);
			}
		}
		return conditions;
	}


	/**
	 * Add a raw SQLString expression to the ORDER BY clause. Never start with
	 * the ORDER BY keyword itself.
	 * 
	 * @param sql
	 *            A raw SQLString expression that is valid in the ORDER BY
	 *            clause.
	 * 
	 * @return This QuerySpec instance.
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public QuerySpec sqlOrderBy(String sql)
	{

		checkThisIsNotEmpty();

		if (orderBy == null) {
			orderBy = new ArrayList(4);
		}

		orderBy.add(sql);

		return this;
	}


	public QuerySpec orderBy(String field)
	{
		return orderBy(field, true);
	}


	@SuppressWarnings({ "rawtypes", "unchecked" })
	public QuerySpec orderBy(String field, boolean isAscending)
	{
		if (this == EMPTY) {
			throw new QuerySpecException("Illegal attempt to modify the EMPTY QuerySpec");
		}

		if (orderBy == null) {
			orderBy = new ArrayList(4);
		}

		orderBy.add(new OOSort(field, isAscending));

		return this;
	}


	/**
	 * Extract an ORDER BY clause from this QuerySpec.
	 * 
	 * @param metadata
	 *            The metadata object through which to translate property names
	 *            into column names. You may pass null if you somehow know that
	 *            the ORDER clause was constructed solely from calls to
	 *            {@link #sqlOrderBy(String)} and never to
	 *            {@link #orderBy(String)} or {@link #orderBy(String, boolean)}.
	 * @param tableAlias
	 *            A table prefix for columns resulting from calls to
	 *            {@link #select(String...)}. So you would get something like
	 *            <code>table_alias.column_name</code>. You may pass null if you
	 *            don't need fully qualified column names.
	 * 
	 * @return The ORDER BY clause.
	 * 
	 * @throws SQLException
	 */
	public String getOrderByClause(MetaData<?> metadata, String tableAlias) throws SQLException
	{
		if (orderBy == null) {
			return "";
		}

		StringBuilder sb = new StringBuilder(SQLString.ORDER_BY.delimited());

		boolean notFirst = false;

		for (Object element : orderBy) {
			if (notFirst) {
				sb.append(',');
			}
			else {
				notFirst = true;
			}
			if (element.getClass() == OOSort.class) {
				if (metadata == null) {
					throw new UnresolvableQuerySpecException();
				}
				OOSort s = (OOSort) element;
				if (tableAlias != null) {
					sb.append(tableAlias).append('.');
				}
				sb.append(Util.getQuotedColumnForField(metadata, s.property));
				if (!s.isAscending) {
					sb.append(" DESC");
				}
			}
			else {
				sb.append((String) element);
			}
		}

		return sb.toString();

	}


	/**
	 * Set the maximum number of records to retrieve from the database.
	 * 
	 * @param i
	 *            The maximum number of records to retrieve from the database.
	 * @return This QuerySpec instance
	 */
	public QuerySpec setMaxRecords(int i)
	{
		checkThisIsNotEmpty();
		maxRecords = i;
		return this;
	}


	/**
	 * Get the maximum number of records to retrieve from the database.
	 * 
	 * @return The maximum number of records to retrieve from the database.
	 */
	public int getMaxRecords()
	{
		return maxRecords;
	}


	/**
	 * Set with which record in the {@link java.sql.Resultset Resultset} to
	 * start.
	 * 
	 * @param i
	 *            The offset.
	 * @return This QuerySpec instance
	 */
	public QuerySpec setOffset(int i)
	{
		checkThisIsNotEmpty();
		offset = i;
		return this;
	}


	/**
	 * Where to start in the {@link java.sql.Resultset Resultset}.
	 * 
	 * @return The offset
	 */
	public int getOffset()
	{
		return offset;
	}


	/**
	 * Bind the specified value to the specified parameter.
	 * 
	 * @param parameter
	 *            The parameter
	 * @param value
	 *            The values to be bound to the parameter
	 * @return This QuerySpec instance
	 */
	public QuerySpec bind(String parameter, Object value)
	{
		if (this == EMPTY) {
			throw new QuerySpecException("Illegal attempt to modify the EMPTY QuerySpec");
		}
		if (bindings == null) {
			bindings = new HashMap<String, Object>();
		}
		if (bindings.containsKey(parameter)) {
			throw new QuerySpecException("Parameter " + parameter + " has already been bound");
		}
		bindings.put(parameter, value);
		return this;
	}


	/**
	 * Bind the specified values to the specified parameters.
	 * 
	 * @see #bind(String, Object)
	 * 
	 * @param parameters
	 *            The Parameters
	 * @param values
	 *            The values
	 * @return This QuerySpec instance
	 */
	public QuerySpec bind(String[] parameters, Object[] values)
	{
		if (parameters.length != values.length) {
			throw new QuerySpecException("Number of parameters must match number of values");
		}
		for (int i = 0; i < parameters.length; ++i) {
			bind(parameters[i], values[i]);
		}
		return this;
	}


	public Map<String, Object> getBindings()
	{
		return bindings;
	}


	/**
	 * Two QuerySpec objects are considered equal if all fields <i>except the
	 * binding array</i> are equal. This is because the contents of the bindings
	 * array does not matter when establishing the identity of the
	 * {@link java.sql.PreparedStatement} that results from a QuerySpec object.
	 */
	@Override
	public boolean equals(Object obj)
	{
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		QuerySpec other = (QuerySpec) obj;
		if (!equals(this.select, other.select)) {
			return false;
		}
		if (!equals(this.update, other.update)) {
			return false;
		}
		if (!equals(this.where, other.where)) {
			return false;
		}
		if (!equals(this.orderBy, other.orderBy)) {
			return false;
		}
		return (this.maxRecords == other.maxRecords && this.offset == other.offset);
	}


	@Override
	public int hashCode()
	{
		int hash = 1;
		hash = (hash * 31) + (select == null ? 0 : select.hashCode());
		hash = (hash * 31) + (update == null ? 0 : update.hashCode());
		hash = (hash * 31) + (where == null ? 0 : where.hashCode());
		hash = (hash * 31) + (orderBy == null ? 0 : orderBy.hashCode());
		hash = (hash * 31) + maxRecords;
		hash = (hash * 31) + offset;
		return hash;
	}


	private static boolean equals(Object obj1, Object obj2)
	{
		if (obj1 == null) {
			return obj2 == null ? true : false;
		}
		return obj2 == null ? false : obj1.equals(obj2);
	}


	private void checkThisIsNotEmpty()
	{
		if (this == EMPTY) {
			throw new DomainObjectException("Illegal attempt to modify the EMPTY QuerySpec");
		}
	}

}

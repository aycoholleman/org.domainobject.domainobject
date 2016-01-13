package org.domainobject.core;

// TODO: Take care of multithreading issues in several methods of this class

import static org.domainobject.core.Condition.BETWEEN;
import static org.domainobject.core.Condition.IN;
import static org.domainobject.core.Condition.IS_NOT_NULL;
import static org.domainobject.core.Condition.IS_NULL;
import static org.domainobject.core.Condition.NOT_BETWEEN;
import static org.domainobject.core.Condition.NOT_IN;
import static org.domainobject.core.Condition.NOT_SET;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.domainobject.exception.AlienMetaDataException;
import org.domainobject.exception.DomainObjectException;
import org.domainobject.exception.DomainObjectSQLException;
import org.domainobject.exception.QuerySpecException;
import org.domainobject.exception.UnmappedFieldException;
import org.domainobject.exception.UnresolvableQueryException;
import org.domainobject.util.Util;

/**
 * A {@code Query} represents a parametrized SQL query. There are four types of
 * methods in the {@code Query} class:
 * <ol>
 * <li>Life cycle methods, starting with
 * {@link #createQuery(Object, String, Object...) createQuery} and ending with
 * {@link #close() close} or {@link #destroy() destroy}</li>
 * <li>Bind methods, which bind values to the parameters within the SQL
 * represented by the {@code Query}</li>
 * <li>Execute methods, which execute the SQL</li>
 * <li>Miscellaneous methods, like {@link #getGeneratedSQL() getSQL}</li>
 * </ol>
 * Most execute methods also end the life cycle of the {@code Query} by calling
 * {@link #close()}. A few methods, like {@link #insert(Object) insert} even go
 * through an entire bind-execute-close cycle.
 */
public final class Query<T> {

	private static final int INIT_QUERY_POOL_SIZE = 4;
	private static final String MSG_QUERY_CLOSED = "Operation not allowed on closed or non-initialized Query";

	private static final class CacheKey {
		private final Object generator;
		private final String queryId;
		private final Object[] queryVars;


		private CacheKey(Object generator, String queryId, Object[] queryVars)
		{
			this.generator = generator;
			this.queryId = queryId;
			this.queryVars = queryVars;
		}


		public boolean equals(Object obj)
		{
			if (this == obj) {
				return true;
			}
			CacheKey other = (CacheKey) obj;
			return queryId.equals(other.queryId) && Arrays.deepEquals(queryVars, other.queryVars) && generator.equals(other.generator);
		}


		public int hashCode()
		{
			int hash = 1;
			hash = hash * 31 + queryId.hashCode();
			hash = hash * 31 + Arrays.deepHashCode(queryVars);
			hash = hash * 31 + generator.hashCode();
			return hash;
		}

	}

	private static final class QueryPool extends ArrayList<Query<?>> {
		private static final long serialVersionUID = 1L;


		public QueryPool(int capacity)
		{
			super(capacity);
		}

		// for now, that's it folks ...

	}

	private static final Map<CacheKey, QueryPool> cache = new HashMap<CacheKey, QueryPool>();


	static void clearCache()
	{
		for (Map.Entry<CacheKey, QueryPool> entry : cache.entrySet()) {
			for (Query<?> query : entry.getValue()) {
				try {
					query.statement.close();
				}
				catch (SQLException e) {
					throw new DomainObjectSQLException(e);
				}
			}
		}
		cache.clear();
	}


	static int clearCache(Context context)
	{
		for (CacheKey key : cache.keySet()) {
			QueryPool pool = cache.get(key);
			Iterator<Query<?>> iterator = pool.iterator();
			// Get the first query from the query pool. If its metadata object
			// was produced by the specified factory, we can wipe out the
			// entire pool, because all query objects in the same pool share
			// the same metadata objects.
			Query<?> query = iterator.next();
			if (query.metadata.getContext() == context) {
				while (iterator.hasNext()) {
					try {
						query.statement.close();
					}
					catch (SQLException e) {
						// log something
					}
					query = iterator.next();
				}
				cache.remove(key);
			}
		}
		return cache.size();
	}


	/**
	 * <p>
	 * Create a {@code Query} for the specified method within the specified
	 * {@code Query} generator. The idea is that, taken together, the generator,
	 * the method and the arguments to that method uniquely identify the SQL
	 * produced by that method. Thus, taken together, they can be used to
	 * construct a key for an internally maintained {@code Query} cache, which
	 * is exactly what happens when you call
	 * {@link #createQuery(Object, String, Object...) createQuery}. Because
	 * generating the SQL for a {@code Query} and instantiating the associated
	 * {@code PreparedStatement} might be expensive, {@code Query} generator
	 * methods would typically look like this:
	 * 
	 * <pre>
	 * public class MyQueryGenerator {
	 * <br>
	 * 	public Query createComplexQuery(Class0 arg0, Class1 arg1, Class2 arg2)
	 * 	{
	 * 		Query query = Query.createQuery(this, "createComplexQuery", arg0, arg1, arg2);
	 * 		if (query.isNew()) {
	 * 			StringBuilder sql = new StringBuilder();
	 * 			// generate SQL using arg0
	 * 			sql.append("SELECT ...");
	 * 			sql.append(" FROM ");
	 * 			query.initialize(sql.toString());
	 * 		}
	 * 		return query;
	 * 	}
	 * }
	 * </pre>
	 * 
	 * </p>
	 * 
	 * <p>
	 * Note that, ultimately, the only point is to pass a set of arguments to
	 * {@code createQuery} that will uniquely identify the SQL wrapped by it.
	 * This has the following consequences:
	 * <ol>
	 * <li>Instead of "createComplexQuery" you might as well have passed "foo"
	 * to {@code createQuery} as long as you don't pass "foo" for any other
	 * method in the same generator. The {@code Query} generator classes
	 * provided by domainobject, though, all stick to the convention of passing
	 * the method name as second argument to {@code createQuery}.</li>
	 * <li>If you have a method that generates a {@code Query} for a constant,
	 * non-parametrized SQL String like "
	 * {@code SELECT * FROM EMPLOYEE WHERE ID = 10}", then you might as well
	 * pass {@code null} for the query generator argument (
	 * {@code Query.createQuery(null, "myMethod")}), because it doesn't really
	 * matter whether this SQL is produced in generator X or generator Y. In
	 * fact, in this case it doesn't even matter whether the SQL is produced in
	 * method A or method B, because no method argument can influence it.
	 * Therefore you could have requested the {@code Query} object using
	 * {@code Query.createQuery(null, "SELECT * FROM EMPLOYEE WHERE ID = 10")}),
	 * if you want to be sure this query gets cached just once.</li>
	 * <li>
	 * If your {@code Query} generator class maintains state and that state
	 * influences the SQL being generated, then that state must also be passed
	 * to {@code createQuery} as part of the {@code args} argument to
	 * {@code createQuery}.</li>
	 * <li>
	 * If an argument to a {@code Query} generating method has no impact on the
	 * SQL being generated within that method, then that argument should
	 * <i>not</i> be passed on to {@code createQuery}. Doing so is not strictly
	 * speaking an error, but it would cause the {@code Query} cache to get
	 * populated with redundant queries (all wrapping the same SQL string).</li>
	 * <li>
	 * Parameter bindings (values that you plan to bind into the {@code Query})
	 * should always be excluded from the {@code args} argument. This is a
	 * special, but noteworthy case of the previous point. With a query like "
	 * {@code SELECT * FROM EMPLOYEE WHERE ID = ?}", it doesn't matter whether
	 * you pass 9 or 10 as a binding for the ID column. Note though that you can
	 * safely pass {@link QuerySpec}s and {@link Condition}s to {@code Query}
	 * generating methods, even though these contain binding information. This
	 * is because the {@code equals} and {@code hashCode} methods of these
	 * classes have been overriden to ignore their binding information.</li>
	 * </ol>
	 * 
	 * @param generator
	 *            The object that generates the SQL
	 * @param method
	 *            The method in which the SQL is generated
	 * @param args
	 *            All arguments to the method that influence the SQL being
	 *            generated, as well as all state, maintained by the generator,
	 *            and other values that also have an impact on the SQL being
	 *            generated.
	 * @return
	 */
	public static <U> Query<U> createQuery(Object generator, String method, Object... args)
	{
		CacheKey key = new CacheKey(generator, method, args);
		QueryPool pool = cache.get(key);
		if (pool != null) {
			for (Query<?> query : pool) {
				if (query.closed) {
					query.paramIndex = 0;
					query.closed = false;
					@SuppressWarnings("unchecked")
					Query<U> result = (Query<U>) query;
					return result;
				}
			}
			// N.B. createCopy() adds the copy to the pool, so we
			// don't have to do that here.
			@SuppressWarnings("unchecked")
			Query<U> result = (Query<U>) pool.iterator().next().createCopy();
			return result;
		}
		return new Query<U>(key);
	}

	private final CacheKey cacheKey;

	private String generatedSQL;
	private String submittedSQL;
	private Map<String, List<Integer>> queryParams;
	private PreparedStatement statement;
	private int paramIndex = 0;
	private MetaData<T> metadata;
	private QuerySpec querySpec;
	private boolean initialized = false;
	private boolean closed = true;


	private Query(CacheKey key)
	{
		this.cacheKey = key;
	}


	/**
	 * Whether or not this {@code Query} has already been initialized. This
	 * method is typically only called by {@code Query} generators to check
	 * whether the SQL for the {@code Query} has already been generated. If so,
	 * they can immediately return the {@code Query} to the caller. Otherwise,
	 * they should generate the SQL, call {@link #initialize(String, MetaData)
	 * initialize} and only then return the {@code Query} to the caller.
	 * 
	 * @return Whether or not this {@code Query} has already been initialized.
	 */
	public boolean isNew()
	{
		return !initialized;
	}


	/**
	 * Whether or not the {@code Query} has been closed using the
	 * {@link #close() close} or {@link #destroy() destroy} method.
	 * 
	 * @return
	 */
	public boolean isOpen()
	{
		return !closed;
	}


	/**
	 * Close this {@code Query}. Closing a {@code Query} does in fact not
	 * relinquish any resources held by domainobject. It just makes the
	 * {@code Query} available for re-use in a new bind-execute cycle. Most
	 * execute methods, like {@link #load(Object)} automatically call
	 * {@code close} when done. However, you may want close the {@code Query}
	 * explicitly yourself, e.g. when you appear to be stuck somewhere midway
	 * between a call to a bind method and an execute method. After a
	 * {@code Query} has been closed you cannot call any more methods on it
	 * until it gets recycled through the
	 * {@link #createQuery(Object, String, Object...) createQuery} method. (You
	 * can actually still call {@link #isOpen() isOpen}, {@code close} and
	 * {@link #destroy() destroy}. But calling {@code close} or {@code destroy}
	 * more than once has no effect. Calling any other method results in an
	 * {@code IllegalStateException.)
	 */
	public void close()
	{
		closed = true;
	}


	/**
	 * Closes this {@code Query} and the {@code PreparedStatement} that it wraps
	 * and removes the {@code Query} from the internally maintained
	 * {@code Query} cache.
	 */
	public void destroy()
	{
		closed = true;
		QueryPool pool = cache.get(cacheKey);
		pool.remove(this);
		if (pool.size() == 0) {
			cache.remove(this.cacheKey);
		}
		// statement might be null in unlikely case that it is destroyed
		// before it is initialized.
		if (statement != null) {
			try {
				statement.close();
			}
			catch (SQLException e) {
				throw new DomainObjectSQLException(e);
			}
		}
	}


	/**
	 * Initialize this {@code Query} with the specified SQL String and metadata
	 * object.
	 * 
	 * @param sql
	 *            The SQL
	 * @param metadata
	 *            The metadata object
	 */
	public void initialize(String sql, MetaData<T> metadata, QuerySpec qs)
	{
		if (initialized) {
			throw new IllegalStateException("Query already initialized");
		}
		initialized = true;
		ParameterExtractor parameterExtractor = new ParameterExtractor(sql);
		this.submittedSQL = parameterExtractor.getProcessedSQL();
		try {
			Connection conn = metadata.getEntity().getConnection();
			Column[] keys = null;
			try {
				keys = metadata.getEntity().getGeneratedKeyColumns();
			}
			catch (UnsupportedOperationException e) {
				// ...
			}
			if (keys == null || keys.length == 0) {
				this.statement = conn.prepareStatement(submittedSQL, Statement.RETURN_GENERATED_KEYS);
			}
			else {
				String[] colNames = new String[keys.length];
				for (int i = 0; i < keys.length; ++i) {
					colNames[i] = keys[i].getName();
				}
				this.statement = conn.prepareStatement(submittedSQL, colNames);
			}
		}
		catch (SQLException e) {
			throw new DomainObjectSQLException(e);
		}
		this.generatedSQL = sql;
		this.metadata = metadata;
		this.querySpec = qs;
		this.queryParams = parameterExtractor.getParameters();
		this.closed = false;
		QueryPool pool = cache.get(cacheKey);
		if (pool == null) {
			pool = new QueryPool(INIT_QUERY_POOL_SIZE);
			cache.put(cacheKey, pool);
		}
		pool.add(this);
	}


	/**
	 * Bind the values of the specified fields into the query.
	 * 
	 * @param object
	 *            An instance of the class containing the specified fields. The
	 *            values of the fields are retrieved from this instance.
	 * @param fields
	 *            The fields wholse values to bind into the query.
	 * 
	 * @return This {@code Query} instance
	 */
	public Query<T> bind(T object, Field[] fields)
	{
		if (closed) {
			throw new IllegalStateException(MSG_QUERY_CLOSED);
		}
		if (metadata == null) {
			throw new DomainObjectException("Think of a better exception to throw");
		}
		if (object.getClass() != metadata.getForClass()) {
			throw new AlienMetaDataException(metadata, object.getClass());
		}
		for (Field field : fields) {
			DataExchangeUnit deu = metadata.getDataExchangeUnit(field);
			deu.send(object, statement, ++paramIndex);
		}
		return this;
	}


	/**
	 * Bind the values of the specified conditions into the query.
	 * 
	 * @param conditions
	 *            The {@code Condition}s whose values to bind into the query.
	 * @return This {@code Query} instance
	 */
	public Query<T> bind(Condition[] conditions)
	{
		if (closed) {
			throw new IllegalStateException(MSG_QUERY_CLOSED);
		}
		return bind(null, conditions);
	}


	////////////////////////////////////////////////////////////////////////////
	//  NB: The logic of this method (regarding when and what to bind) must   //
	//  exactly mirror Condition#translateSelf() !!! The only difference is   //
	//  that this method does not trap any errors, because that has already   //
	//  been done in the constructors and the translate() method of           //
	//  Condition.                                                            //
	////////////////////////////////////////////////////////////////////////////
	/**
	 * Bind the values of the specified conditions into the query. If the value
	 * of a {@code Condition} appears to be {@link Condition#NOT_SET}, then it
	 * will be retrieved from the object that you passed as the first argument
	 * to this method. For example, with a {@code Condition} created like
	 * {@code new Condition("salary", LESS_THAN)}, the value to be bound for
	 * salary is retrieved from the salary field in the object you passed as the
	 * first argument to this method. Thus, the object you pass must be an
	 * Employee, or any other class containing a salary field.
	 * 
	 * @param object
	 *            The object that provides the value for a {@code Condition} if
	 *            that {@code Condition} does not provide one itself.
	 * @param conditions
	 *            The {@code Condition}s whose values to bind into the query.
	 * 
	 * @return This {@code Query} instance
	 */
	public Query<T> bind(T object, Condition[] conditions)
	{
		if (closed) {
			throw new IllegalStateException(MSG_QUERY_CLOSED);
		}
		if (metadata == null) {
			throw new DomainObjectException("Think of a better exception to throw");
		}
		for (Condition condition : conditions) {

			String operator = condition.getOperator();

			if (operator == IS_NULL || operator == IS_NOT_NULL) {
				// Then the Condition got translated into "xyz IS NULL" or
				// "xyz IS NOT NULL" - something without parameters, so there
				// is nothing to bind
				continue;
			}

			String param = condition.getParameterName();
			Object value = condition.getValue();

			if (operator == IN || operator == NOT_IN) {
				if (value instanceof Object[]) {
					Object[] array = (Object[]) value;
					bindParameter(param, array[0]);
					for (int i = 1; i < array.length; ++i) {
						String nextParam = param + String.valueOf(i + 1);
						bindParameter(nextParam, array[i]);
					}
				}
				else {
					bindParameter(param, value);
				}
			}

			else if (operator == BETWEEN || operator == NOT_BETWEEN) {
				Object[] array = (Object[]) value;
				bindParameter(param, array[0]);
				bindParameter(param + "_2", array[1]);
			}

			else if (condition.getValue() == null) {
				// Got translated into "xyz IS NULL" or "xyz IS NOT NULL"
			}

			else if (condition.getValue() == NOT_SET) {
				if (object == null) {
					throw new UnresolvableQueryException();
				}
				DataExchangeUnit deu = metadata.getDataExchangeUnit(condition.getField());
				if (deu == null) {
					throw new UnmappedFieldException(metadata.getForClass(), condition.getField());
				}
				deu.send(object, statement, ++paramIndex);
			}

			else {
				bindParameter(param, value);
			}

			if (condition.getSiblings() != null) {
				List<Condition> list = condition.getSiblings();
				Condition[] siblings = list.toArray(new Condition[list.size()]);
				bind(object, siblings);
			}

		}

		return this;

	}


	public void bind(QuerySpec qs)
	{
		bind(qs, null);
	}


	public void bind(QuerySpec qs, T object)
	{
		if (closed) {
			throw new IllegalStateException(MSG_QUERY_CLOSED);
		}
		if (qs.getBindings() != null) {
			for (String key : qs.getBindings().keySet()) {
				bindParameter(key, qs.getBindings().get(key));
			}
		}
		if (qs.getConditionsInUpdateClause() != null) {
			List<Condition> conditions = qs.getConditionsInUpdateClause();
			bind(object, conditions.toArray(new Condition[conditions.size()]));
		}
		if (qs.getConditionsInWhereClause() != null) {
			List<Condition> conditions = qs.getConditionsInWhereClause();
			bind(object, conditions.toArray(new Condition[conditions.size()]));
		}
	}


	private void bindParameter(String name, Object value)
	{
		List<Integer> positions = queryParams.get(name);
		if (positions == null) {
			throw new QuerySpecException("No such parameter in query: " + name);
		}
		try {
			for (Integer position : positions) {
				statement.setObject(position.intValue(), value);
			}
		}
		catch (SQLException e) {
			throw new DomainObjectSQLException(e);
		}
	}


	public void insert(T object)
	{
		if (closed) {
			throw new IllegalStateException(MSG_QUERY_CLOSED);
		}
		bind(object, metadata.getAttributeFields());
		try {
			statement.executeUpdate();
			Column[] cols = metadata.getEntity().getPrimaryKeyColumns();
			if (cols == null || cols.length == 0) {
				return;
			}
			cols = metadata.getEntity().getGeneratedKeyColumns();
			if (cols == null || cols.length == 0) {
				return;
			}
			ResultSet rs = statement.getGeneratedKeys();
			for (Column col : cols) {
				DataExchangeUnit deu = metadata.getDataExchangeUnitForColumn(col.getName());
				if (deu != null) {
					rs.next();
					deu.receive(object, rs, 1);
				}
			}
			rs.close();
		}
		catch (SQLException e) {
			throw new DomainObjectSQLException(e);
		}
		finally {
			close();
		}
	}


	public int executeUpdate()
	{
		if (closed) {
			throw new IllegalStateException(MSG_QUERY_CLOSED);
		}
		try {
			bind(querySpec);
			statement.executeUpdate();
			return statement.getUpdateCount();
		}
		catch (SQLException e) {
			throw new DomainObjectSQLException(e);
		}
	}


	public int fetchInt()
	{
		if (closed) {
			throw new IllegalStateException(MSG_QUERY_CLOSED);
		}
		try {
			bind(querySpec);
			ResultSet rs = statement.executeQuery();
			if (!rs.next()) {
				// TODO throw something
			}
			return rs.getInt(1);
		}
		catch (SQLException e) {
			throw new DomainObjectSQLException(e);
		}
	}


	/**
	 * Populate the specified object with the values from the first row coming
	 * back from the SELECT statement wrapped by this {@code Query}. This method
	 * will close the {@code Query} when done.
	 * 
	 * @see #close()
	 * 
	 * @param object
	 *            The object to populate.
	 * @return Whether or not the SELECT statement returned at least one row
	 *         from which to populate the object.
	 */
	public boolean load(T object)
	{
		try {
			bind(querySpec, object);
			ResultSet rs = statement.executeQuery();
			boolean exists = rs.next();
			if (exists) {
				Util.populate(object, metadata, rs);
			}
			rs.close();
			return exists;
		}
		catch (SQLException e) {
			throw new DomainObjectSQLException(e);
		}
		finally {
			close();
		}
	}


	public boolean update(T object)
	{
		try {
			bind(querySpec, object);
			return 1 == statement.executeUpdate();
		}
		catch (SQLException e) {
			throw new DomainObjectSQLException(e);
		}
		finally {
			close();
		}
	}


	/**
	 * Create a cursor that lets you iterate over the objects created and
	 * populated from the SELECT query wrapped by this {@code Query} object.
	 * 
	 * @return The cursor
	 */
	public Cursor<T> createCursor()
	{
		if (closed) {
			throw new IllegalStateException(MSG_QUERY_CLOSED);
		}
		bind(querySpec);
		return new Cursor<T>(this, metadata);
	}


	/**
	 * Get the SQL String with which this {@code Query} was initialized.
	 * 
	 * @see #initialize(String, MetaData)
	 * 
	 * @return The SQL String with which this {@code Query} was initialized.
	 */
	public String getGeneratedSQL()
	{
		return generatedSQL;
	}


	/**
	 * Get the SQL String that was submitted to the JDBC layer. In this String
	 * all named parameters have been replaced by positional parameters (i.e.
	 * question marks), because JDBC does not support named parameters.
	 * 
	 * @return The SQL String that was submitted to the JDBC layer.
	 */
	public String getSubmittedSQL()
	{
		return submittedSQL;
	}


	PreparedStatement getStatement()
	{
		return statement;
	}


	private Query<T> createCopy()
	{
		Query<T> copy = new Query<T>(cacheKey);
		copy.generatedSQL = this.generatedSQL;
		copy.submittedSQL = this.submittedSQL;
		copy.metadata = this.metadata;
		copy.queryParams = this.queryParams;

		try {
			Connection conn = metadata.getEntity().getConnection();
			copy.statement = conn.prepareStatement(submittedSQL, Statement.RETURN_GENERATED_KEYS);
		}
		catch (SQLException e) {
			throw new DomainObjectSQLException(e);
		}

		QueryPool pool = cache.get(cacheKey);
		if (pool == null) {
			// This is a theoretical possibility in a multi-threaded environment
			// if another thread called the #destroy() method on this instance
			// just before we got to this point in the #createCopy() method, and
			// this instance was the only Query in the pool. In that case the
			// entire pool is removed from the Query cache.
			pool = new QueryPool(INIT_QUERY_POOL_SIZE);
			cache.put(cacheKey, pool);
		}
		pool.add(copy);

		initialized = true;
		closed = false;

		return copy;

	}

}
package org.domainobject.core;

import java.lang.reflect.Constructor;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.domainobject.exception.DomainObjectException;
import org.domainobject.exception.DomainObjectSQLException;
import org.domainobject.persister.StandardPersister;

/**
 * A cursor lets you iterate over persistent objects, retrieved from the
 * database through a SELECT query.
 * 
 * @param <T>
 *            The type of persistent objects that the {@code Cursor} iterates
 *            over.
 */
public class Cursor<T> implements Iterator<T>, Iterable<T> {

	private final Query<T> query;
	private final MetaData<T> metadata;
	private final ResultSet resultSet;
	private final DataExchangeUnit[] dataExchangeUnits;
	private final Constructor<T> constructor;

	private T last;


	/**
	 * Instantiate a new Cursor object. Only {@link StandardPersister} and its
	 * subclasses are allowed instantiate <code>Cursor</code>s.
	 * 
	 * @param metadadata
	 *            The persistable
	 * @param resultSet
	 *            The <code>ResultSet</code> containing the data with which to
	 *            populate the <code>DomainObject</code>s.
	 * @param autoCloseStatement
	 *            Whether or not to automatically close the
	 *            {@link java.sql.Statement} once the last record in the
	 *            <code>ResultSet</code> has been retrieved. The rule for this
	 *            is simple: if the <code>ResultSet</code> was retrieved from a
	 *            <code>PreparedStatement</code> that was added to the cache of
	 *            <code>PreparedStatement</code>s on the
	 *            {@link PersistableMetaData metadata object}, the
	 *            code>PreparedStatement</code> should not be automatically
	 *            closed by the <code>Cursor</code>. The
	 *            <code>PreparedStatement</code> may after all have to be
	 *            executed again later on. Otherwise, it's up to you, but you
	 *            should probably let the <code>Cursor</code> close the
	 *            <code>PreparedStatement</code> for you.
	 * 
	 * @throws NoSuchMethodException
	 * @throws SecurityException
	 * @throws SQLException
	 * 
	 * 
	 * @see {@link StandardPersister#createMetaData()}
	 * @see PersistableMetaData#cacheStatement(Integer, PreparedStatement)
	 */
	Cursor(Query<T> query, MetaData<T> metadadata)
	{
		try {
			this.query = query;
			this.metadata = metadadata;
			this.resultSet = query.getStatement().executeQuery();
			this.constructor = (Constructor<T>) metadata.getForClass().getConstructor();
			this.dataExchangeUnits = alignDataExchangeUnitsWithResultSet();
		}
		catch (NoSuchMethodException e) {
			close();
			throw new DomainObjectException("Missing no-arg constructor in " + metadadata.getForClass());
		}
		catch (SQLException e) {
			close();
			throw new DomainObjectSQLException(e);
		}
	}


	/**
	 * @Override
	 */
	public boolean hasNext()
	{
		try {
			if (resultSet.isClosed()) {
				return false;
			}
			if (resultSet.next()) {
				return true;
			}
			close();
			return false;
		}
		catch (SQLException e) {
			throw new DomainObjectSQLException(e);
		}
	}


	/**
	 * Get the next <code>DomainObject</code> from the <code>Cursor</code>.
	 */
	public T next()
	{
		try {
			return (last = populate(constructor.newInstance()));
		}
		catch (Exception e) {
			close();
			throw new DomainObjectException(e);
		}
	}


	/**
	 * Create and return a {@link List} of all <code>DomainObject</code>s in the
	 * <code>Cursor</code>. Note that this "runs" the <code>Cursor</code> all
	 * the way to the end. Thus, if your query returns a million records, a
	 * million <code>DomainObject</code>s are created in memory. Nevertheless,
	 * with large but manageable result sets you might prefer this method over
	 * repeatedly calling {@link #next()} as it gives you a performance benefit.
	 * 
	 * @return a List of <code>DomainObject</code>s
	 */
	public List<T> asList()
	{
		return asList(0);
	}


	/**
	 * Create and return a {@link List} of all <code>DomainObject</code>s in the
	 * <code>Cursor</code>.
	 * 
	 * @param initialCapacity
	 *            Specifies the initial capacity of the <code>List</code>.
	 *            Useful if you expect a lot of <code>DomainObject</code>s to
	 *            come back.
	 * 
	 * @return a <code>List</code> of <code>DomainObject</code>s
	 */
	public List<T> asList(int initialCapacity)
	{
		List<T> list;
		if (initialCapacity <= 0) {
			list = new ArrayList<T>();
		}
		else {
			list = new ArrayList<T>(initialCapacity);
		}
		try {
			while (resultSet.next()) {
				list.add(populate(constructor.newInstance()));
			}
			close();
			return list;
		}
		catch (Exception e) {
			close();
			throw new DomainObjectException(e);
		}
	}


	@Override
	protected void finalize()
	{
		close();
	}


	/**
	 * Deletes the <code>Persistable</code> object most recently retrieved by
	 * {@link #next()} from the database.
	 */
	public void remove()
	{
		if (last == null) {
			throw new IllegalStateException("You must call next() before calling remove()");
		}
		// TODO: Get MetaDataConfiguratorFactory from somewhere
		//new StandardPersister().delete(last);
	}


	public Iterator<T> iterator()
	{
		return this;
	}


	private T populate(T persistable) throws Exception
	{
		for (int i = 0; i < dataExchangeUnits.length; ++i) {
			dataExchangeUnits[i].receive(persistable, resultSet, i + 1);
		}
		return persistable;
	}


	// Sorts and removes data exchange units according to the columns
	// in the result set, so that binding the ResultSet to the Persistable
	// boject takes place as fast as possible.
	private DataExchangeUnit[] alignDataExchangeUnitsWithResultSet() throws SQLException
	{
		ResultSetMetaData rsmd = resultSet.getMetaData();
		ArrayList<DataExchangeUnit> list = new ArrayList<DataExchangeUnit>(rsmd.getColumnCount());
		for (int i = 1; i <= rsmd.getColumnCount(); ++i) {
			DataExchangeUnit deu = metadata.getDataExchangeUnitForColumn(rsmd.getColumnLabel(i));
			if (deu != null) {
				list.add(deu);
			}
			else if(metadata.getAdaptability() == MetaData.Adaptability.EXTEND) {
				Column column = new ResultSetColumn(rsmd, i);
				deu = metadata.getConfigurator().getDataExchangeUnit(column);
				if(deu != null) {
					list.add(deu);
				}
			}
		}
		return list.toArray(new DataExchangeUnit[list.size()]);
	}


	private void close()
	{
		try {
			query.close();
			if (resultSet != null) {
				resultSet.close();
			}
		}
		catch (SQLException e) {
			throw new DomainObjectSQLException(e);
		}
	}
}
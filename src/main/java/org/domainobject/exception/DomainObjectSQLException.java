package org.domainobject.exception;

import java.sql.SQLException;

import org.domainobject.persister.StandardPersister;

/**
 * An unchecked exception wrapping a {@link java.sql.SQLException}. Only
 * {@link StandardPersister} throws this Exception. If a {@link QueryGenerator
 * SQLString adapter} in the domainobject package stumbles upon a SQLException,
 * it throws a {@link SQLGenerationException}. You should consider doing the
 * same when extending StandardPersister c.q. implementing SQLString adapter.
 */
public class DomainObjectSQLException extends DomainObjectException {

	private static final long serialVersionUID = 6439966542083486856L;

	private final SQLException sqlException;


	public DomainObjectSQLException(final SQLException e)
	{
		this.sqlException = e;
	}


	/**
	 * Returns the message from the wrapped SQLException.
	 * 
	 * @return The message from the wrapped SQLException.
	 */
	@Override
	public String getMessage()
	{
		return sqlException.getMessage();
	}


	/**
	 * Returns the error code from the wrapped SQLException.
	 * 
	 * @return The error code from the wrapped SQLException.
	 */
	public int getErrorCode()
	{
		return sqlException.getErrorCode();
	}


	/**
	 * Returns the SQLString state from the wrapped SQLException.
	 * 
	 * @return The SQLString state from the wrapped SQLException.
	 */
	public String getSQLState()
	{
		return sqlException.getSQLState();
	}


	/**
	 * Returns the wrapped SQLException.
	 * 
	 * @return The wrapped SQLException.
	 */
	public SQLException getSQLException()
	{
		return sqlException;
	}

}

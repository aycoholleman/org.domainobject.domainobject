package org.domainobject.orm.exception;

/**
 * Thrown by methods the QuerySpec upon detecting an error.
 */
public class QuerySpecException extends DomainObjectException {

	private static final long serialVersionUID = 1L;


	public QuerySpecException()
	{
	}


	public QuerySpecException(String arg0)
	{
		super(arg0);
	}


	public QuerySpecException(Throwable cause)
	{
		super(cause);
	}


	public QuerySpecException(String message, Throwable cause)
	{
		super(message, cause);
	}

}

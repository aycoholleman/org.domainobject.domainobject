package org.domainobject.orm.exception;

/**
 * Thrown when you specify an invalid {@link Condition}. This usually means you
 * have specified an invalid operator-value combination.
 */
public class InvalidConditionException extends DomainObjectException {

	private static final long serialVersionUID = 1L;


	public InvalidConditionException()
	{
		super();
	}


	public InvalidConditionException(String message, Throwable cause)
	{
		super(message, cause);
	}


	public InvalidConditionException(String arg0)
	{
		super(arg0);
	}


	public InvalidConditionException(Throwable cause)
	{
		super(cause);
	}

}

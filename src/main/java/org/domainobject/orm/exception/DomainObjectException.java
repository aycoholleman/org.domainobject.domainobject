package org.domainobject.orm.exception;

/**
 * The root of all runtime exceptions in the domainobject package. 
 */
public class DomainObjectException extends RuntimeException {
	
	private static final long serialVersionUID = -5483708805255513358L;


	public static RuntimeException rethrow(Throwable t) {
		if(t instanceof RuntimeException) {
			return (RuntimeException) t;
		}
		return new DomainObjectException(t);
	}

	public DomainObjectException()
	{
	}


	public DomainObjectException(String arg0)
	{
		super(arg0);
	}


	public DomainObjectException(Throwable cause)
	{
		super(cause);
	}


	public DomainObjectException(String message, Throwable cause)
	{
		super(message, cause);
	}

}

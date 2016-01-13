package org.domainobject.orm.exception;

/**
 * Thrown when a {@link StandardPersister} cannot complete some persistency operation
 * because the {@link Persistable} object it operates upon does not provide
 * the information needed for the operation.
 */
public class InsufficientMetaDataException extends DomainObjectException {

	private static final long serialVersionUID = 1L;


	public InsufficientMetaDataException()
	{
		super();
	}


	public InsufficientMetaDataException(String message, Throwable cause)
	{
		super(message, cause);
	}


	public InsufficientMetaDataException(String arg0)
	{
		super(arg0);
	}


	public InsufficientMetaDataException(Throwable cause)
	{
		super(cause);
	}

}

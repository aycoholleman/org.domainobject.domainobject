package org.domainobject.orm.exception;


/**
 * Thrown when something goes wrong during the assembly or retrieval of a
 * {@link PersistableMetaData} object.
 */
public class MetaDataAssemblyException extends DomainObjectException {

	private static final long serialVersionUID = 1L;


	public MetaDataAssemblyException()
	{
		super();
	}


	public MetaDataAssemblyException(String message, Throwable cause)
	{
		super(message, cause);
	}


	public MetaDataAssemblyException(String arg0)
	{
		super(arg0);
	}


	public MetaDataAssemblyException(Throwable cause)
	{
		super(cause);
	}

}

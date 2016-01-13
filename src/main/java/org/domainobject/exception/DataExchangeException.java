package org.domainobject.exception;

import org.domainobject.core.DataExchangeUnit;

public class DataExchangeException extends DomainObjectException {
	
	private static final long serialVersionUID = 4122630002829574816L;

	private final DataExchangeUnit deu;
	private final boolean whileSending;


	public DataExchangeException(DataExchangeUnit deu, boolean whileSending)
	{
		super();
		this.deu = deu;
		this.whileSending = whileSending;
	}

	public DataExchangeException(DataExchangeUnit deu, boolean whileSending, BindException cause)
	{
		super(cause);
		this.deu = deu;
		this.whileSending = whileSending;
	}
	
	@Override
	public String getMessage() {
		StringBuilder sb = new StringBuilder(64);
		sb.append("Error while ");
		if(whileSending) {
			sb.append("sending data from field ").append(deu.getField().getName());
			sb.append("to column ").append(deu.getColumn().getName());
		}
		else {
			sb.append("receiving data from column ").append(deu.getColumn().getName());
			sb.append("for field ").append(deu.getField().getName());
		}
		Throwable cause = getCause();
		if(cause != null) {
			if(cause instanceof BindException) {
				sb.append(" using a ").append(deu.getBinder().getClass().getName());
				sb.append(": ").append(cause.getMessage());
			}
		}
		return sb.toString();
	}

}

package org.domainobject.orm.exception;


import org.domainobject.orm.persister.StandardPersister;

/**
 * Thrown by {@link StandardPersister#loadParent(Class, String...)} if the _Persistable
 * object that the StandardPersister operates upon appears to have no parent of the
 * specified type.
 */
public class OrphanException extends DomainObjectException {

	private static final long serialVersionUID = 1L;

	private final Object child;
	private final Object parent;


	public OrphanException(Object child, Object parent)
	{
		this.child = child;
		this.parent = parent;
	}


	public String getMessage()
	{
		//TODO: make sensible message for orphan exception
		return null;

//		String primaryKey = ArrayUtil.implode(Util.getPrimaryKey(child));
//		String foreignKey = ArrayUtil.implode(Util.getForeignKeyVales(child, parent));
//
//		StringBuilder message = new StringBuilder();
//
//		message.append("The ").append(child.getClass().getSimpleName());
//		message.append(" with primary key ").append(primaryKey);
//		message.append(" and foreign key ").append(foreignKey);
//		message.append(" is an orphan");
//
//		return message.toString();
	}

}

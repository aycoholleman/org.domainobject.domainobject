package org.domainobject.orm.bind;

import java.util.HashMap;

import org.domainobject.orm.core.MetaDataConfigurator;
import org.domainobject.orm.exception.MetaDataAssemblyException;

/**
 * The default binder repository when assembling metadata objects.
 * 
 * @see MetaDataConfigurator#setBinderRepository(BinderRepository)
 */
public class DefaultBinderRepository implements BinderRepository {

	private static DefaultBinderRepository sharedInstance;

	public static DefaultBinderRepository getSharedInstance()
	{
		if (sharedInstance == null) {
			sharedInstance = new DefaultBinderRepository();
		}
		return sharedInstance;
	}

	private final HashMap<Class<?>, Binder> binders = new HashMap<Class<?>, Binder>();

	public DefaultBinderRepository()
	{

		binders.put(String.class, new StringBinder());

		Binder stringyBinder = new StringyBinder();
		binders.put(char.class, stringyBinder);
		binders.put(Character.class, stringyBinder);
		binders.put(StringBuilder.class, stringyBinder);
		binders.put(StringBuffer.class, stringyBinder);

		IntegerBinder intBinder = new IntegerBinder();
		binders.put(int.class, intBinder);
		binders.put(Integer.class, intBinder);

		LongBinder longBinder = new LongBinder();
		binders.put(long.class, longBinder);
		binders.put(Long.class, longBinder);

	}

	public Binder getBinder(Class<?> forClass)
	{
		for (Class<?> c = forClass; c != null; c = c.getSuperclass()) {
			Binder binder = binders.get(forClass);
			if (binder != null) {
				return binder;
			}
		}
		throw new MetaDataAssemblyException("No suitable Binder found for class "
				+ forClass.getName());
	}

	public void addBinder(Class<?> forClass, Binder binder)
	{
		binders.put(forClass, binder);
	}

}

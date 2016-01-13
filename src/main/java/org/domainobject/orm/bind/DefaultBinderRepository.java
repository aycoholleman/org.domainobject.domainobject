package org.domainobject.orm.bind;

import java.util.HashMap;

import org.domainobject.orm.core.MetaDataConfigurator;

/**
 * The default binder repository when assembling metadata objects.
 * 
 * @see MetaDataConfigurator#setBinderRepository(IBinderRepository)
 */
public class DefaultBinderRepository implements IBinderRepository {

	private static DefaultBinderRepository instance;

	public static DefaultBinderRepository getSharedInstance()
	{
		if (instance == null) {
			instance = new DefaultBinderRepository();
		}
		return instance;
	}

	private final HashMap<Class<?>, Binder> binders;

	private boolean checkSuperClasses = true;

	private DefaultBinderRepository()
	{
		binders = new HashMap<>();
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

	public void setBinder(Class<?> forClass, Binder binder)
	{
		binders.put(forClass, binder);
	}

	@Override
	public Binder getBinder(Class<?> forClass)
	{
		if (!checkSuperClasses)
			return binders.get(forClass);
		while (forClass != null) {
			Binder binder = binders.get(forClass);
			if (binder != null)
				return binder;
			forClass = forClass.getSuperclass();
		}
		return null;
	}

	/**
	 * Whether or not to check if a binder exists for the super class of a class
	 * if no binder exists for the class itself.
	 * 
	 * @return
	 */
	public boolean isCheckSuperClasses()
	{
		return checkSuperClasses;
	}

	/**
	 * Specifies whether or not to check if a binder exists for the super class
	 * of a class if no binder exists for the class itself.
	 * 
	 * @param checkSuperClasses
	 */
	public void setCheckSuperClasses(boolean checkSuperClasses)
	{
		this.checkSuperClasses = checkSuperClasses;
	}

}

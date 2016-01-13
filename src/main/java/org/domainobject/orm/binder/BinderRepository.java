package org.domainobject.orm.binder;

import org.domainobject.orm.core.MetaDataConfigurator;

/**
 * <p>
 * A {@code BinderRepository} functions as the initial set of {@link Binder}s to
 * choose from when assembling the metadata object for a persistent class. Each
 * persistent field in the persistent class (each field that maps to a column)
 * is assigned a {@code Binder} during metadata assembly, dependent on the type
 * of the field. That {@code Binder} is, in principle, chosen from a
 * {@code BinderRepository}, but you can override this using
 * {@link MetaDataConfigurator#setBinder(Class, Binder)} or
 * {@link MetaDataConfigurator#setFieldBinder(String, Binder)}.
 * </p>
 * 
 * @see Binder
 * @see MetaDataConfigurator
 */
public interface BinderRepository {

	Binder getBinder(Class<?> forClass);


	void addBinder(Class<?> forClass, Binder binder);

}
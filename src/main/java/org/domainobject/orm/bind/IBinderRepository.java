package org.domainobject.orm.bind;

import org.domainobject.orm.core.MetaData;
import org.domainobject.orm.core.MetaDataConfigurator;

/**
 * 
 * A {@code BinderRepository} functions as an initial set of {@link Binder}s to
 * choose from when assembling a {@link MetaData metadata object}.
 * 
 * @see Binder
 * @see MetaDataConfigurator#setBinderRepository(BinderRepository)
 */
public interface IBinderRepository {

	Binder getBinder(Class<?> forClass);

}
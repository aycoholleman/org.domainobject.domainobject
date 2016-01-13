package org.domainobject.orm.bind;

import org.domainobject.orm.core.MetaData;
import org.domainobject.orm.core.MetaDataConfigurator;

/**
 * 
 * A {@code BinderRepository} functions as an initial set of {@link IBinder}s to
 * choose from when assembling a {@link MetaData metadata object}.
 * 
 * @see IBinder
 * @see MetaDataConfigurator#setBinderRepository(BinderRepository)
 */
public interface IBinderRepository {

	IBinder getBinder(Class<?> forClass);

}
package org.domainobject.orm.persister;

import org.domainobject.orm.core.Context;
import org.domainobject.orm.core.MetaData;
import org.domainobject.orm.generator.StandardQueryGenerator;

public abstract class DomainObject {

	private final MetaData<DomainObject> metadata;
	private final StandardPersisterDedicated<DomainObject> spd;


	@SuppressWarnings("unchecked")
	public DomainObject()
	{
		metadata = (MetaData<DomainObject>) Context.getDefaultContext().createMetaData(getClass());
		StandardQueryGenerator sqg = StandardQueryGenerator.getGenerator(metadata.getContext());
		spd = new StandardPersisterDedicated<DomainObject>(metadata, sqg);
	}


	public DomainObject load(int id, String... select)
	{
		return spd.load(this, id, select);
	}


	public void save()
	{
		spd.save(this);
	}

}

package org.domainobject.persister;

import org.domainobject.core.Context;
import org.domainobject.core.MetaData;
import org.domainobject.generator.StandardQueryGenerator;

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

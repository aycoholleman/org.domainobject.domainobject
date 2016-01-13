package org.domainobject.persister;

import org.domainobject.core.MetaData;
import org.domainobject.generator.StandardQueryGenerator;

/**
 * A {@code StandardPersisterDedicated} performs common persistency operations
 * on persistent objects. It features exactly the same operations as its sister
 * classes {@link StandardPersisterDynamic} and {@link StandardPersister}.
 * 
 * @param <T> The type of persistent objects operated upon.
 */
public class StandardPersisterDedicated<T> {

	private final StandardPersisterDynamic spd;
	private final MetaData<T> metadata;


	public StandardPersisterDedicated(MetaData<T> metadata)
	{
		StandardQueryGenerator sqg = StandardQueryGenerator.getGenerator(metadata.getContext());
		this.spd = new StandardPersisterDynamic(sqg);
		this.metadata = metadata;
	}


	public StandardPersisterDedicated(MetaData<T> metadata, StandardQueryGenerator sqg)
	{
		this.spd = new StandardPersisterDynamic(sqg);
		this.metadata = metadata;
	}


	public T load(T object, int id, String... select)
	{
		return spd.load(object, metadata, id, select);
	}


	public T load(T object, long id, String... select)
	{
		return spd.load(object, metadata, id, select);
	}


	public T load(T object, String... select)
	{
		return spd.loadUsingKey(object, metadata, select);
	}


	public T loadUsingKeyWithValue(T object, String key, Object value, String... select)
	{
		return spd.loadUsingKeyWithValue(object, metadata, key, value, select);
	}


	public T loadUsingKeyWithValue(T object, String[] key, Object[] value, String... select)
	{
		return spd.loadUsingKeyWithValue(object, metadata, key, value, select);
	}


	public T loadUsingKey(T object, String[] key, String... select)
	{
		return spd.loadUsingKey(object, metadata, key, select);
	}


	public <U> U loadParent(Object object, U parent, String... foreignKey)
	{
		MetaData<U> parentMetaData = getMetaData(parent);
		return spd.loadParent(object, metadata, parent, parentMetaData, foreignKey);
	}


	public void save(T object)
	{
		spd.save(object, metadata);
	}


	private <U> MetaData<U> getMetaData(U object)
	{
		@SuppressWarnings("unchecked")
		MetaData<U> m = (MetaData<U>) metadata.getContext().createMetaData(object.getClass());
		return m;
	}
}

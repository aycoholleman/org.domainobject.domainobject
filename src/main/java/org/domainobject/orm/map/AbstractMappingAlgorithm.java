package org.domainobject.orm.map;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

/**
 * Default implementation used for {@link IMappingAlgorithm}. Unless you
 * explicitly provide your own implementation when configuring metadata objects,
 * you can cast the metadata configurator's {@link IMappingAlgorithm} to an
 * {@link AbstractMappingAlgorithm} instance and have access to setters like
 * {@link #setMapStaticFields(boolean)}.
 * 
 * @author Ayco Holleman
 *
 */
public abstract class AbstractMappingAlgorithm implements IMappingAlgorithm {

	private boolean mapStaticFields = false;
	private boolean mapTransientFields = false;
	private boolean mapSuperClassFields = false;

	@Override
	public String mapClassToEntityName(Class<?> cls)
	{
		return mapFieldNameToColumnName(cls.getSimpleName());
	}

	@Override
	public String mapFieldToColumnName(Field field, Class<?> cls)
	{
		if (field.getDeclaringClass() != cls && !mapSuperClassFields)
			return null;
		int modifiers = field.getModifiers();
		if (Modifier.isStatic(modifiers) && !mapStaticFields)
			return null;
		if (Modifier.isTransient(modifiers) && !mapTransientFields)
			return null;
		return mapFieldNameToColumnName(field.getName());
	}

	/**
	 * Maps the specified field name to a column name. To be implemented by
	 * concrete subclasses.
	 * 
	 * @param fieldName
	 * @return
	 */
	protected abstract String mapFieldNameToColumnName(String fieldName);

	/**
	 * Returns whether or not static fields are mapped. By default static fields
	 * are <i>not</i> mapped.
	 * 
	 * @return
	 */
	public boolean isMapStaticFields()
	{
		return mapStaticFields;
	}

	/**
	 * Whether or not to map static fields. By default static fields are
	 * <i>not</i> mapped.
	 * 
	 * @param mapStaticFields
	 */
	public void setMapStaticFields(boolean mapStaticFields)
	{
		this.mapStaticFields = mapStaticFields;
	}

	/**
	 * Returns whether or not transient fields are mapped. By default transient
	 * fields are <i>not</i> mapped.
	 * 
	 * @return
	 */
	public boolean isMapTransientFields()
	{
		return mapTransientFields;
	}

	/**
	 * Whether or not to map transient fields. By default transient fields are
	 * <i>not</i> mapped.
	 * 
	 * @param mapTransientFields
	 */
	public void setMapTransientFields(boolean mapTransientFields)
	{
		this.mapTransientFields = mapTransientFields;
	}

	/**
	 * Returns whether or not super class fields are mapped. By default super
	 * class fields are <i>not</i> mapped.
	 * 
	 * @return
	 */
	public boolean isMapSuperClassFields()
	{
		return mapSuperClassFields;
	}

	/**
	 * Whether or not to map super class fields. By default super class fields
	 * are <i>not</i> mapped.
	 * 
	 * @param mapSuperClassFields
	 */
	public void setMapSuperClassFields(boolean mapSuperClassFields)
	{
		this.mapSuperClassFields = mapSuperClassFields;
	}

}

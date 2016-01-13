package org.domainobject.orm.map;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;


public class BasicMappingAlgorithm implements IMappingAlgorithm {

	public String mapClassToEntityName(Class<?> cls)
	{
		return mapFieldNameToColumnName(cls.getSimpleName());
	}


	public String mapFieldToColumnName(Field field, Class<?> cls)
	{
		if (field.getDeclaringClass() != cls) {
			return null;
		}
		int modifiers = field.getModifiers();
		if (Modifier.isStatic(modifiers) || Modifier.isTransient(modifiers)) {
			return null;
		}
		return mapFieldNameToColumnName(field.getName());
	}


	protected String mapFieldNameToColumnName(String fieldName)
	{
		return fieldName;
	}

}

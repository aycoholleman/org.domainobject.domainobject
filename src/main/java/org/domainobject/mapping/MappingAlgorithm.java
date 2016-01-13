package org.domainobject.mapping;


import java.lang.reflect.Field;

public interface MappingAlgorithm {

	String mapClassToEntityName(Class<?> cls);

	String mapFieldToColumnName(Field field, Class<?> cls);

}

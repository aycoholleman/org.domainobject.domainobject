package org.domainobject.orm.map;


import java.lang.reflect.Field;

public interface IMappingAlgorithm {

	String mapClassToEntityName(Class<?> cls);

	String mapFieldToColumnName(Field field, Class<?> cls);

}

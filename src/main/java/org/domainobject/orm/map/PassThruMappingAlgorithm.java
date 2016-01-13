package org.domainobject.orm.map;

/**
 * Maps class names and fields names as-is to table names and column names.
 * 
 * @author Ayco Holleman
 *
 */
public class PassThruMappingAlgorithm extends AbstractMappingAlgorithm {

	public PassThruMappingAlgorithm()
	{
		super();
	}

	@Override
	protected String mapFieldNameToColumnName(String fieldName)
	{
		return fieldName;
	}

}

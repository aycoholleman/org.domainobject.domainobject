package org.domainobject.orm.map;

import static java.lang.Character.isLowerCase;
import static java.lang.Character.isUpperCase;
import static java.lang.Character.toUpperCase;

/**
 * Maps class names and field names to uppercase table names and column names,
 * using the underscore character to separate the name parts.
 * 
 * @see LowerCaseMappingAlgorithm
 * 
 * @author Ayco Holleman
 *
 */
public class UpperCaseMappingAlgorithm extends AbstractMappingAlgorithm {

	@Override
	protected String mapFieldNameToColumnName(String fieldName)
	{
		char[] chars = fieldName.toCharArray();
		int maxLen = (int) Math.ceil(chars.length * 1.5F);
		char[] colName = new char[maxLen];
		colName[0] = toUpperCase(chars[0]);
		int j = 1;
		for (int i = 1; i < chars.length; ++i) {
			if (isUpperCase(chars[i])) {
				if ((i != (chars.length - 1)) && isLowerCase(chars[i + 1])) {
					colName[j++] = '_';
					colName[j++] = chars[i];
				}
				else if (isLowerCase(chars[i - 1])) {
					colName[j++] = '_';
					colName[j++] = toUpperCase(chars[i]);
				}
				else {
					colName[j++] = toUpperCase(chars[i]);
				}
			}
			else {
				colName[j++] = toUpperCase(chars[i]);
			}
		}
		return new String(colName, 0, j);
	}

}

package org.domainobject.mapping;

import org.domainobject.core.Entity;

/**
 * The LowerCaseMappingAlgorithm maps field names to column names according to common
 * naming conventions. Java properties are assumed to be camelCase, while column
 * names are assumed to be lower_case_with_underscores. Examples:
 * 
 * <pre>
 * lastName -> last_name
 * isUpperCase -> is_upper_case
 * angryManWithABadPTSTSyndrom -> angry_man_with_a_bad_ptst_syndrom
 * </pre>
 * 
 * Note that in some cases, this mapper will not work correctly with the article
 * "a" and other one-letter words. For example
 * <code>angryManWithAPTSTSyndrom</code> maps to
 * <code>angry_man_with_aptst_syndrom</code>, not to
 * </code>angry_man_with_a_ptst_syndrom</code> as you would probably hope.
 * 
 * The same logic used to map properties to columns is used to map class names
 * to {@link Entity}s. E.g. class LowerCaseMappingAlgorithm would map to table/view/query
 * lower_case_mapper.
 * 
 * @see UpperCaseMapper
 */
public class LowerCaseMappingAlgorithm extends BasicMappingAlgorithm {

	@Override
	protected String mapFieldNameToColumnName(String fieldName)
	{

		char[] chars = fieldName.toCharArray();

		// The length of the column can never exceed 1.5 times the length
		// of the property. Only when the letters in the field name
		// strictly alternate between uppercase and lowercase letters will
		// this maximum be reached. For example "AbCdEfGh" maps to
		// "ab_cd_fg_gh"; for every two letters there is one underscore.
		int maxColumnLength = (int) Math.ceil((float) chars.length * 1.5F);
		char[] column = new char[maxColumnLength];

		column[0] = isUpper(chars[0]) ? toLower(chars[0]) : chars[0];

		int j = 1;
		for (int i = 1; i < chars.length; ++i) {
			if (isUpper(chars[i])) {
				if ((i != (chars.length - 1)) && isLower(chars[i + 1])) {
					column[j++] = '_';
					column[j++] = toLower(chars[i]);
				}
				else if (isLower(chars[i - 1])) {
					column[j++] = '_';
					column[j++] = toLower(chars[i]);
				}
				else {
					column[j++] = toLower(chars[i]);
				}
			}
			else {
				column[j++] = chars[i];
			}
		}
		return new String(column, 0, j);
	}


	private static boolean isUpper(char c)
	{
		return Character.isUpperCase(c);
	}


	private static boolean isLower(char c)
	{
		return Character.isLowerCase(c);
	}


	public static char toLower(char c)
	{
		return Character.toLowerCase(c);
	}

}

package org.domainobject.orm.map;

import static java.lang.Character.isLowerCase;
import static java.lang.Character.isUpperCase;
import static java.lang.Character.toLowerCase;

/**
 * <p>
 * Maps class names and field names to lowercase table names and column names,
 * using the underscore character to separate the name parts. Examples:
 * <ul>
 * <li>lastName &#8594; last_name
 * <li>isUpperCase &#8594; is_upper_case
 * <li>patientWithASeverePTSTSyndrom &#8594; patient_with_a_severe_ptst_syndrom
 * </ul>
 * <p>
 * Note that with one-letter words like the article &#34;a&#34; this mapper will
 * not produce the likely intended result:
 * <ul>
 * <li>patientWithAPTSTSyndrom &#8594; patient_with_aptst_syndrom
 * </ul>
 * <p>
 * The logic used to map fields to columns is also used to map class names to
 * table names.
 */
public class LowerCaseMappingAlgorithm extends AbstractMappingAlgorithm {

	@Override
	protected String mapFieldNameToColumnName(String fieldName)
	{
		char[] chars = fieldName.toCharArray();
		/*
		 * Length of column name can never exceed 1.5 times the length of the
		 * field name. Only when the letters in the field name strictly
		 * alternate between uppercase and lowercase letters will this maximum
		 * be reached. For example "AbCdEfGh" maps to "ab_cd_fg_gh".
		 */
		int maxLen = (int) Math.ceil(chars.length * 1.5F);
		char[] colName = new char[maxLen];
		colName[0] = isUpperCase(chars[0]) ? toLowerCase(chars[0]) : chars[0];
		int j = 1;
		for (int i = 1; i < chars.length; ++i) {
			if (isUpperCase(chars[i])) {
				if ((i != (chars.length - 1)) && isLowerCase(chars[i + 1])) {
					colName[j++] = '_';
					colName[j++] = toLowerCase(chars[i]);
				}
				else if (isLowerCase(chars[i - 1])) {
					colName[j++] = '_';
					colName[j++] = toLowerCase(chars[i]);
				}
				else {
					colName[j++] = toLowerCase(chars[i]);
				}
			}
			else {
				colName[j++] = chars[i];
			}
		}
		return new String(colName, 0, j);
	}

}

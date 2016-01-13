package org.domainobject.orm.map;

public class UpperCaseMappingAlgorithm extends BasicMappingAlgorithm {

	protected String mapFieldNameToColumnName(String fieldName)
	{

		char[] chars = fieldName.toCharArray();

		// The length of the column can never exceed 1.5 times the length
		// of the property. Only when the letters in the property name
		// strictly alternate between uppercase and lowercase letters will
		// this maximum be reached. For example "AbCdEfGh" maps to
		// "AB_CD_EF_GH"; for every two letters there is one underscore.
		int maxColumnLength = (int) Math.ceil((float) chars.length * 1.5F);
		char[] column = new char[maxColumnLength];

		column[0] = toUpper(chars[0]);

		int j = 1;
		for (int i = 1; i < chars.length; ++i) {
			if (isUpper(chars[i])) {
				if ((i != (chars.length - 1)) && isLower(chars[i + 1])) {
					column[j++] = '_';
					column[j++] = chars[i];
				}
				else if (isLower(chars[i - 1])) {
					column[j++] = '_';
					column[j++] = toUpper(chars[i]);
				}
				else {
					column[j++] = toUpper(chars[i]);
				}
			}
			else {
				column[j++] = toUpper(chars[i]);
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


	public static char toUpper(char c)
	{
		return Character.toUpperCase(c);
	}
}

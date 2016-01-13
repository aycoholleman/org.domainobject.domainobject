package org.domainobject.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.domainobject.exception.SQLGenerationException;

/**
 * Utility class that replaces named parameters in a SQL query with positional
 * parameters and, in the process, produces a list of all parameter names and
 * their position(s) within the query. Note that a named parameter may occur
 * multiple times in a SQL query, for example:
 * {@code SELECT * FROM BOOKS WHERE AUTHOR = :name OR TITLE = :name}.
 */
class ParameterExtractor {

	private final String in;
	private final StringBuilder out;
	private final Map<String, List<Integer>> params = new HashMap<String, List<Integer>>();

	private int paramPosition = 0;

	ParameterExtractor(String sql)
	{
		this.in = sql;
		this.out = new StringBuilder(sql.length());
		process();
	}

	/**
	 * Get all named parameters and their position(s) in the query string.
	 * 
	 * @return All named parameters and their position(s) in the query string.
	 */
	Map<String, List<Integer>> getParameters()
	{
		return params;
	}

	/**
	 * Get the total number of parameters within the query string.
	 * 
	 * @return The total number of parameters within the query string.
	 */
	int getParameterCount()
	{
		return paramPosition;
	}

	/**
	 * A query string in which all named parameters have been replaced with
	 * positional parameters (i.e. question marks).
	 * 
	 * @return The processed SQL
	 */
	String getProcessedSQL()
	{
		return out.toString();
	}

	private void process()
	{
		boolean inString = false;
		boolean inParam = false;
		boolean escaped = false;
		StringBuilder param = null;
		for (int i = 0; i < in.length(); ++i) {
			char c = in.charAt(i);
			if (inString) {
				out.append(c);
				if (c == '\'') {
					if (!escaped)
						inString = false;
					else
						escaped = true;
				}
				else if (c == '\\')
					escaped = true;
				else
					escaped = false;
			}
			else if (inParam) {
				if (isParameterCharacter(c)) {
					param.append(c);
					if (i == in.length() - 1) {
						addParameter(param);
					}
				}
				else {
					addParameter(param);
					out.append(c);
					inParam = false;
					if (c == '\'') {
						inString = true;
					}
					else if (c == ':') {
						/*
						 * Two adjacent parameters ... this can never be valid
						 * SQL, so we might as well stop here and throw an
						 * Exception. But we don't, because we are not an SQL
						 * parser.
						 */
						out.append('?');
						inParam = true;
						param = new StringBuilder();
					}
				}
			}
			else if (c == ':') {
				out.append('?');
				inParam = true;
				param = new StringBuilder();
			}
			else {
				out.append(c);
				if (c == '\'') {
					inString = true;
				}
			}
		}
	}

	private static boolean isParameterCharacter(char c)
	{
		return Character.isLetterOrDigit(c) || c == '_';
	}

	private void addParameter(StringBuilder param)
	{
		if (param.length() == 0) {
			throw new SQLGenerationException("Zero length parameter found in query string");
		}
		String parameter = param.toString();
		List<Integer> positions = params.get(parameter);
		if (positions == null) {
			positions = new ArrayList<Integer>(4);
			params.put(parameter, positions);
		}
		positions.add(++paramPosition);
	}

}

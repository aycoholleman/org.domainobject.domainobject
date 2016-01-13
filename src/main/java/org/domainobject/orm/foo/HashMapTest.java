package org.domainobject.orm.foo;

import java.util.HashMap;

import org.domainobject.util.debug.BeanPrinter;

public class HashMapTest {

	/**
	 * @param args
	 */
	public static void main(String[] args)
	{
		Object obj = new Object();
		HashMap<String, Object> map = new HashMap<String, Object>();
		String s1 = "Hello";
		String s2 = "Hello";
		map.put(s1, obj);
		map.put(s2, obj);
		System.out.println("obj hashcode: " + obj.hashCode());
		System.out.println("s1 hashcode: " + s1.hashCode());
		System.out.println("s2 hashcode: " + s2.hashCode());
	}

}

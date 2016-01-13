package org.domainobject.orm.foo;

import java.util.HashMap;
import java.util.Map;

public class HashMapKeyTest {

	private static class Test0 {
		private int field = 0;


		@Override
		public boolean equals(Object obj)
		{
			return true;
		}


		@Override
		public int hashCode()
		{
			return 2;
		}
	}

	private static class Test1 {
		private int field = 0;


		@Override
		public boolean equals(Object obj)
		{
			return true;
		}


		@Override
		public int hashCode()
		{
			return 2;
		}
	}


	/**
	 * @param args
	 */
	public static void main(String[] args)
	{
		Map map = new HashMap();
		Test0 test0 = new Test0();
		Test1 test1 = new Test1();
		map.put(test0, "hallo");
		map.put(test1, "hallo");
		System.out.println("Hashmap size: " + map.size());
	}

}

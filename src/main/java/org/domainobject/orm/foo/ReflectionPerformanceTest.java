package org.domainobject.orm.foo;

import java.lang.reflect.Field;

public class ReflectionPerformanceTest {

	public static void main(String[] args) throws Exception
	{
		
		final Field f = Article.class.getDeclaredField("id");
		f.setAccessible(true);
		
		final Article a = new Article();
		
		final long start = System.currentTimeMillis();
		
		final long iterations = 5000000000L;
		
		
		for(long i = 0L; i< iterations;++i) {
			f.setLong(a, i);
		}
		
		
//		for(long i = 0L; i< iterations;++i) {
//			f.set(a, i);
//		}
		
		
//		for(long i = 0L; i< iterations;++i) {
//			a.id = i;
//		}
		
		
		System.out.println("seconds: " +  (((double)System.currentTimeMillis() - (double) start)/(double) 1000));

	}

}

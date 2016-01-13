package org.domainobject.foo;

import java.sql.SQLException;

public class Test2 {
	
	int var = 0;

	public static void main(String[] args) throws Exception
	{
		Test2 t = new Test2();
		long start = System.currentTimeMillis();
		for(int i=0; i<1000000000;++i) {
			t.method0(i);
		}
		System.out.println("millis: " +  (((double)System.currentTimeMillis() - (double) start)/(double) 1000));
	}
	
	public void method0(int i) {
		int j = -100;
		try {
			if(i % 20 == 0) {
				j--;
			}
			else {
				var = i + j;
			}
		}
		catch(Exception e) {
			var = 1;
			throw new RuntimeException(e);
		}
	}
	
	public void method1(int i) throws Exception {
		int j = -100;
		if(i % 20 == 0) {
			j--;
		}
		else {
			var = i + j;
		}
	}
	
	public void method2(int i) throws SQLException {
		if(i == 1000000000) {
			throw new SQLException();
		}
	}
	
	
	

}

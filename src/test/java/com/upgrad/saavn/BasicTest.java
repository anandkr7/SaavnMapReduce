package com.upgrad.saavn;

import org.junit.Assert;
import org.junit.Test;

public class BasicTest {

	@Test
	public void testBasic() {

		int first = 1;
		int second = 2;

		Assert.assertTrue("Invalid sum", (first + second) == 3);

	}
	
	@Test
	public void testMinus() {

		int first = 1;
		int second = 2;

		Assert.assertTrue("Invalid sum", (second - first) == 1);

	}

}

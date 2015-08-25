package proj.zoie.test;

import junit.framework.Test;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

public class ZoieTestSuite extends TestSuite {

	public static Test suite()
	{
        TestSuite suite=new TestSuite();
        suite.addTest(new ZoieTest("testStreamDataProvider"));
        suite.addTest(new ZoieTest("testRealtime"));
        suite.addTest(new ZoieTest("testAsyncDataConsumer"));
        suite.addTest(new ZoieTest("testDelSet"));
        return suite;
	}

	public static void main(String[] args) {
		TestRunner.run(suite());
	}
}

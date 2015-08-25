package proj.zoie.server;

import mx4j.tools.adaptor.http.HttpAdaptor;

import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import proj.zoie.impl.indexing.ZoieSystem;

public class ZoieServer {
	private static final Logger log = Logger.getLogger(ZoieServer.class);
	/**
	 * @param args
	 */
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		ApplicationContext appCtx=new FileSystemXmlApplicationContext("server/conf/applicationContext.spring");
		
		final ZoieSystem zoieSystem=(ZoieSystem)appCtx.getBean("indexingSystem");
		
		zoieSystem.start();
		
		final HttpAdaptor jmxHttpAdaptor=(HttpAdaptor)appCtx.getBean("httpAdaptor");
		jmxHttpAdaptor.start();
		
		Runtime.getRuntime().addShutdownHook(new Thread(){
			public void run()
			{
                log.info("stopping jmx http adaptor...");
				jmxHttpAdaptor.stop();
				zoieSystem.shutdown();
			}
		});
	}
}

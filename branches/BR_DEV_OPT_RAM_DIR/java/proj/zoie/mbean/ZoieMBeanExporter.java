package proj.zoie.mbean;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.jmx.export.MBeanExporter;
import org.springframework.remoting.httpinvoker.HttpInvokerProxyFactoryBean;

import proj.zoie.service.api.SearchRequest;
import proj.zoie.service.api.SearchResult;
import proj.zoie.service.api.ZoieSearchService;

public class ZoieMBeanExporter extends MBeanExporter implements
		BeanFactoryAware, InitializingBean, DisposableBean {
    private static Logger logger = Logger.getLogger(ZoieMBeanExporter.class);
    
	@Override
	protected void registerBeans()
	{
	  try 
	  {
	    super.registerBeans();
	  } 
	  catch(Exception ex) {
	    logger.error("Instance already exists, registering JMX bean failed: "+ex.getMessage(),ex);
	  }
    }
	
	
	
	public static void main(String[] args) throws Exception{
		HttpInvokerProxyFactoryBean factoryBean = new HttpInvokerProxyFactoryBean();
		factoryBean.setServiceUrl("http://localhost:8888/services/SearchService");
		factoryBean.setServiceInterface(ZoieSearchService.class);
		factoryBean.afterPropertiesSet();
		
		ZoieSearchService svc = (ZoieSearchService)(factoryBean.getHttpInvokerRequestExecutor());
		
		SearchResult res = svc.search(new SearchRequest());
		
		System.out.println(res.getTotalHits());
	}
}

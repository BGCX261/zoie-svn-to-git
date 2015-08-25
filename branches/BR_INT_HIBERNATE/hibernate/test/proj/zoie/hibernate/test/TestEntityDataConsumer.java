package proj.zoie.hibernate.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.log4j.Logger;

import proj.zoie.api.DataConsumer;
import proj.zoie.api.DataProvider;
import proj.zoie.api.ZoieException;

public class TestEntityDataConsumer implements DataConsumer<File>,DataProvider<File> {

	private static final Logger log = Logger.getLogger(TestEntityDataConsumer.class);
	
	protected static int id = 0;
	
	private DataConsumer<TestEntity> _subConsumer;
	public TestEntityDataConsumer(DataConsumer<TestEntity> subConsumer) {
		_subConsumer = subConsumer;
	}

	public void consume(
			Collection<proj.zoie.api.DataConsumer.DataEvent<File>> data)
			throws ZoieException {
		
		ArrayList<DataEvent<TestEntity>> convertedList = new ArrayList<DataEvent<TestEntity>>(data.size());
		for (DataEvent<File> elem : data){
			try{
				TestEntity entity = convert(elem.getData());
				convertedList.add(new DataEvent<TestEntity>(elem.getVersion(),entity));
			}
			catch(Exception ex){
				log.error(ex.getMessage(),ex);
			}
		}
		_subConsumer.consume(convertedList);
	}
	
	private static TestEntity convert(File file) throws IOException{
		String path = file.getAbsolutePath();
		StringBuilder sb=new StringBuilder();
		sb.append(path).append("\n");
		FileReader freader=null;
		try
		{
			freader=new FileReader(file);
			BufferedReader br=new BufferedReader(freader);
			while(true)
			{
				String line=br.readLine();
				if (line!=null){
					sb.append(line).append("\n");
				}
				else
				{
					break;
				}
			}
		}
		catch(Exception e)
		{
			log.error(e);
		}
		finally
		{
			if (freader!=null)
			{
				try {
					freader.close();
				} catch (IOException e) {
					log.error(e);
				}
			}
		}

		String content = sb.toString();
		
		return new TestEntity(id++,content,path);
	}

}

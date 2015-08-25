package proj.zoie.impl.indexing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;

import proj.zoie.api.indexing.Indexable;
import proj.zoie.api.indexing.IndexableInterpreter;

public class FileIndexableInterpreter implements IndexableInterpreter<File> 
{
	private static int id=0;
	private static final Logger log = Logger.getLogger(FileIndexableInterpreter.class);
	
	private class FileIndexable implements Indexable
	{
		private File _file;
		private FileIndexable(File file)
		{
			_file=file;
		}
		public Document[] buildDocuments() 
		{
			Document doc=new Document();
			StringBuilder sb=new StringBuilder();
			sb.append(_file.getAbsoluteFile()).append("\n");
			doc.add(new Field("path",_file.getAbsolutePath(),Store.YES,Index.ANALYZED));
			FileReader freader=null;
			try
			{
				freader=new FileReader(_file);
				BufferedReader br=new BufferedReader(freader);
				while(true)
				{
					String line=br.readLine();
					if (line!=null){
						sb.append(br.readLine()).append("\n");
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

			doc.add(new Field("content",sb.toString(),Store.YES,Index.ANALYZED));
			return new Document[]{doc};
		}
		
		public boolean isSkip()
		{
			return false;
		}
		
		public boolean isDeleted()
		{
			return false;
		}

		public int getUID() 
		{
			return id;
		}
	}
	
	public Indexable interpret(File src) 
	{
		Indexable idxable = new FileIndexable(src);
		id++;
		return idxable;
	}
}

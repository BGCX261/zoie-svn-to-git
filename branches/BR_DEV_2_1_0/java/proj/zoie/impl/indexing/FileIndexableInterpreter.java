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

import proj.zoie.api.indexing.AbstractZoieIndexable;
import proj.zoie.api.indexing.ZoieIndexable;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;

public class FileIndexableInterpreter implements ZoieIndexableInterpreter<File> 
{
	protected static int id = 0;
	private static final Logger log = Logger.getLogger(FileIndexableInterpreter.class);
	
	protected class FileIndexable extends AbstractZoieIndexable
	{
		private File _file;
		private long _uid;
		private FileIndexable(File file, long uid)
		{
			_file=file;
			_uid = uid;
		}
		
		public Document[] buildDocuments(){
			return new Document[]{buildDocument()};
		}
		
		public IndexingReq[] buildIndexingReqs(){
			IndexingReq req = new IndexingReq(buildDocument(),null);
			return new IndexingReq[]{req};
		}
		
		public Document buildDocument() 
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

			doc.add(new Field("content",sb.toString(),Store.YES,Index.ANALYZED));
			return doc;
		}
		
		public boolean isSkip()
		{
			return false;
		}
		
		public boolean isDeleted()
		{
			return false;
		}

		public long getUID() 
		{
			return _uid;
		}
	}
	
	public ZoieIndexable convertAndInterpret(File src) {
		ZoieIndexable idxable = new FileIndexable(src, id);
		id++;
		return idxable;
	}
}

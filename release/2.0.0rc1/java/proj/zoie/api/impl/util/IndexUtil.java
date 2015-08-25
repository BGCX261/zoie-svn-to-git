package proj.zoie.api.impl.util;

import java.io.IOException;

import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;

public class IndexUtil {
	private IndexUtil()
	{	
	}
	
	public static int getNumSegments(Directory idx) throws IOException
	{
		SegmentInfos infos=new SegmentInfos();
		infos.read(idx);
		return infos.size();
	}
}

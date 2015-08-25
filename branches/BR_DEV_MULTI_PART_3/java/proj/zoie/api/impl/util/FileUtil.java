package proj.zoie.api.impl.util;

import java.io.File;
import java.io.IOException;

public class FileUtil {
	/**
	   * utility method to delete a directory
	   * @param dir
	   * @throws IOException
	   */
	  private static void deleteDir(File dir) throws IOException
	  {
	    if (dir == null) return;
	    
	    if (dir.isDirectory())
	    {
	      File[] files=dir.listFiles();
	      for (File file : files)
	      {
	        deleteDir(file);
	      }
          if (!dir.delete())
          {
            throw new IOException("cannot remove directory: "+dir.getAbsolutePath());
          }
        }
        else
        {
          if (!dir.delete())
          {
            throw new IOException("cannot delete file: "+dir.getAbsolutePath());
          }
        }
	  }

	  /**
	   * Purges an index
	   */
	  public static void rmDir(File location) throws IOException
	  {
	    String name=location.getName()+"-"+System.currentTimeMillis();
	    File parent=location.getParentFile();
	    File tobeDeleted=new File(parent,name);
	    location.renameTo(tobeDeleted);
	    // try to delete the files, ok if it fails, this is just for testing
	    deleteDir(tobeDeleted);
	  }
}

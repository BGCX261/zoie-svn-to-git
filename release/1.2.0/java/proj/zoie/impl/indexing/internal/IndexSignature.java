package proj.zoie.impl.indexing.internal;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.log4j.Logger;

public class IndexSignature {
	private static Logger log = Logger.getLogger(IndexSignature.class);
	
	private final String indexPath;         // index directory
    private long   _version;                     // current SCN

    public IndexSignature(String idxPath, long version)
    {
      indexPath = idxPath;
      _version = version;
    }

    public void updateVersion(long version)
    {
      _version = version;
    }

    public long getVersion()
    {
      return _version;
    }

    public String getIndexPath()
    {
      return indexPath;
    }

    public void save(File file)
        throws IOException
    {
      StringBuilder builder = new StringBuilder();
      builder.append(indexPath).append('@').append(_version);

      if (!file.exists())
      {
        file.createNewFile();
      }
      FileOutputStream fout = null;
      try
      {
        fout = new FileOutputStream(file);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fout, "UTF-8"));
        writer.write(builder.toString());
        writer.flush();
      }
      finally
      {
        if (fout != null)
        {
          try
          {
            fout.close();
          }
          catch (IOException e)
          {
        	log.warn("Problem closing index directory file: " + e.getMessage());
          }
        }
      }
    }

    public static IndexSignature read(File file)
    {
      if (file.exists())
      {
        FileInputStream fin = null;
        String line;
        try
        {
          fin = new FileInputStream(file);
          BufferedReader reader = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
          line = reader.readLine();
        }
        catch (IOException ioe)
        {
          log.error("Problem reading index directory file.", ioe);
          return null;
        }
        finally
        {
          if (fin != null)
          {
            try
            {
              fin.close();
            }
            catch (IOException e)
            {
              log.warn("Problem closing index directory file: " + e.getMessage());
            }
          }
        }

        if (line != null)
        {
          String[] parts = line.split("@");
          String idxPath = parts[0];
          long version = 0L;

          try
          {
            version = Long.parseLong(parts[1]);
          }
          catch (Exception e)
          {
            log.warn(e.getMessage());
            version = 0L;
          }

          IndexSignature sig = new IndexSignature(idxPath, version);
          return sig;
        }
        else
        {
          return null;
        }
      }
      else
      {
        log.info("Starting with empty search index: maxSCN file not found");
        return null;
      }
    }
}

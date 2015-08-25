package proj.zoie.impl.indexing.internal;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
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

    public void save(File file) throws IOException
    {
      if (!file.exists())
      {
        file.createNewFile();
      }
      FileOutputStream fout = null;
      try
      {
        fout = new FileOutputStream(file);
        save(fout);
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
    
    public void save(OutputStream out) throws IOException
    {
      StringBuilder builder = new StringBuilder();
      builder.append(indexPath).append('@').append(_version);

      BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"));
      writer.write(builder.toString());
      writer.flush();
    }

    public static IndexSignature read(File file)
    {
      if (file.exists())
      {
        FileInputStream fin = null;
        try
        {
          fin = new FileInputStream(file);
          return read(fin);
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
      }
      else
      {
        log.info("Starting with empty search index: maxSCN file not found");
        return null;
      }
    }
    
    public static IndexSignature read(InputStream in) throws IOException
    {
      BufferedReader reader = new BufferedReader(new InputStreamReader(in, "UTF-8"));
      String line = reader.readLine();
      
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
        
        return new IndexSignature(idxPath, version);
      }
      else
      {
        return null;
      }
    }
}

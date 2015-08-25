package proj.zoie.impl.indexing.internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;

import proj.zoie.api.DataConsumer;
import proj.zoie.api.ZoieException;
import proj.zoie.api.indexing.AbstractZoieIndexable;
import proj.zoie.api.indexing.Indexable;
import proj.zoie.api.indexing.IndexableInterpreter;
import proj.zoie.api.indexing.ZoieIndexable;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;

public class DelegateIndexDataConsumer<V> implements DataConsumer<V> {
	private static final Logger log = Logger.getLogger(DelegateIndexDataConsumer.class);
	private final DataConsumer<ZoieIndexable> _diskConsumer;
	private final DataConsumer<ZoieIndexable> _ramConsumer;
	private final IndexableInterpreter<V> _interpreter;
	
	private static class ZoieIndexableDecorator extends AbstractZoieIndexable{
		private final Indexable _inner;
		private ZoieIndexableDecorator(Indexable inner){
			_inner = inner;
		}
		
		public static ZoieIndexableDecorator decorate(Indexable inner)
		{
		  return (inner == null ? null : new ZoieIndexableDecorator(inner));
		}

		public Document[] buildDocuments() {
			return _inner.buildDocuments();
		}

		public int getUID() {
			return _inner.getUID();
		}

		public boolean isDeleted() {
			return _inner.isDeleted();
		}

		public boolean isSkip() {
			return _inner.isSkip();
		}
		
	}
	
	public DelegateIndexDataConsumer(DataConsumer<ZoieIndexable> diskConsumer,DataConsumer<ZoieIndexable> ramConsumer,IndexableInterpreter<V> interpreter)
	{
	  	_diskConsumer=diskConsumer;
	  	_ramConsumer=ramConsumer;
	  	_interpreter=interpreter;
	}
	
	public void consume(Collection<DataEvent<V>> data)
			throws ZoieException {
		if (data!=null)
		{
		  ArrayList<DataEvent<ZoieIndexable>> indexableList=new ArrayList<DataEvent<ZoieIndexable>>(data.size());
		  Iterator<DataEvent<V>> iter=data.iterator();
		  while(iter.hasNext())
		  {
			  try{
			    DataEvent<V> event=iter.next();
			    ZoieIndexable indexable = null;
			    if (_interpreter instanceof ZoieIndexableInterpreter){
			    	indexable = ((ZoieIndexableInterpreter<V>)_interpreter).convertAndInterpret(event.getData());
			    }
			    else{
			    	indexable = ZoieIndexableDecorator.decorate(_interpreter.interpret(event.getData()));
			    }
			    DataEvent<ZoieIndexable> newEvent=new DataEvent<ZoieIndexable>(event.getVersion(),indexable);
			    indexableList.add(newEvent);
			  }
			  catch(Exception e){
				log.error(e.getMessage(),e);
			  }
		  }
		  
		  if(_diskConsumer != null)
		  {
		    synchronized(_diskConsumer) // this blocks the batch disk loader thread while indexing to RAM
		    {
	          if (_ramConsumer != null)
	          {
	            ArrayList<DataEvent<ZoieIndexable>> ramList=new ArrayList<DataEvent<ZoieIndexable>>(indexableList);
	            _ramConsumer.consume(ramList);
	          }
	          _diskConsumer.consume(indexableList);
		    }
		  }
		  else
		  {
		    if (_ramConsumer != null)
		    {
			  _ramConsumer.consume(indexableList);
		    }
		  }
		}
	}
}

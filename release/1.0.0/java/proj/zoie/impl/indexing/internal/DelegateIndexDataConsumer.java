package proj.zoie.impl.indexing.internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import proj.zoie.api.DataConsumer;
import proj.zoie.api.ZoieException;
import proj.zoie.api.indexing.Indexable;
import proj.zoie.api.indexing.IndexableInterpreter;

public class DelegateIndexDataConsumer<V> implements DataConsumer<V> {
	private final DataConsumer<Indexable> _diskConsumer;
	private final DataConsumer<Indexable> _ramConsumer;
	private final IndexableInterpreter<V> _interpreter;
	
	public DelegateIndexDataConsumer(DataConsumer<Indexable> diskConsumer,DataConsumer<Indexable> ramConsumer,IndexableInterpreter<V> interpreter)
	{
	  	_diskConsumer=diskConsumer;
	  	_ramConsumer=ramConsumer;
	  	_interpreter=interpreter;
	}
	
	public void consume(Collection<DataEvent<V>> data)
			throws ZoieException {
		if (data!=null)
		{
		  ArrayList<DataEvent<Indexable>> indexableList=new ArrayList<DataEvent<Indexable>>(data.size());
		  Iterator<DataEvent<V>> iter=data.iterator();
		  while(iter.hasNext())
		  {
			  DataEvent<V> event=iter.next();
			  Indexable indexable = _interpreter.interpret(event.getData());
			  DataEvent<Indexable> newEvent=new DataEvent<Indexable>(event.getVersion(),indexable);
			  indexableList.add(newEvent);
		  }
		  if (_ramConsumer != null)
		  {
			  ArrayList<DataEvent<Indexable>> ramList=new ArrayList<DataEvent<Indexable>>(indexableList);
			  _ramConsumer.consume(ramList);
		  }
		  if (_diskConsumer != null)
		  {
			  _diskConsumer.consume(indexableList);
		  }
		}
	}
}

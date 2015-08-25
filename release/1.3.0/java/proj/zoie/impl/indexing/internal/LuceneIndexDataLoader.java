package proj.zoie.impl.indexing.internal;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectRBTreeMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Similarity;

import proj.zoie.api.DataConsumer;
import proj.zoie.api.ZoieException;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.impl.util.IntSetAccelerator;
import proj.zoie.api.indexing.Indexable;

public abstract class LuceneIndexDataLoader<R extends IndexReader> implements DataConsumer<Indexable> {
	private static final Logger log = Logger.getLogger(LuceneIndexDataLoader.class);
	protected final Analyzer _analyzer;
	protected final Similarity _similarity;
	protected final SearchIndexManager<R> _idxMgr;

	protected LuceneIndexDataLoader(Analyzer analyzer, Similarity similarity,SearchIndexManager<R> idxMgr) {
		_analyzer = analyzer;
		_similarity = similarity;
		_idxMgr=idxMgr;
	}

	protected abstract BaseSearchIndex getSearchIndex();

	public void consume(Collection<DataEvent<Indexable>> events) throws ZoieException {
		int eventCount = events.size();
        if (events == null || eventCount == 0)
			return;

		BaseSearchIndex idx = getSearchIndex();

		Int2ObjectMap<List<Document>> addList = new Int2ObjectRBTreeMap<List<Document>>();
		long version = idx.getVersion();		// current version

		IntSet delSet =new IntOpenHashSet();

		// sort events by version number
		PriorityQueue<DataEvent<Indexable>> queue = new PriorityQueue<DataEvent<Indexable>>(eventCount, DataEvent.getComparator());
		for(DataEvent<Indexable> e : events)
		{
		  if(e != null) queue.offer(e);
		}
		events.clear(); // clear the event collection, so that GC can reclaim memory from indexed events
		events = null;
		
		DataEvent<Indexable> evt = null;
		try {
			while ((evt = queue.poll()) != null) {
				version = Math.max(version, evt.getVersion());
				// interpret and get get the indexable instance
				Indexable indexable = evt.getData();
				if (indexable == null || indexable.isSkip())
					continue;

				int uid = indexable.getUID();
				delSet.add(uid);
				addList.remove(uid);
				if (!indexable.isDeleted()) // update event
				{
					// build the Lucene document
					Document[] docs = indexable.buildDocuments();
					for (Document doc : docs) {
						if (doc != null) // if doc is provided, interpret as
											// a delete, e.g. update with
											// nothing
						{
							ZoieIndexReader.fillDocumentID(doc, uid);

							// add to the insert list
							List<Document> docList = addList.get(uid);
							if (docList == null) {
								docList = new LinkedList<Document>();
								addList.put(uid, docList);
							}
							docList.add(doc);
						}
					}
				} else {
					addList.remove(uid);
				}
			}

			List<Document> docList = new LinkedList<Document>();
			for (List<Document> tmpList : addList.values()) {
				docList.addAll(tmpList);
			}
			idx.updateIndex(new IntSetAccelerator(delSet), docList, _analyzer,_similarity);
		} catch (IOException ioe) {
			log.error("Problem indexing batch: " + ioe.getMessage(), ioe);
		} finally {
			try {
				if (idx != null) {
					idx.incrementEventCount(eventCount);
					idx.setVersion(version); // update the version of the
												// index
				}
			} catch (Exception e) // catch all exceptions, or it would screw
									// up jobs framework
			{
				log.warn(e.getMessage());
			} finally {
				if (idx instanceof DiskSearchIndex) {
					log.info("disk indexing requests flushed.");
				}
			}
		}
	}
	
    public void loadFromIndex(BaseSearchIndex index) throws ZoieException
    {
      try
      {
        BaseSearchIndex idx = getSearchIndex();
        idx.loadFromIndex(index);
      }
      catch(IOException ioe)
      {
        log.error("Problem copying segments: " + ioe.getMessage(), ioe);
      }
    }
}

package relop;

import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.BucketScan;
import index.HashIndex;


/**
 * Wrapper for bucket scan, an index access method.
 */
public class IndexScan extends Iterator {
	
	protected HeapFile heapFile;
	protected HashIndex hashIndex;
	private Schema schema1;
	private boolean open;
	private BucketScan bucketScan;
  /**
   * Constructs an index scan, given the hash index and schema.
   */
  public IndexScan(Schema schema, HashIndex index, HeapFile file) {
	  this.setSchema(schema);
	  this.schema1 = schema;
	  this.hashIndex = index;
	  this.heapFile = file;
	  this.bucketScan = hashIndex.openScan();
	  open = true;
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
	  bucketScan.close();
	  bucketScan = hashIndex.openScan();
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
	  return open;
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
	  bucketScan.close();
	  open = false;
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
	  if (open){
		  return bucketScan.hasNext();
	  }
	  return false;
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
    Tuple tuple = new Tuple(schema1);
    if (open){
    	RID rid = bucketScan.getNext();
    	byte[] data = heapFile.selectRecord(rid);
    	if (data != null){
    		tuple = new Tuple(schema1, data);
    	}else{
    		throw new IllegalStateException("No more tuples....");
    	}
    }
    return tuple;
  }

  /**
   * Gets the key of the last tuple returned.
   */
  public SearchKey getLastKey() {
	  return bucketScan.getLastKey();
  }

  /**
   * Returns the hash value for the bucket containing the next tuple, or maximum
   * number of buckets if none.
   */
  public int getNextHash() {
	  return bucketScan.getNextHash();
  }

} // public class IndexScan extends Iterator

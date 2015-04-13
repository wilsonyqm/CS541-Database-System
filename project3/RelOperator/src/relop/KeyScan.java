package relop;

import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.*;

/**
 * Wrapper for hash scan, an index access method.
 */

public class KeyScan extends Iterator {
	private HashScan hashScan;
	private SearchKey searchkey;
	protected HeapFile heapFile;
	private HashIndex hashIndex;
	private Schema schema1;
	private boolean open;
  /**
   * Constructs an index scan, given the hash index and schema.
   */
  public KeyScan(Schema schema, HashIndex index, SearchKey key, HeapFile file) {
	  this.setSchema(schema);
	  this.searchkey = key;
	  this.heapFile = file;
	  this.hashIndex = index;
	  this.schema1 = schema;
	  hashScan  = hashIndex.openScan(searchkey);
	  open = true;
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
	  System.out.println("keyscan");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
	  if (open){
		  hashScan.close();
		  open = false;
		  hashScan = hashIndex.openScan(searchkey);
		  open = true;
	  }
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
	  hashScan.close();
	  open = false;
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
	  return hashScan.hasNext();
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
	 Tuple tuple = new Tuple(schema1);
	 if(open){
		 RID rid = hashScan.getNext();
		 byte[] data = heapFile.selectRecord(rid);
		 if (data != null){
			 tuple = new Tuple(schema1, data);
		 }else{
			 throw new IllegalStateException("No more tuples....");
		 }
	 }
	 return tuple;
  }

} // public class KeyScan extends Iterator

package relop;

import global.RID;
import heap.HeapFile;
import heap.HeapScan;

/**
 * Wrapper for heap file scan, the most basic access method. This "iterator"
 * version takes schema into consideration and generates real tuples.
 */
public class FileScan extends Iterator {
	private HeapScan heapScan;
	protected HeapFile hpFile;
	private Schema schema1;
	private boolean open;

	private RID rid;
  /**
   * Constructs a file scan, given the schema and heap file.
   */
  public FileScan(Schema schema, HeapFile file) {
    //throw new UnsupportedOperationException("Not implemented")
	this.heapScan = file.openScan();
	this.hpFile = file;
	this.setSchema(schema);
	this.schema1 = schema;
	open = true;
  }

  /**
   * Gives a one-line explanation of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
    //throw new UnsupportedOperationException("Not implemented");
	  System.out.println("heap scan");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
	  heapScan.close();
	  heapScan = hpFile.openScan();
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
    //throw new UnsupportedOperationException("Not implemented");
	return open;
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
	if (open){
		heapScan.close();
		open = false;
	}else{
		throw new UnsupportedOperationException("Not implemented");
	}
	
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
	if (open){
		return heapScan.hasNext();
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
	if(open){
		rid = new RID();
		byte[] data = heapScan.getNext(rid);
		tuple = new Tuple(schema1, data);
	}
	return tuple;
  }

  /**
   * Gets the RID of the last tuple returned.
   */
  public RID getLastRID() {
	return rid;
  }

} // public class FileScan extends Iterator

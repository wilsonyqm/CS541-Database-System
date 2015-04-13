package relop;

import global.SearchKey;
import heap.HeapFile;
import index.BucketScan;
import index.HashIndex;

/**
 * The simplest of all join algorithms: nested loops (see textbook, 3rd edition,
 * section 14.4.1, page 454).
 */
public class HashJoin extends Iterator {

	
	//private Schema 	schema;
	
	private boolean startJoin = true;
	Tuple leftTuple;
	
	// boolean variable to indicate whether the pre-fetched tuple is consumed or not
	private boolean nextTupleIsConsumed;
	
	// pre-fetched tuple
	private Tuple nextTuple; 
	
	//left column and right column
	private int left_col;
	private int right_col;
		
	private HashTableDup hashtable = new HashTableDup();
	private IndexScan left;
	private IndexScan right;
	
	private int position;
	private int hashvalue;
	
	private Tuple[] matchingTuples;
	private Tuple currentTuple;
	/**
	 * Constructs a join, given the left and right iterators and join predicates
	 * (relative to the combined schema).
	 */
	public HashJoin(Iterator left, Iterator right, int left_col, int right_col) {
		
		this.schema = Schema.join(left.schema, right.schema);		
		this.left_col = left_col;
		this.right_col = right_col;
		this.left = makeIndexScan(left,left_col);
		this.right = makeIndexScan(right,right_col);
		nextTupleIsConsumed = true;
		
	}
	
	/*
	 * Create indexscan of each iterator
	 * */
	public IndexScan makeIndexScan(Iterator iter, int index){
		if(iter instanceof IndexScan){
			return (IndexScan) iter;
		}else{
			
			if (iter instanceof FileScan){
				FileScan scan = (FileScan) iter;
				HeapFile file = scan.hpFile;
				HashIndex hashindex = new HashIndex(null);
				
				while(scan.hasNext()){
					hashindex.insertEntry(new SearchKey(scan.getNext().getField(index)), scan.getLastRID());
				}
				return new IndexScan(scan.schema, hashindex, file);
			}else{
				HeapFile file = new HeapFile(null);
				while(iter.hasNext()){
					file.insertRecord(iter.getNext().getData());
				}
				FileScan scan = new FileScan(iter.schema,file);
				
				HashIndex hashindex = new HashIndex(null);
				while(scan.hasNext()){
					hashindex.insertEntry(new SearchKey(scan.getNext().getField(index)), scan.getLastRID());
				}
				return new IndexScan(scan.schema, hashindex, file);
			}
		}
	}

	/**
	 * Gives a one-line explanation of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	public void explain(int depth) {		
		System.out.println("Hash Join");
		left.explain(depth+1);
		right.explain(depth+1);
	}
	
	/**
	 * Restarts the iterator, i.e. as if it were just constructed.
	 */
	public void restart() {
		left.restart();
		right.restart();
		startJoin = true;
		nextTupleIsConsumed = true;
	}

	/**
	 * Returns true if the iterator is open; false otherwise.
	 */
	public boolean isOpen() {
		return left.isOpen() & right.isOpen();
	}

	/**
	 * Closes the iterator, releasing any resources (i.e. pinned pages).
	 */
	public void close() {
		left.close();
		right.close();
	}

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException if no more tuples
	 */
	public Tuple getNext() {		
		nextTupleIsConsumed = true;
		return nextTuple;
	}

	/*
	 * create temp hashtable for out relation
	 */
	public HashTableDup CreateMemoryHashTable(int hashvalue){
		HashTableDup table = new HashTableDup();
		left.restart();
		while (left.hasNext() && left.getNextHash() != hashvalue){
			left.getNext();
		}
		
		while(left.hasNext() && left.getNextHash() == hashvalue){
			Tuple tuple = left.getNext();
			SearchKey key = new SearchKey(tuple.getField(left_col));
			table.add(key,tuple);			
		}
		return table;
	}
	
	/**
	 * Returns true if there are more tuples, false otherwise.
	 * 
	 */
	
	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
				
		if (!nextTupleIsConsumed)
			return true;
		
		if (!right.hasNext())
			return false;
		
		if (startJoin) {
			startJoin = false;
			left.restart();
			right.restart();
			hashvalue = right.getNextHash();
			position = 0;
			matchingTuples = null;
			hashtable = CreateMemoryHashTable(hashvalue);
		}

		
		while (true) {

			while (right.getNextHash() == hashvalue) {
								
				if (matchingTuples == null){
					currentTuple = right.getNext();
					SearchKey key = new SearchKey(currentTuple.getField(right_col));
					matchingTuples = hashtable.getAll(key);
					position = 0;
				}
				
				if (matchingTuples != null){
					while (position < matchingTuples.length){
							
						if (matchingTuples[position].getField(left_col).equals(currentTuple.getField(right_col))){
							nextTuple = Tuple.join(matchingTuples[position], currentTuple, schema);
							position++;
							nextTupleIsConsumed = false;
							return true;
						}else{
							position++;
						}	
					}
					
					matchingTuples = null;
				}
			}
			
			//finish the bucket
			if (right.hasNext()) {
				hashvalue = right.getNextHash();
				matchingTuples = null;
				position = 0;
				hashtable = CreateMemoryHashTable(hashvalue);
			}else{
				return false;
			}
		}
	}
	
}


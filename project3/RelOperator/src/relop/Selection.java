package relop;

/**
 * The selection operator specifies which tuples to retain under a condition; in
 * Minibase, this condition is simply a set of independent predicates logically
 * connected by OR operators.
 */
public class Selection extends Iterator {

  /**
   * Constructs a selection, given the underlying iterator and predicates.
   */
	
  private Iterator iterator;
  private Predicate predicate[];
  
  
  private boolean nextTupleIsConsumed;
  private Tuple nextTuple;
  
  public Selection(Iterator iter, Predicate... preds) {
	  this.iterator = iter;
	  this.predicate = preds;
	  
	  this.schema = iterator.getSchema();
	  nextTupleIsConsumed = true; 
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
	  System.out.println("selection");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
	  iterator.restart();
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
	  return iterator.isOpen();
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
	  iterator.close();
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
	  if (!nextTupleIsConsumed)
			return true;
		
	while (true) {

		while (iterator.hasNext()) {
				// try to match
			nextTuple = iterator.getNext();
			for (int i = 0; i < predicate.length; i++)
				if (predicate[i].evaluate(nextTuple)) {
					nextTupleIsConsumed = false;
					return true;	
			}
		}
		return false;
	}
	 
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

} // public class Selection extends Iterator

package relop;

/**
 * The projection operator extracts columns from a relation; unlike in
 * relational algebra, this operator does NOT eliminate duplicate tuples.
 */
public class Projection extends Iterator {
	
	private Iterator iterator;
	private Integer[] fields;
	/**
   * Constructs a projection, given the underlying iterator and field numbers.
   */
  public Projection(Iterator iter, Integer... fields) {
	  this.iterator = iter;
	  this.fields = fields;
	  this.schema = new Schema(fields.length);
	  
	  for (int i = 0; i < fields.length;i++){
		  int fieldNumber = fields[i];
		  int type = iter.schema.fieldType(fieldNumber);
		  int length = iter.schema.fieldLength(fieldNumber);
		  String name = iter.schema.fieldName(fieldNumber);
		  schema.initField(i, type, length, name);;
	  }
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
	  System.out.println("Projection");
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
	  return iterator.hasNext();
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
	  Tuple projectTuple = new Tuple(schema); 
	  if (iterator.hasNext()){
		  Tuple nextTuple = iterator.getNext();
		  for (int i = 0; i < fields.length;i++){
			  int fieldNumber = fields[i];
			  projectTuple.setField(i,nextTuple.getField(fieldNumber));			  
		  }
	  }
	  return projectTuple;
  }

} // public class Projection extends Iterator

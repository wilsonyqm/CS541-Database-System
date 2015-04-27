package query;

import index.HashIndex;
import global.Minibase;
import global.RID;
import global.SearchKey;
import heap.HeapFile;
import parser.AST_Insert;
import relop.Schema;
import relop.Tuple;

/**
 * Execution plan for inserting tuples.
 */
class Insert implements Plan {

  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if table doesn't exists or values are invalid
   */

	protected String file;
	protected Object[] values;
	protected Schema schema;
	
  public Insert(AST_Insert tree) throws QueryException {
	file = tree.getFileName();
	values = tree.getValues();
	schema = Minibase.SystemCatalog.getSchema(file);
	QueryCheck.tableExists(file);
	QueryCheck.insertValues(schema, values);
		
  } // public Insert(AST_Insert tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
	  HeapFile f = new HeapFile(file);
	  Tuple tuple = new Tuple(schema,values);
	  RID rid = f.insertRecord(tuple.getData());
	  
	  //update hashindex
	  IndexDesc[] desc = Minibase.SystemCatalog.getIndexes(file);	  
	  for (int i = 0; i < desc.length;i++){
		  String column = desc[i].columnName;
		  String index = desc[i].indexName;
		  HashIndex hash = new HashIndex(index);
		  SearchKey key = new SearchKey(tuple.getField(column));
		  hash.insertEntry(key, rid);
	  }
    // print the output message
    System.out.println("1 row affected.");

  } // public void execute()

} // class Insert implements Plan

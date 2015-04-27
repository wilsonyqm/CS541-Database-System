package query;

import index.HashIndex;
import global.Minibase;
import global.RID;
import global.SearchKey;
import heap.HeapFile;
import parser.AST_Delete;
import relop.FileScan;
import relop.Iterator;
import relop.Predicate;
import relop.Schema;
import relop.Selection;
import relop.Tuple;

/**
 * Execution plan for deleting tuples.
 */
class Delete implements Plan {

  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if table doesn't exist or predicates are invalid
   */
	protected String file;
	protected Predicate[][] predicates;
	protected Schema schema;
	
  public Delete(AST_Delete tree) throws QueryException {
	  file = tree.getFileName();
	  schema = QueryCheck.tableExists(file);
	  predicates = tree.getPredicates();
	  QueryCheck.predicates(schema, predicates);
	  
  } // public Delete(AST_Delete tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
	  HeapFile f = new HeapFile(file);
	  FileScan scan = new FileScan(schema, f);
	  
	  
	  int count = 0;
	  while(scan.hasNext()){
		  
		  Tuple tuple = scan.getNext();
		  RID rid = scan.getLastRID();
		 
		 // boolean inner = true;		  
		  for (int i  = 0; i < predicates.length;i++){
			  boolean ok = false;
			  for (int j = 0; j < predicates[i].length;j++){
				  if (predicates[i][j].evaluate(tuple)){
					  ok = true;
				  }
			  }
			  
			  if (ok == false){
				  break;
			  }
			  
			  
			  if (i == predicates.length -1){
				  count++;
				  
				  f.deleteRecord(rid);
				  IndexDesc[] desc = Minibase.SystemCatalog.getIndexes(file);
				  for (int k = 0; k < desc.length;k++){
					  String col = desc[i].columnName;
					  String index = desc[i].indexName;
					  HashIndex hash = new HashIndex(index);
					  SearchKey key = new SearchKey(tuple.getField(col)); 
					  hash.deleteEntry(key, rid);
				  }
			  }
		  }
		  
	  }
	  
	  
	  System.out.println( count + " rows deleted");
  } // public void execute()

} // class Delete implements Plan

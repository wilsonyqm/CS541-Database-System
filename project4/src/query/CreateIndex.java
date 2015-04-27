package query;


import global.Minibase;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import parser.AST_CreateIndex;
import relop.FileScan;
import relop.Schema;
import relop.Tuple;

/**
 * Execution plan for creating indexes.
 */
class CreateIndex implements Plan {

  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if index already exists or table/column invalid
   */
	protected String file;
	protected String column;
	protected String table;

	protected  HashIndex index;
	protected  Schema schema;
	
  public CreateIndex(AST_CreateIndex tree) throws QueryException {
	  file = tree.getFileName();
	  column = tree.getIxColumn();
	  table = tree.getIxTable(); 	
		
	  QueryCheck.fileNotExists(file);	
	  schema = QueryCheck.tableExists(table);
	  QueryCheck.columnExists(schema,column);
		
		//check if index exist
	  IndexDesc[] index = Minibase.SystemCatalog.getIndexes(table);
	  for (int i = 0 ; i < index.length; i++){
		  QueryCheck.indexExists(index[i].columnName);
	  }
  } // public CreateIndex(AST_CreateIndex tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
	  	index = new HashIndex(file);
		FileScan fs = new FileScan(schema, new HeapFile(table));
		//insert index
		while (fs.hasNext()){
			Tuple tuple = fs.getNext();
			index.insertEntry(new SearchKey(tuple.getField(column)),fs.getLastRID());
		}
		fs.close();
		Minibase.SystemCatalog.createIndex(file, table, column);
		System.out.println("Index " + file + "created on table" + "Column" + column);

  } // public void execute()

} // class CreateIndex implements Plan

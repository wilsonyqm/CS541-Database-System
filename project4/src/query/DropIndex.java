package query;

import global.Minibase;
import index.HashIndex;
import parser.AST_DropIndex;

/**
 * Execution plan for dropping indexes.
 */
class DropIndex implements Plan {

  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if index doesn't exist
   */
	protected String file;
  public DropIndex(AST_DropIndex tree) throws QueryException {
	  	file = tree.getFileName();
		QueryCheck.indexExists(file);
  } // public DropIndex(AST_DropIndex tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
	  new HashIndex(file).deleteFile();
	    // add the schema to the catalog
	  Minibase.SystemCatalog.dropIndex(file);
	  System.out.println("index dropped");

  } // public void execute()

} // class DropIndex implements Plan

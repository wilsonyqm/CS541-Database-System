package query;



import java.util.ArrayList;

import global.Minibase;
import heap.HeapFile;
import parser.AST_Select;
import relop.FileScan;
import relop.Iterator;
import relop.Predicate;
import relop.Projection;
import relop.Schema;
import relop.Selection;
import relop.SimpleJoin;
import relop.Tuple;

/**
 * Execution plan for selecting tuples.
 */
class Select implements Plan {

  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if validation fails
   */
	protected String[] tables;
	protected String[] columns;
	protected Predicate[][] predicates;
	protected Schema[] schema;
	protected FileScan[] filescan;
	protected boolean[] visited;
	protected Iterator[] tablescan;
  public Select(AST_Select tree) throws QueryException {
	  tables = tree.getTables();
	  columns = tree.getColumns();
	  predicates = tree.getPredicates();
	  schema = new Schema[tables.length];
	  filescan = new FileScan[tables.length];
	  tablescan= new Iterator[tables.length];
	  visited=new boolean[predicates.length];
	  Schema all=new Schema(0); 
	  for (int i = 0; i < tables.length;i++){
		  String table = tables[i];
		  schema[i] = Minibase.SystemCatalog.getSchema(table);
		  all=Schema.join(all, schema[i]);
		  QueryCheck.tableExists(table);		  
		  filescan[i] = new FileScan(schema[i],new HeapFile(table));
	  }
	  
	  for (int i =0; i < columns.length;i++){
		  QueryCheck.columnExists(all, columns[i]);
	  }
	  
	  for (int i =0; i < predicates.length;i++){
		  QueryCheck.predicates(all, predicates);
	  }
	  for (int i=0;i<tables.length;i++){
		  tablescan[i]=new FileScan(schema[i],new HeapFile(tables[i]));
	  }
	  
	
  } // public Select(AST_Select tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
	 // ArrayList<Predicate> exist=new ArrayList<Predicate>();
	  
	  //selection first
	  
	  @SuppressWarnings("unchecked")
	ArrayList<Predicate[]>[] selarr=new ArrayList[tables.length];
	  for (int k = 0; k < filescan.length;k++){
		 // boolean empty=true;
		  selarr[k]=new ArrayList<Predicate[]>();
		  for (int i = 0; i < predicates.length;i++){
			  boolean ok=true;
			  for (int j = 0; j < predicates[i].length;j++){
				  Predicate predicate = predicates[i][j];
				  ok= ok & predicate.validate(schema[k]);
				  if(!ok) break;
//				  if (!exist.contains(predicate) && predicate.validate(currschema) ){
//					  //empty=false;
//					  
//					  currscan=new Selection(currscan,predicate);
	//				  while(currscan.hasNext()){
	//					  Tuple tuple = currscan.getNext();
	//					  if(predicate.evaluate(tuple))
	//						  hf.insertRecord(tuple.getData());
	//				  }
					  //exist.add(predicates[i][j]);
			  }
			  if(ok){
				  visited[i]=true;
				  selarr[k].add(predicates[i]);
			  }
		  }		
	  }
//		  currschema= Schema.join(currschema, schema[k]);
//		  currscan=new SimpleJoin(currscan,filescan[k],null);
	  for(int k=0; k<tables.length;k++){
		  if(selarr[k].size()>0){
			  Predicate[][] cur_pred=new Predicate[selarr[k].size()][];
			  for(int i=0;i<selarr[k].size();i++){
				  cur_pred[i]=selarr[k].get(i);
				  tablescan[k]=new Selection(tablescan[k],cur_pred[i]);
			  }
		  }
	  }
	  
	  Schema curr=schema[0];
	  Iterator iter=tablescan[0];
	  for(int i=1;i<tablescan.length;i++){
		  Schema next= Schema.join(curr, schema[i]);
		  ArrayList<Predicate[]> joinarr=new ArrayList<Predicate[]>();
		  for(int j=0;j<predicates.length;j++){
			  if(!visited[j]){
				  boolean matched = true;
				  for(Predicate pred:predicates[j]){
					  matched &=pred.validate(next);
				  }
				  if(matched){
					  joinarr.add(predicates[j]);
					  visited[j]=true;
				  }
			  }
		  }
		  Predicate[][] join_pred=new Predicate[joinarr.size()][];
		  for(int j=0;j<join_pred.length;j++){
			  join_pred[j]=joinarr.get(j);
			  iter=new SimpleJoin(iter,tablescan[i],join_pred[j]);
		  }
		  curr=next;
		  
	  }
	  //Simple Join
	  if(columns.length>0){
		  Integer[] fields=new Integer[columns.length];
		  for(int n=0;n<columns.length;n++){
			  fields[n]=curr.fieldNumber(columns[n]);
		  }
		  iter=new Projection(iter,fields);
	  }
	  iter.execute();
	  iter.close();
    // print the output message
   // System.out.println("0 rows affected. (Not implemented)");

  } // public void execute()

} // class Select implements Plank
package tests;
import global.AttrOperator;
import global.AttrType;
import heap.*;

import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import relop.*;
// YOUR CODE FOR PART3 SHOULD GO HERE.

public class QEPTest extends TestDriver {
	private static Schema s_dep,s_emp;
	public static void main(String args[]) throws IOException{
		try{
			QEPTest test=new QEPTest();
			test.create_minibase();
			test.run();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	protected void run() throws IOException{
		BufferedReader br=new BufferedReader(new FileReader("src/tests/SampleData/Department.txt"));	
		HeapFile dep=new HeapFile(null);
		HeapFile emp=new HeapFile(null);

		try{
			String line=br.readLine();
			s_emp=new Schema(5);
			
			String[] words=line.split(", ");
			//construct the table Department
			
			s_emp.initField(0, AttrType.INTEGER, 4, words[0]);
			s_emp.initField(1, AttrType.STRING, 20, words[1]);
			s_emp.initField(2, AttrType.INTEGER, 4, words[2]);
			s_emp.initField(3, AttrType.INTEGER, 12, words[3]);
			s_emp.initField(4, AttrType.INTEGER, 4, words[4]);
			initCounts();
			saveCounts(null);
			
			Tuple tuple=new Tuple(s_emp);
			//insert table Department;
			line=br.readLine();
			while(line!=null){
				words=line.split(", ");
				for(int i=0;i<5;i++){
					words[i].trim();
					//System.out.println(i+" "+words[i]);
					
					Pattern pattern=Pattern.compile("[a-zA-Z]+");
					Matcher matcher=pattern.matcher(words[i]);
					if(matcher.find())
						tuple.setField(i, words[i]);
					else{
						int temp=Integer.parseInt(words[i]);
						tuple.setField(i, temp);
					}

				}
				tuple.insertIntoFile(emp);
				//tuple.print();
				//System.out.println();
				line=br.readLine();
			}
			saveCounts("Department");
		}catch(Exception ex){
			ex.printStackTrace();
			br.close();
		}
		
		//insert table Employee
		
		BufferedReader br1=new BufferedReader(new FileReader("src/tests/SampleData/Employee.txt"));
		try{
			String line1=br1.readLine();
			String[] words1=line1.split(", ");
			s_dep=new Schema(4);
			
			s_dep.initField(0, AttrType.INTEGER, 4, words1[0]);
			s_dep.initField(1, AttrType.STRING, 20, words1[1]);
			s_dep.initField(2, AttrType.INTEGER, 12, words1[2]);
			s_dep.initField(3, AttrType.INTEGER, 12, words1[3]);
			initCounts();
			saveCounts(null);
			Tuple tuple=new Tuple(s_dep);
			line1=br1.readLine();
			while(line1!=null){
				words1=line1.split(", ");
				for(int i=0;i<4;i++){
					words1[i].trim();
					//System.out.println(i+" HERE "+words1[i]);
					Pattern pattern=Pattern.compile("[a-zA-Z]+");
					Matcher matcher=pattern.matcher(words1[i]);
					if(matcher.find())
						tuple.setField(i, words1[i]);
					else{
						//System.out.println("INT");
						int temp=Integer.parseInt(words1[i]);
						tuple.setField(i, temp);
					}
				}
				tuple.insertIntoFile(dep);
				//tuple.print();
				//System.out.println();
				line1=br1.readLine();
			}
			saveCounts("Employee");
		}catch(Exception ex1){
			ex1.printStackTrace();
			br1.close();
		}
		
		//end of table insertion
		//1.Display for each employee his Name and Salary
		saveCounts("null");
		System.out.println("Display for each employee his Name and Salary");
		FileScan scan_emp=new FileScan(s_emp,emp);
		Projection pro=new Projection(scan_emp,1,3);
		pro.execute();
		saveCounts("Query 1");
		
		//Display the Name for the departments with MinSalary = 1000
		saveCounts("null");
		System.out.println("Display the Name for the departments with MinSalary = 1000");
		FileScan scan_dep =new FileScan(s_dep,dep);
		Predicate pred=new Predicate(AttrOperator.EQ,AttrType.FIELDNO,2,AttrType.INTEGER,1000);
		Selection sel=new Selection(scan_dep,pred);
		pro=new Projection(sel,1);
		pro.execute();
		saveCounts("Query 2");
		
		//Display the Name for the departments with MinSalary = MaxSalary
		saveCounts("null");
		System.out.println("Display the Name for the departments with MinSalary = MaxSalary");
		scan_dep=new FileScan(s_dep,dep);
		pred=new Predicate(AttrOperator.EQ,AttrType.FIELDNO,2,AttrType.FIELDNO,3);
		sel=new Selection(scan_dep,pred);
		pro=new Projection(sel,1);
		pro.execute();
		saveCounts("Query 3");
		
		//Display the Name for employees whose Age > 30 and Salary < 1000
		saveCounts("null");
		System.out.println("Display the Name for employees whose Age > 30 and Salary < 1000");
		scan_emp=new FileScan(s_emp,emp);
		Predicate[] preds=new Predicate[]{new Predicate(AttrOperator.GT,AttrType.FIELDNO,2,AttrType.INTEGER,30),
						  				  new Predicate(AttrOperator.LT,AttrType.FIELDNO,3,AttrType.INTEGER,1000)};
		sel=new Selection(scan_emp,preds[0]);
		sel=new Selection(sel,preds[1]);
		pro=new Projection(sel,1);
		pro.execute();
		saveCounts("Query 4");
		
		//For each employee, display his Salary and the Name of his department
		saveCounts("null");
		System.out.println("For each employee, display his Salary and the Name of his department");
		Predicate[] join_preds=new Predicate[]{new Predicate(AttrOperator.EQ,AttrType.FIELDNO,4,AttrType.FIELDNO,5)};
		SimpleJoin join=new SimpleJoin(new FileScan(s_emp,emp),new FileScan(s_dep,dep),join_preds);
		pro=new Projection(join,3,6);
		pro.execute();
		saveCounts("Query 5");
		
		//Display the Name and Salary for employees who work in the department that has DeptId = 3
		saveCounts("null");
		System.out.println("Display the Name and Salary for employees who work in the department that has DeptId = 3");
		pred=new Predicate(AttrOperator.EQ,AttrType.FIELDNO,4,AttrType.INTEGER,3);
		scan_emp=new FileScan(s_emp,emp);
		sel=new Selection(scan_emp,pred);
		pro=new Projection(sel,1,3);
		pro.execute();
		saveCounts("Query 6");
		
		//Display the Salary for each employee who works in a department that has MaxSalary > 100000
		saveCounts("null");
		System.out.println("Display the Salary for each employee who works in a department that has MaxSalary > 100000");
		join_preds=new Predicate[]{new Predicate(AttrOperator.EQ,AttrType.FIELDNO,4,AttrType.FIELDNO,5)};
		pred=new Predicate(AttrOperator.GT,AttrType.FIELDNO,8,AttrType.INTEGER,100000);
		join=new SimpleJoin(new FileScan(s_emp,emp),new FileScan(s_dep,dep),join_preds);
		sel=new Selection(join,pred);
		pro=new Projection(sel,3);
		pro.execute();
		saveCounts("Query 7");
		
		//Display the Name for each employee whose Salary is less than the MinSalary of his department
		saveCounts("null");
		System.out.println("Display the Name for each employee whose Salary is less than the MinSalary of his department");
		join_preds=new Predicate[]{new Predicate(AttrOperator.EQ,AttrType.FIELDNO,4,AttrType.FIELDNO,5)};
		pred=new Predicate(AttrOperator.LT,AttrType.FIELDNO,3,AttrType.FIELDNO,7);
		join=new SimpleJoin(new FileScan(s_emp,emp),new FileScan(s_dep,dep),join_preds);
		sel=new Selection(join,pred);
		pro=new Projection(sel,1);
		pro.execute();
		saveCounts("Query 8");
		
	}
}

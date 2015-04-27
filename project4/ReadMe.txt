Licensed By 
Xiaobo Zhang, Qiaomu Yao
Purdue University, CS 541 Database System
Project 4.

Contribution:
Xiaobo Zhang: Implemented CreateIndex, DropIndex, Delete, Insert, Update
Qiaomu Yao: Implemented Select, Update

Select Algorithm:
First use the predicates validate function to find the Query is in a single table or multiple, if in a single table, store the predicates in an array list and move to the other table. Then do the selection on the array list predicates and get the selected result.
Second, do the Simple join with the selection results for all the tables.
Third, do the projection on the specific column number.

Update and Deletion: need to evaluate the predicates first and find the tuple that matches all the predicates and do the operations.


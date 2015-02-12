rem CS 541 SQL Project 12
rem Qiaomu Yao
@script1.sql
drop table interncp;
drop table internavg;
------q7---------------
---create intern copy table 
create table interncp
as select * from intern;
--delete the intern that's not ended
delete from interncp
where enddate is null;
-- cannot use ='' or =null here!!!!
create table internavg as select * from(
select cmp.cmpid,avg(tt.duration) as duration
from company cmp,(
select  i.sid as sid,i.cmpid as cmpid,i.startdate -i.enddate 
as duration
from interncp i) tt
where cmp.cmpid=tt.cmpid
group by cmp.cmpid);
--select * from internavg;
--assume all the data are reasonable

drop table numofintern;
drop table numofschool;
drop table fulltab;	
drop table ranktab;
drop table numofschint;
create table fulltab as 
(select * from
((intern
natural join
company)
natural join
student));
---table of ranking
create table ranktab as select * from 
(select f1.cmpid,f1.schoolid,sch1.rank,sch1.name,f1.title from fulltab f1, school sch1
where sch1.schid=f1.schoolid );
---
create table numofintern as select * from(
select cmpid,count(cmpid) as numi from
fulltab
group by cmpid)
;

create table numofschool as select * from(
select cmpid,count(cmpid) as nums from( 
select distinct cmpid ,schoolid from fulltab)
group by cmpid);

create table numofschint as select * from (numofschool natural join numofintern);


-------------------------------------------
create or replace procedure pro_comp_report 
is
--with name as (subq),name2 as (subq2),.. selet * from name1 join name2
--create view name as select * from

--the procedure function
---start to open cursor
	cmp company.cmpid%type;
	cmptitle company.title%type;
	res_nums numofschool.nums%type;
	res_numi numofintern.numi%type;
	res_rank ranktab.rank%type;
	res_most school.schid%type;
	res_ranktitle school.name%type;
	res_mosttitle school.name%type;
	res_duration internavg.duration%type;
	res_duration_m internavg.duration%type;
	res_duration_d internavg.duration%type;
	--consider rank
	cursor c1
	is 
	select cmpid,title from company;
	cursor c2(cmp2 company.cmpid%type)
	is
	select nums,numi from numofschint
	where cmpid=cmp2;
	cursor c3(cmp3 company.cmpid%type)
	is
	select rank,name from (select * from ranktab
	       where cmpid=cmp3 order by rank)
	where rownum=1;
	cursor c4(cmp4 company.cmpid%type)
	is 
	select aaa.schoolid, sss.name from(select schoolid from (select schoolid,count(schoolid) as schnum from 
	       	      (select cmpid,schoolid from fulltab
	       	      where cmpid=cmp4)
		      group by schoolid order by count(schoolid) desc) 
	where rownum=1)	aaa, school sss
	where sss.schid=aaa.schoolid;
	cursor c5(cmp5 company.cmpid%type)
	is
	select duration from internavg
		where cmpid=cmp5;
begin
     dbms_output.put_line('CompanyTitle  NumOfInterns  NumOfSchools  AvgInternDuration  TopSchoolInterned  SchoolWithMostIntern');
     dbms_output.put_line('------------  ------------  ------------  -----------------  -----------------  --------------------');
open c1;
     loop
     fetch c1 into cmp,cmptitle;
     if c1%FOUND then
     	open c2(cmp2=>cmp);
	     fetch c2 into res_nums,res_numi;
	     if c2%FOUND then
	     	null;
	     end if;
	close c2;
	open c3(cmp3=>cmp);
	     fetch c3 into res_rank,res_ranktitle;
	     if c3%FOUND then
	     null;
	     end if;
	close c3;
	open c4(cmp4=>cmp);
	     fetch c4 into res_most,res_mosttitle;
	     if c4%FOUND then
	     null;
	     end if;
	close c4;
	open c5(cmp5=>cmp);
	     fetch c5 into res_duration;
	     if c5%FOUND then
	     	res_duration_m:=round(res_duration/30,0);
		res_duration_d:=round(mod(res_duration,30),0);
	     	
	     end if;
	close c5;
	dbms_output.put_line(cmptitle||'             '||res_numi||'           '||res_nums||'             '||res_duration_m||'-'||res_duration_d||'            '||res_ranktitle||'              '||res_mosttitle);
	else
	exit;
     end if;
      
     end loop;
close c1;
end pro_comp_report;
/




-----------2.2

----create friendship union table
drop table q11_union;
drop table internavg;
create table q11_union
as select * from
(
select friend.sid1 as sid11,friend.sid2 as sid22
from friend union all select  A1.sid1,A1.sid2 from (select friend.sid2 
as sid1,friend.sid1 as sid2 from friend order by sid1) A1
);
--use this table type for duration 

create table internavg as select * from(
select cmp.cmpid,avg(tt.duration) as duration
from company cmp,(
select  i.sid as sid,i.cmpid as cmpid,i.startdate -i.enddate 
as duration
from interncp i) tt
where cmp.cmpid=tt.cmpid
group by cmp.cmpid);



---create procedure
create or replace procedure pro_friend_suggestion
is
--declare
	
	sid1 student.sid%type;
	name1 student.name%type;
	sid2 student.sid%type;
	name2 student.name%type;
	sid_sharedfriends student.sid%type;
	cid_sharedcourses course.cid%type;
	numofsharedfriends integer:=0;
	numofsharedcourses integer:=0;
	schooltitle school.name%type;
	internedcmpid intern.cmpid%type;
	internedtitle company.title%type;
	internedduration internavg.duration%type;
	internedduration_m  internavg.duration%type;
	internedduration_d  internavg.duration%type;
	
	cursor c1 is 
	select sid,name
	from student;
----find the recommend friend	return sid,name and school title
	cursor c2(sid2_temp student.sid%type)
	is
	select tempn.sid,tempn.name,tempsch.name
	from school tempsch,student tempst,(
	select s.sid,s.name
	from student s
	     --s1 s2 are not friend
	where (not exists (select sid22 from q11_union
	      	   	  where sid11=sid2_temp and sid22=s.sid))
			  -- s1 s2 have same intern company
		and ((exists (select * from intern intern_temp1,
		   	   intern intern_temp2
		   	   where  intern_temp1.cmpid=intern_temp2.cmpid and
			   intern_temp1.sid=s.sid and intern_temp2.sid=sid2_temp))
			   or
			   --s1 s2 have a common friend
			   (exists (select * from student commonfriend,
			   q11_union tempu1, q11_union tempu2
			   where tempu1.sid11=sid2_temp and tempu1.sid22=commonfriend.sid and tempu2.sid11=s.sid and tempu2.sid22=commonfriend.sid and commonfriend.sid<>s.sid and commonfriend.sid<>sid2_temp))
			   )
			   --unique
		and s.sid<>sid2_temp
			   ) tempn
	where tempn.sid=tempst.sid and tempst.schoolid=tempsch.schid;

---find each shared friend			  			    
	cursor c3(sid3_1 student.sid%type,sid3_2 student.sid%type)
	is
	select distinct sid from student
	where (exists (select * from q11_union u1,q11_union u2
	      	      where u1.sid11<>u2.sid11 and u1.sid11=sid3_1
		      and u2.sid11=sid3_2 and u1.sid22=sid and u2.sid22=sid));
---find each of shared course
	cursor c4(sid4_1 student.sid%type,sid4_2 student.sid%type)
	is
	select distinct st1.cid from studied st1,studied st2
	where st1.cid=st2.cid and st1.sid=sid4_1 and st2.sid=sid4_2;
---find the interned company of recommend friend
	cursor c5(sid5 student.sid%type)
	is
	select cmpid,title,startdate-enddate 
	from(company natural join intern)
	where sid5=sid;
--start execute procedure
begin
open c1;
     loop
	fetch c1 into sid1,name1;
	if c1%FOUND then
	   dbms_output.put_line('Student ID: '||sid1);
	   dbms_output.put_line('Student Name: '||name1);
	   open c2(sid1);
	   	loop
			numofsharedfriends:=0;
			numofsharedcourses:=0;
			fetch c2 into sid2,name2,schooltitle;
			if c2%FOUND then
			dbms_output.put_line('FriendID  '||'FriendName  '||'NumOfSharedFriends  '||'NumOfSharedCourses  '||'SchoolName');
			dbms_output.put_line('--------  '||'----------  '||'------------------  '||'------------------  '||'----------');
			
			open c3(sid1,sid2);
			     loop
				fetch c3 into sid_sharedfriends;
				if c3%FOUND then
				   numofsharedfriends:=numofsharedfriends+1;
			
				else 
				exit;
				end if;  
			     end loop;
			close c3;
			
			open c4(sid1,sid2);
			     loop
				fetch c4 into cid_sharedcourses;
				if c4%FOUND then
				   numofsharedcourses:=numofsharedcourses+1;
			
				else 
				exit;
				end if;  
			     end loop;
			close c4;
			dbms_output.put_line(sid2||'         '||name2||'            '||numofsharedfriends||'                  '||numofsharedcourses||'                 '||schooltitle);				
			open c5(sid2);
			fetch c5 into internedcmpid,internedtitle,internedduration;
			if c5%FOUND then
			dbms_output.put_line('|      '||'CompaniesInterned');
			dbms_output.put_line('|                '||'CompanyID CompanyName   InternshipDuration');
			dbms_output.put_line('|                '||'--------- -----------   ------------------');
			   internedduration_m:=round(internedduration/30,0);
                	   internedduration_d:=round(mod(internedduration,30),0);
                	dbms_output.put_line('|				'||internedcmpid||'        '||internedtitle||'           '||internedduration_m||'-'||internedduration_d);

			end if;
			close c5;
			
			else 
			exit;
			end if;	
		end loop;
	close c2;
	else
	exit;
	end if;
     end loop;
close c1;
end pro_friend_suggestion;
/

-----------------2.3

create or replace procedure pro_school_report
is
	 res_schid school.schid%type;
	 res_name school.name%type;
	 res_rank school.rank%type;
	 onestudent student.sid%type;
	 oneintern student.sid%type;
	 numofstudents integer:=0;
	 numofinterns integer:=0;
	 res_cmpid company.cmpid%type;
	 res_title company.title%type;
	 numofstudents_cmp integer:=0;
--school cursor for each school
	 cursor c1 
       	 is
	 select schid,name,rank
	 from school;
--students cursor finding the total num of students
	 cursor c2(schid2 school.schid%type)
	 is
	 select distinct s2.sid from student s2
	 where s2.schoolid=schid2;
--finding the total num of interned studens
	 cursor c3(schid3 school.schid%type)
	 is
	 select distinct s3.sid from student s3
	 where s3.schoolid=schid3 and (exists 
	       (select * from intern i3
	       where i3.sid=s3.sid
	       ));
--finding the company interned
	 cursor c4(schid4 school.schid%type)
	 is  
	 select cmpid,title,count(cmpid) as num from
	 (select distinct cmpid,title,sid from (select * from
	 	 	     	  ((company natural join intern) natural join 
	 		    	 student)) s4
				 where  schid4=s4.schoolid)
	 group by cmpid,title;
begin
open c1;
     loop
	fetch c1 into res_schid,res_name,res_rank;
	if c1%FOUND then
	   numofstudents:=0;
	   numofinterns:=0;
	   numofstudents_cmp:=0;
	   dbms_output.put_line('SchoolID: '||res_schid);
	   dbms_output.put_line('SchoolName: '||res_name);
	   open c2(res_schid);
	   	loop
			fetch c2 into onestudent;
			if c2%FOUND then
			   numofstudents:=numofstudents+1;
			else
			exit;
			end if;
		end loop;
	   close c2;
	   open c3(res_schid);
	   	loop
			fetch c3 into oneintern;
			if c3%FOUND then
			   numofinterns:=numofinterns+1;
			else
			exit;
			end if;
		end loop;
	   close c3;
	   dbms_output.put_line('TotalNumOfStudents  NumOfStudentsInterned  SchoolRank');
	   dbms_output.put_line('------------------  ---------------------  ----------');
	   dbms_output.put_line(numofstudents||'                          '||numofinterns||'                   '||res_rank); 
	   open c4(res_schid);
		  dbms_output.put_line('CompaniesInterned: ');
		  dbms_output.put_line('|              CompanyID  CompanyName   NumOfStudens');
		  dbms_output.put_line('|              ---------  -----------   ------------');
		loop
			fetch c4 into res_cmpid,res_title,numofstudents_cmp;
			if c4%FOUND then
			     dbms_output.put_line('|               '||res_cmpid||'          '||res_title||'        '||numofstudents_cmp);	 
			else
			exit;
			end if;
		end loop;
	   close c4;
	else
	exit;
	end if;
     end loop;
close c1;
end pro_school_report;
/

exec pro_comp_report
exec pro_friend_suggestion
exec pro_school_report

---drop all the tables----please run script1.sql and q12.sql before run procedure

BEGIN
   FOR cur_rec IN (SELECT object_name, object_type
                     FROM user_objects
                    WHERE object_type IN
                             ('TABLE',
                              'VIEW',
                              'PACKAGE',
                              'PROCEDURE',
                              'FUNCTION',
                              'SEQUENCE'
                             ))
   LOOP
      BEGIN
         IF cur_rec.object_type = 'TABLE' then
            EXECUTE IMMEDIATE    'DROP '
                              || cur_rec.object_type
                              || ' "'
                              || cur_rec.object_name
                              || '" CASCADE CONSTRAINTS';
         ELSE
            null;
         END IF;
      EXCEPTION
         WHEN OTHERS
         THEN
            DBMS_OUTPUT.put_line (   'FAILED: DROP '
                                  || cur_rec.object_type
                                  || ' "'
                                  || cur_rec.object_name
                                  || '"'
                                 );
      END;
   END LOOP;
END;
/


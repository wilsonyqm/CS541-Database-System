rem CS 541 SQL Project 12
rem Qiaomu Yao
select S.name 
from (select studied.sid,count(studied.sid) as num from studied group by studied.sid) A, student S
where A.num=1 and S.sid=A.sid
;

---q2

create table q2_union
as select * from
(
select friend.sid1 as sid11,friend.sid2 as sid22
from friend union all select  A1.sid1,A1.sid2 from (select friend.sid2 
as sid1,friend.sid1 as sid2 from friend order by sid1) A1
);

--q2_count
Create table q2_count(
    sid integer ,
    num integer
);
Insert into q2_count
select * from(
select t1.sid11, count(t1.sid11) as num
from q2_union t1
group by t1.sid11);


select S1.name,S2.name
from(
--pair generation
select A.sid as id1,C.sid as id2,A.num+C.num as id3
from q2_count A,q2_count C
where A.sid<C.sid 
    and (not exists (select BB.sid11,BB.sid22
                        from q2_union BB
                        where (BB.sid11= A.sid and BB.sid22=C.sid)))
    and (not exists (select AA.sid,CC.sid 
      from q2_count AA,q2_count CC
      where ((AA.num+CC.num>A.num+C.num)
      and (AA.sid<CC.sid)
      and (not exists (select B.sid11,B.sid22
                        from q2_union B
                        where (B.sid11= AA.sid and B.sid22=CC.sid))))
      ))
) R, Student S1,Student S2
where (S1.sid=R.id1 and S2.sid=R.id2)
;
--select * from q2_union;
--select * from q2_count;
drop table q2_union;
drop table q2_count;

--q3
--q3----------
select R.title
from company R
where (not exists (select S.cmpid
from 
(
select A.sid, A.cmpid, C.rank
from intern A, school C, student B
where (B.sid=A.sid and C.schid=B.schoolid and C.rank>3)
) S
where R.cmpid=S.cmpid))
;
---q4
select t1.name, count(*)
from (
select s.name as name, st1.sid as sid1,st2.sid as sid2
from school s,friend f, student st1, student st2
where s.schid=st1.schoolid and s.schid=st2.schoolid
and f.sid1=st1.sid and f.sid2=st2.sid
) t1
where not exists (select * from
(
select s.name as name, st1.sid as sid1,st2.sid as sid2
from school s,friend f, student st1, student st2
where s.schid=st1.schoolid and s.schid=st2.schoolid
and f.sid1=st1.sid and f.sid2=st2.sid
) t2
where t1.sid1=t2.sid2 and t1.sid2=t2.sid1)
group by t1.name
;
--q5
----q5------------------
select sch.name,cmp.title, t3.num 
from school sch, company cmp,
(
select schid ,cmpid , count(*) as num
from
(
select  distinct sid, schoolid as schid, cmpid
from student 
natural join --nice natural join
intern 
) t1
group by schid,cmpid) t3
,
(
select  max(t2.num) as maxnum
from (
select schid ,cmpid , count(*) as num
from
(
select  distinct sid, schoolid as schid, cmpid
from student 
natural join
--nice natural join
intern 
) t1
group by schid,cmpid) t2) t4
where sch.schid=t3.schid and cmp.cmpid=t3.cmpid and t4.maxnum=t3.num;
-----return school, company, students num

--q6
create table temp
as select * from(
select t2.schid as schid1, count(*) as num
from (
select distinct t1.cid,t1.schoolid as schid
from (
select *
from 
studied 
natural join
student
) t1
group by t1.cid,t1.schoolid
) t2
group by t2.schid order by num desc
);

select t4.name, t5.maxnum
from school t4,
(
select t3.schid1 as schid2, t3.num as maxnum
from temp t3, (select * from temp where rownum=1) tt3
where t3.num=tt3.num
) t5
where t4.schid=t5.schid2;
drop table temp;
--q7
------q7---------------
---create intern copy table 
create table interncp
as select * from intern;
--delete the intern that's not ended
delete from interncp
where enddate is null;
-- cannot use ='' or =null here!!!!
select  i.sid as sid,i.cmpid as cmpid,i.startdate -i.enddate 
as duration
from interncp i;

select cmp.title,avg(tt.duration)
from company cmp,(
select  i.sid as sid,i.cmpid as cmpid,i.startdate -i.enddate 
as duration
from interncp i) tt
where cmp.cmpid=tt.cmpid
group by cmp.title;

drop table interncp;
--assume all the data are reasonable

--q8

create table temp as select * from (select cmpid,schoolid, count(sid) as count
from (select sid,cmpid,schoolid from 
intern
natural join
student) t
group by cmpid,schoolid order by schoolid);
select * from temp;

select sch3.name,sch4.name from
school sch3,school sch4,
(select  sch_t.schid,te.cmpid,te.count
from school sch_t,
(select sch1.cmpid as cmpid,sch1.count as count,sch1.schoolid as schid from temp sch1 where (not exists (select * from temp sch2 where sch1.count<sch2.count and sch1.schoolid=sch2.schoolid))) te
where sch_t.schid=te.schid) sch5,
(select  sch_t.schid,te.cmpid,te.count
from school sch_t,
(select sch1.cmpid as cmpid,sch1.count as count,sch1.schoolid as schid from temp sch1 where (not exists (select * from temp sch2 where sch1.count<sch2.count and sch1.schoolid=sch2.schoolid))) te
where sch_t.schid=te.schid) sch6
where sch3.schid>sch4.schid and sch6.schid=sch3.schid and sch5.schid=sch4.schid
and sch5.cmpid=sch6.cmpid; 

drop table temp;

--q9
create table temp as select * from (
select cmpid, count(cmpid) as count
from 
(select distinct sid, schoolid, cmpid from
(student
natural join
intern))
group by cmpid order by count(cmpid) desc);
--find the most internships companyi
select c.title,t1.title,a.sid
from studied a,intern i,course c,(
select cmp.cmpid as cmpid,cmp.title as title,tt.count
from company cmp,temp tt,(select * from temp where rownum=1) t
where t.count=tt.count and cmp.cmpid=tt.cmpid) t1
where t1.cmpid=i.cmpid and a.sid=i.sid  and a.cid=c.cid;
drop table temp;

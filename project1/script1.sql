CREATE TABLE SCHOOL(
	SCHID 	INTEGER CHECK (SCHID >= 0 AND SCHID <= 9999),
	NAME	VARCHAR2(40) NOT NULL,
	ADDRESS	VARCHAR2(100),
	RANK	INTEGER CHECK (RANK > 0),
	PRIMARY KEY (SCHID));

CREATE TABLE STUDENT(
	SID 		INTEGER CHECK (SID >= 0 AND SID <= 9999),
	NAME		VARCHAR2(30) NOT NULL,
	SCHOOLID	INTEGER CHECK (SCHOOLID >= 0 AND SCHOOLID <= 9999),
	COLLEGEYEAR 		VARCHAR2(10) CHECK (COLLEGEYEAR  IN ('Freshman','Sophomore','Junior','Senior')),
	PRIMARY KEY (SID),
	FOREIGN KEY (SCHOOLID) REFERENCES SCHOOL (SCHID));


CREATE TABLE FRIEND(
	SID1	INTEGER CHECK (SID1 >= 0 AND SID1 <= 9999),
	SID2	INTEGER CHECK (SID2 >= 0 AND SID2 <= 9999),
	DESCRIPTION	VARCHAR2(20),
	PRIMARY KEY (SID1, SID2),
	FOREIGN KEY (SID1) REFERENCES STUDENT (SID),
	FOREIGN KEY (SID2) REFERENCES STUDENT (SID));

CREATE TABLE COURSE(
	CID		INTEGER CHECK (CID >= 0 AND CID <= 9999),
	Title	VARCHAR2(30) NOT NULL,
	PRIMARY KEY (CID));

CREATE TABLE STUDIED(
	CID 	INTEGER CHECK (CID >= 0 AND CID <= 9999), 
	SID 	INTEGER CHECK (SID >= 0 AND SID <= 9999),
	PRIMARY KEY (CID, SID),
	FOREIGN KEY (CID) REFERENCES COURSE(CID),
	FOREIGN KEY (SID) REFERENCES STUDENT(SID));

CREATE TABLE COMPANY(
	CMPID	INTEGER CHECK (CMPID >= 0 AND CMPID <= 9999),
	Title	VARCHAR2(20),
	ADDRESS VARCHAR2(100),
	PRIMARY KEY (CMPID));

CREATE TABLE INTERN(
	SID 	INTEGER CHECK (SID >= 0 AND SID <= 9999),
	CMPID 	INTEGER CHECK (CMPID >= 0 AND CMPID <= 9999),
	STARTDATE 	DATE,
	ENDDATE		DATE,
	PRIMARY KEY (SID, CMPID, STARTDATE),
	FOREIGN KEY (SID) REFERENCES STUDENT(SID),
	FOREIGN KEY (CMPID) REFERENCES COMPANY(CMPID));


-- following lines are sample dataset.
-- you will need to populate more ones to see/test each query.

INSERT INTO SCHOOL VALUES (0001, 'Purdue', 'W Lafayette, IN', 1);
INSERT INTO SCHOOL VALUES (0002, 'UIUC', 'Urbana, IL', 2);
INSERT INTO SCHOOL VALUES (0003, 'Maryland', 'College Park, Maryland', 3);
INSERT INTO SCHOOL VALUES (0004, 'Berkley', 'San Francisco,CA', 4);
INSERT INTO SCHOOL VALUES (0005, 'MIT', 'Boston, MA', 5);

INSERT INTO STUDENT VALUES (0001, 'James', 0001, 'Sophomore');
INSERT INTO STUDENT VALUES (0002, 'Tom', 0001, 'Junior');
INSERT INTO STUDENT VALUES (0003, 'Bob', 0001, 'Freshman');
INSERT INTO STUDENT VALUES (0004, 'Wilson', 0001, 'Sophomore');
INSERT INTO STUDENT VALUES (0005, 'Amy', 0002, 'Freshman');
INSERT INTO STUDENT VALUES (0006, 'Justin', 0003, 'Sophomore');
INSERT INTO STUDENT VALUES (0007, 'Das', 0004, 'Junior');
INSERT INTO STUDENT VALUES (0008, 'Nice', 0004, 'Junior');
INSERT INTO STUDENT VALUES (0009, 'Foo', 0005, 'Junior');
INSERT INTO STUDENT VALUES (0010, 'Vasu', 0005, 'Sophomore');
INSERT INTO STUDENT VALUES (0011, 'Nan', 0005, 'Freshman');
INSERT INTO STUDENT VALUES (0012, 'Lee', 0005, 'Junior');
INSERT INTO STUDENT VALUES (0013, 'Jack', 0005, 'Sophomore');

INSERT INTO FRIEND VALUES(0001, 0002, 'friend');
INSERT INTO FRIEND VALUES(0001, 0003, 'friend');
INSERT INTO FRIEND VALUES(0001, 0004, 'friend');
INSERT INTO FRIEND VALUES(0002, 0003, 'friend');
INSERT INTO FRIEND VALUES(0003, 0006, 'friend');
INSERT INTO FRIEND VALUES(0003, 0007, 'friend');
INSERT INTO FRIEND VALUES(0003, 0008, 'friend');
INSERT INTO FRIEND VALUES(0004, 0008, 'friend');
INSERT INTO FRIEND VALUES(0005, 0006, 'friend');
INSERT INTO FRIEND VALUES(0007, 0008, 'friend');
INSERT INTO FRIEND VALUES(0002, 0006, 'friend');
INSERT INTO FRIEND VALUES(0002, 0008, 'friend');
INSERT INTO FRIEND VALUES(0001, 0005, 'friend');
INSERT INTO FRIEND VALUES(0010, 0011, 'friend');
INSERT INTO FRIEND VALUES(0011, 0010, 'friend');
INSERT INTO FRIEND VALUES(0009, 0010, 'friend');
INSERT INTO FRIEND VALUES(0007, 0011, 'friend');


INSERT INTO COURSE VALUES(503, 'Operating System');
INSERT INTO COURSE VALUES(541, 'Database System');
INSERT INTO COURSE VALUES(580, 'Algorithm Design');
INSERT INTO COURSE VALUES(570, 'Artifitial Intellegience');
INSERT INTO COURSE VALUES(510, 'SE');
INSERT INTO COURSE VALUES(512, 'ML');
INSERT INTO COURSE VALUES(511, 'DS');


INSERT INTO STUDIED VALUES(503, 0001);
INSERT INTO STUDIED VALUES(541, 0002);
INSERT INTO STUDIED VALUES(580, 0002);
INSERT INTO STUDIED VALUES(503, 0004);
INSERT INTO STUDIED VALUES(541, 0004);
INSERT INTO STUDIED VALUES(570, 0004);
INSERT INTO STUDIED VALUES(511, 0004);
INSERT INTO STUDIED VALUES(510, 0006);
INSERT INTO STUDIED VALUES(512, 0005);
INSERT INTO STUDIED VALUES(512, 0009);
INSERT INTO STUDIED VALUES(570, 0009);
INSERT INTO STUDIED VALUES(503, 0008);
INSERT INTO STUDIED VALUES(580, 0008);
INSERT INTO STUDIED VALUES(580, 0007);
INSERT INTO STUDIED VALUES(580, 0003);
INSERT INTO STUDIED VALUES(580, 0010);
INSERT INTO STUDIED VALUES(580, 0011);
INSERT INTO STUDIED VALUES(580, 0012);
INSERT INTO STUDIED VALUES(580, 0013);


INSERT INTO COMPANY VALUES(1111, 'Google', 'Mountain View, CA');
INSERT INTO COMPANY VALUES(1112, 'Facebook', 'Menlo Park, CA');
INSERT INTO COMPANY VALUES(1113, 'LinkedIn', 'Mountain View, CA');

INSERT INTO INTERN VALUES(0001, 1113, '25-MAY-14', '15-JUL-12');
INSERT INTO INTERN VALUES(0002, 1113, '20-MAY-14', '5-JUL-12');
INSERT INTO INTERN VALUES(0002, 1112, '20-JUN-14', '');
INSERT INTO INTERN VALUES(0004, 1111, '5-JUL-12', '20-MAY-12');
INSERT INTO INTERN VALUES(0004, 1111, '20-AUG-13', '22-MAY-13');
INSERT INTO INTERN VALUES(0004, 1112, '20-MAY-14', '5-JUL-13');
INSERT INTO INTERN VALUES(0004, 1111, '20-DEC-14', '30-JAN-13');
INSERT INTO INTERN VALUES(0007, 1111, '20-DEC-14', '30-JAN-13');
INSERT INTO INTERN VALUES(0007, 1111, '20-DEC-13', '30-JAN-12');
INSERT INTO INTERN VALUES(0008, 1111, '20-DEC-13', '30-JAN-12');
INSERT INTO INTERN VALUES(0008, 1112, '20-DEC-14', '22-FEB-13');
INSERT INTO INTERN VALUES(0005, 1111, '20-MAY-14', '7-JUL-13');
INSERT INTO INTERN VALUES(0006, 1111, '11-MAY-14', '3-JUL-13');
INSERT INTO INTERN VALUES(0009, 1111, '11-MAY-14', '3-JUL-13');
INSERT INTO INTERN VALUES(0010, 1111, '11-MAY-14', '3-JUL-13');
INSERT INTO INTERN VALUES(0011, 1111, '11-MAY-14', '3-JUL-13');
INSERT INTO INTERN VALUES(0012, 1112, '11-MAY-14', '3-JUL-13');
INSERT INTO INTERN VALUES(0013, 1112, '11-MAY-13', '3-JUL-12');
INSERT INTO INTERN VALUES(0013, 1113, '11-MAY-14', '3-JUL-13');
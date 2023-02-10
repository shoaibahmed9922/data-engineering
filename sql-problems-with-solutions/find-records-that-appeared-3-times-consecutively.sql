-- TABLE DEFINITION
CREATE TABLE test (
    id INTEGER PRIMARY KEY,
    num INTEGER
);

INSERT INTO test VALUES (1,1);
INSERT INTO test VALUES (2,1);
INSERT INTO test VALUES (3,1);
INSERT INTO test VALUES (4,2);
INSERT INTO test VALUES (5,5);
INSERT INTO test VALUES (6,5);
INSERT INTO test VALUES (7,1);
INSERT INTO test VALUES (8,2);
INSERT INTO test VALUES (9,7);
INSERT INTO test VALUES (10,7);
INSERT INTO test VALUES (11,7);
INSERT INTO test VALUES (12,7);
INSERT INTO test VALUES (13,1);
INSERT INTO test VALUES (14,1);
INSERT INTO test VALUES (15,1);
INSERT INTO test VALUES (16,1);


-- SOLUTION BELOW

select id, num
from(
        select
        id,
        num,
        CASE when (num=LEAD(num,1) OVER(order by id asc) AND num=LEAD(num,2) OVER(order by id asc))
        OR (num=LAG(num,1) OVER(order by id asc) AND num=LEAD(num,1) OVER(order by id asc))
        OR (num=LAG(num,1) OVER(order by id asc) AND num=LAG(num,2) OVER(order by id asc))
            then true else false
        end as c
        from test
    )temp
where temp.c = 1

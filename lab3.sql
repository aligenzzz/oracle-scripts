-- schemas' creation
CREATE USER C##dev_schema IDENTIFIED BY dev;
CREATE USER C##prod_schema IDENTIFIED BY prod;

GRANT EXECUTE ANY PROCEDURE TO SYSTEM;
GRANT EXECUTE ANY PROCEDURE ON SCHEMA C##DEV_SCHEMA TO SYSTEM;
GRANT EXECUTE ANY PROCEDURE ON SCHEMA C##PROD_SCHEMA TO SYSTEM;
GRANT SELECT ANY TABLE TO SYSTEM;
GRANT SELECT ANY TABLE ON SCHEMA C##DEV_SCHEMA TO SYSTEM;
GRANT SELECT ANY TABLE ON SCHEMA C##PROD_SCHEMA TO SYSTEM;
GRANT SELECT ANY DICTIONARY TO SYSTEM;
GRANT SELECT_CATALOG_ROLE TO SYSTEM;
GRANT EXECUTE_CATALOG_ROLE TO SYSTEM;

GRANT SELECT_CATALOG_ROLE TO FUNCTION create_object;
GRANT SELECT_CATALOG_ROLE TO FUNCTION update_object;

-- dev schema's intialization
CREATE TABLE C##dev_schema.common_table (
    id INT PRIMARY KEY,
    name VARCHAR(50)
);

CREATE TABLE C##dev_schema.foreign_table (
    id INT PRIMARY KEY,
    common_id INT,
    FOREIGN KEY (common_id) REFERENCES C##dev_schema.common_table(id)
);

CREATE TABLE C##dev_schema.diff_table (
    id INT PRIMARY KEY,
    name VARCHAR(50),
    description VARCHAR(100)
);

CREATE TABLE C##dev_schema.new_table (
    id INT PRIMARY KEY,
    name VARCHAR(200)
);

CREATE OR REPLACE PROCEDURE C##dev_schema.my_procedure AS
BEGIN
  NULL;
END;
/

CREATE OR REPLACE FUNCTION C##dev_schema.my_function
RETURN NUMBER AS
BEGIN
  RETURN 1;
END;
/

-- prod schema's initialization
CREATE TABLE C##prod_schema.common_table (
    id INT PRIMARY KEY,
    name VARCHAR(50)
);

CREATE TABLE C##prod_schema.foreign_table (
    id INT PRIMARY KEY,
    common_id INT,
    FOREIGN KEY (common_id) REFERENCES C##prod_schema.common_table(id)
);

CREATE TABLE C##prod_schema.diff_table (
    id INT PRIMARY KEY,
    name VARCHAR(50)
);

CREATE TABLE C##prod_schema.circle1 (
    id INT PRIMARY KEY,
    circle2_id INT
);

CREATE TABLE C##prod_schema.circle2 (
    id INT PRIMARY KEY,
    circle1_id INT,
    FOREIGN KEY (circle1_id) REFERENCES C##prod_schema.circle1(id)
);

ALTER TABLE C##prod_schema.circle1
ADD CONSTRAINT fk_circle2_id
FOREIGN KEY (circle2_id) REFERENCES C##prod_schema.circle2(id);

CREATE OR REPLACE PROCEDURE C##prod_schema.my_procedure AS
BEGIN
  NULL;
END;
/

CREATE OR REPLACE FUNCTION C##prod_schema.my_function
RETURN NUMBER AS
BEGIN
  RETURN 2;
END;
/

CREATE TABLE comparison_result(
    table_name VARCHAR2(100),
    is_different NUMBER(1) DEFAULT 0,
    is_only_in_dev_schema NUMBER(1) DEFAULT 0,
    is_only_in_prod_schema NUMBER(1) DEFAULT 0
);

CREATE TABLE sorted_tables (
    table_name VARCHAR2(100)
);

CREATE OR REPLACE PROCEDURE sort_tables_in_schema(schema_name IN VARCHAR2) 
AS
BEGIN
    FOR rec IN (
        WITH DEPENDENCYTREE(table_name, lvl) AS (
            SELECT table_name, 1 AS lvl
            FROM all_tables
            WHERE owner = schema_name
            AND NOT EXISTS (
                SELECT 1
                FROM all_constraints
                WHERE constraint_type = 'R'
                AND r_constraint_name = constraint_name
            )
            UNION ALL
            SELECT a.table_name, b.lvl + 1
            FROM all_constraints a
            JOIN DEPENDENCYTREE b ON a.r_constraint_name = b.table_name
            WHERE a.owner = schema_name
            AND a.constraint_type = 'R'
        )
        SELECT table_name
        FROM DEPENDENCYTREE
        ORDER BY lvl
    ) LOOP
        BEGIN
            INSERT INTO sorted_tables (table_name) VALUES (rec.table_name);
        END;
    END LOOP;
END sort_tables_in_schema;
/

CREATE OR REPLACE PROCEDURE compare_schemas(dev_schema in VARCHAR2, prod_schema in VARCHAR2, ddl_output in VARCHAR2) 
AS
    diff NUMBER := 0;
    query_string VARCHAR2(4000) := '';
    temp_string VARCHAR2(4000) := '';
BEGIN      
    FOR same_table IN 
        (SELECT table_name FROM all_tables dev_tables WHERE OWNER = dev_schema
        INTERSECT
        SELECT prod_tables.table_name FROM all_tables prod_tables WHERE OWNER = prod_schema) 
    LOOP
        SELECT COUNT(*) INTO diff FROM
        (SELECT dev_table.COLUMN_NAME name, dev_table.DATA_TYPE FROM all_tab_columns dev_table 
        WHERE OWNER=dev_schema AND TABLE_NAME = same_table.table_name) dev_columns
        FULL JOIN
        (SELECT prod_table.COLUMN_NAME name, prod_table.DATA_TYPE FROM all_tab_columns prod_table
        WHERE OWNER = prod_schema AND TABLE_NAME = same_table.table_name) prod_columns
        ON dev_columns.name = prod_columns.name
        WHERE dev_columns.name IS NULL OR prod_columns.name IS NULL;

        IF diff > 0 THEN
            INSERT INTO comparison_result (table_name, is_different) VALUES (same_table.table_name, 1);
        ELSE
            INSERT INTO comparison_result (table_name) VALUES (same_table.table_name);
        END IF;
    END LOOP;

    FOR other_table IN 
        (SELECT dev_tables.table_name name FROM all_tables dev_tables WHERE dev_tables.OWNER = dev_schema
        MINUS 
        SELECT prod_tables.table_name FROM all_tables prod_tables WHERE prod_tables.OWNER = prod_schema) 
    LOOP
        INSERT INTO comparison_result (table_name, is_only_in_dev_schema) VALUES (other_table.name, 1);
    END LOOP;
    FOR other_table IN 
        (SELECT prod_tables.table_name name FROM all_tables prod_tables WHERE prod_tables.OWNER = prod_schema
        MINUS
        SELECT dev_tables.table_name FROM all_tables dev_tables WHERE dev_tables.OWNER = dev_schema) 
    LOOP
        INSERT INTO comparison_result (table_name, is_only_in_prod_schema) VALUES (other_table.name, 1);
    END LOOP;
    
    DBMS_OUTPUT.PUT_LINE('Tables in ' || dev_schema || ':');
    check_cyclic_dependencies(dev_schema);
    sort_tables_in_schema(dev_schema);
    FOR rec IN (
        SELECT comparison_result.*
        FROM sorted_tables 
        JOIN comparison_result 
        ON sorted_tables.table_name = comparison_result.table_name
    ) LOOP
        IF rec.is_different = 1 THEN
            DBMS_OUTPUT.PUT_LINE('Table ' || rec.table_name || ' is with different structure');
        ELSIF rec.is_only_in_dev_schema = 1 THEN
            DBMS_OUTPUT.PUT_LINE('Table ' || rec.table_name || ' is only in ' || dev_schema);

            SELECT create_object('TABLE', rec.table_name, prod_schema, dev_schema) INTO temp_string;
            query_string := query_string || CHR(10) || temp_string;
        ELSE
            DBMS_OUTPUT.PUT_LINE('Table ' || rec.table_name || ' is same');
        END IF;            
    END LOOP; 
    EXECUTE IMMEDIATE 'DELETE FROM sorted_tables';
    
    DBMS_OUTPUT.PUT_LINE(CHR(10) || 'Tables in ' || prod_schema || ':');
    check_cyclic_dependencies(prod_schema);
    sort_tables_in_schema(prod_schema);
    FOR rec IN (
        SELECT comparison_result.*
        FROM sorted_tables 
        JOIN comparison_result 
        ON sorted_tables.table_name = comparison_result.table_name
    ) LOOP
        IF rec.is_different = 1 THEN
            DBMS_OUTPUT.PUT_LINE('Table ' || rec.table_name || ' is with different structure');

            SELECT update_object('TABLE', rec.table_name, prod_schema, dev_schema) INTO temp_string;
            query_string := query_string || CHR(10) || temp_string;

        ELSIF rec.is_only_in_prod_schema = 1 THEN
            DBMS_OUTPUT.PUT_LINE('Table ' || rec.table_name || ' is only in ' || prod_schema);

            SELECT delete_object('TABLE', rec.table_name, prod_schema) INTO temp_string;
            query_string := query_string || CHR(10) || temp_string;
        ELSE
            DBMS_OUTPUT.PUT_LINE('Table ' || rec.table_name || ' is same');
        END IF;            
    END LOOP; 
    EXECUTE IMMEDIATE 'DELETE FROM sorted_tables';

    EXECUTE IMMEDIATE 'DELETE FROM comparison_result';

    IF ddl_output = 1 THEN
        DBMS_OUTPUT.PUT_LINE(CHR(10) || 'DDL Script:' || CHR(10));
        DBMS_OUTPUT.PUT_LINE(query_string);
    END IF;

    query_string := '';

    compare_schemas_objects(dev_schema, prod_schema, query_string);

    IF ddl_output = 1 THEN
        DBMS_OUTPUT.PUT_LINE(CHR(10) || 'DDL Script:' || CHR(10));
        DBMS_OUTPUT.PUT_LINE(query_string);
    END IF;
END compare_schemas;
/

CREATE TABLE schema_dependencies(
    child_obj VARCHAR2(100), 
    parent_obj VARCHAR2(100)
);

CREATE OR REPLACE PROCEDURE check_cyclic_dependencies(schema_name in VARCHAR2) 
AS
    result VARCHAR2(100);
BEGIN          
    FOR schema_table IN (SELECT schema_tables.table_name name FROM all_tables schema_tables WHERE owner = schema_name) 
    LOOP
        INSERT INTO schema_dependencies (child_obj, parent_obj)
            SELECT DISTINCT a.table_name, c_pk.table_name r_table_name FROM all_cons_columns a
            JOIN all_constraints c ON a.owner = c.owner AND a.constraint_name = c.constraint_name
            JOIN all_constraints c_pk ON c.r_owner = c_pk.owner AND c.r_constraint_name = c_pk.constraint_name
        WHERE c.constraint_type = 'R' AND a.table_name = schema_table.name;
    END LOOP;

    WITH Paths AS (
        SELECT child_obj, parent_obj, SYS_CONNECT_BY_PATH(child_obj, ',') AS path
        FROM schema_dependencies
        START WITH child_obj IN (SELECT DISTINCT child_obj FROM schema_dependencies)
        CONNECT BY NOCYCLE PRIOR parent_obj = child_obj
        AND LEVEL > 1
    )
    SELECT CASE 
             WHEN EXISTS (
               SELECT 1 
               FROM Paths 
               WHERE REGEXP_COUNT(path, ',') > 1
             ) THEN 'There ARE cyclic dependencies' 
             ELSE 'There are NOT cyclic dependencies' 
           END
    INTO result
    FROM dual;
    
    DBMS_OUTPUT.PUT_LINE(result);
    
    EXECUTE IMMEDIATE 'DELETE FROM schema_dependencies';
  
END check_cyclic_dependencies;
/

CREATE OR REPLACE PROCEDURE compare_schemas_objects(
    dev_schema IN VARCHAR2,
    prod_schema IN VARCHAR2,
    query_string out VARCHAR2
)
AS
    dev_text VARCHAR2(32767);
    prod_text VARCHAR2(32767);
    TYPE objarray IS VARRAY(4) OF VARCHAR2(10); 
    objects_arr objarray; 
    total INTEGER; 
    temp_string VARCHAR2(4000) := '';
BEGIN
    objects_arr := OBJARRAY('PROCEDURE', 'FUNCTION', 'INDEX', 'PACKAGE');
    total := objects_arr.count;

    DBMS_OUTPUT.PUT_LINE(CHR(10) || 'Objects comparison:');
    
    FOR i IN 1 .. total LOOP
        FOR same_object IN 
            (SELECT dev_objects.object_name 
            FROM all_objects dev_objects 
            WHERE owner = dev_schema AND object_type = objects_arr(i)
            INTERSECT
            SELECT prod_objects.object_name 
            FROM all_objects prod_objects 
            WHERE owner = prod_schema AND object_type = objects_arr(i)) 
        LOOP    
            SELECT REGEXP_REPLACE(LISTAGG(text, ' ') WITHIN GROUP (ORDER BY line), ' {2,}', ' ') 
            INTO dev_text
            FROM all_source
            WHERE owner = dev_schema AND name = same_object.object_name;
    
            SELECT REGEXP_REPLACE(LISTAGG(text, ' ') WITHIN GROUP (ORDER BY line), ' {2,}', ' ') 
            INTO prod_text
            FROM all_source
            WHERE owner = prod_schema AND name = same_object.object_name;
            
            IF dev_text != prod_text THEN
                DBMS_OUTPUT.PUT_LINE(objects_arr(i) || ' ' || same_object.object_name || ' has different structure');

                SELECT update_object(objects_arr(i), same_object.object_name, prod_schema, dev_schema) INTO temp_string;
                query_string := query_string || CHR(10) || temp_string;
            ELSE
                DBMS_OUTPUT.PUT_LINE(objects_arr(i) || ' ' || same_object.object_name || ' is the same');
            END IF;
        END LOOP;

        FOR other_object IN 
            (SELECT dev_objects.object_name 
            FROM all_objects dev_objects 
            WHERE owner = dev_schema AND object_type = objects_arr(i)
            MINUS
            SELECT prod_objects.object_name 
            FROM all_objects prod_objects 
            WHERE owner = prod_schema AND object_type = objects_arr(i)) 
        LOOP
            DBMS_OUTPUT.PUT_LINE(objects_arr(i) || ' ' || other_object.object_name || ' is only in ' || dev_schema);

            SELECT create_object(objects_arr(i), other_object.object_name, prod_schema, dev_schema) INTO temp_string;
            query_string := query_string || CHR(10) || temp_string;
        END LOOP;

        FOR other_object IN 
            (SELECT prod_objects.object_name 
            FROM all_objects prod_objects 
            WHERE owner = prod_schema AND object_type = objects_arr(i)
            MINUS
            SELECT dev_objects.object_name 
            FROM all_objects dev_objects 
            WHERE owner = dev_schema AND object_type = objects_arr(i)) 
        LOOP
            DBMS_OUTPUT.PUT_LINE(objects_arr(i) || ' ' || other_object.object_name || ' is only in ' || prod_schema);

            SELECT delete_object(objects_arr(i), other_object.object_name, prod_schema) INTO temp_string;
            query_string := query_string || CHR(10) || temp_string;           
        END LOOP;
    END LOOP;
END compare_schemas_objects;
/

CREATE OR REPLACE FUNCTION create_object (object_type IN VARCHAR2, object_name IN VARCHAR2, main_schema IN VARCHAR2, aux_schema IN VARCHAR2) 
RETURN VARCHAR2 IS
    result VARCHAR(4000);
BEGIN
    IF object_type = 'TABLE' THEN
        DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SQLTERMINATOR', TRUE);
        DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'PRETTY', TRUE);
        DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SEGMENT_ATTRIBUTES', FALSE);
        DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'STORAGE', FALSE);
        DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'TABLESPACE', FALSE);
    END IF;

    result := DBMS_METADATA.GET_DDL(object_type, object_name, aux_schema);
    result := REPLACE(result, aux_schema, main_schema);

    RETURN result;
END create_object;
/

CREATE OR REPLACE FUNCTION update_object (object_type IN VARCHAR2, object_name IN VARCHAR2, main_schema IN VARCHAR2, aux_schema IN VARCHAR2) 
RETURN VARCHAR2 IS
    result VARCHAR(4000);
BEGIN
    IF object_type = 'TABLE' THEN
        DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SQLTERMINATOR', TRUE);
        DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'PRETTY', TRUE);
        DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SEGMENT_ATTRIBUTES', FALSE);
        DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'STORAGE', FALSE);
        DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'TABLESPACE', FALSE);
    END IF;

    result := DBMS_METADATA.GET_DDL(object_type, object_name, aux_schema);
    result := REPLACE(result, aux_schema, main_schema);

    IF object_type = 'TABLE' THEN
        result := 'DROP ' || object_type || ' ' || main_schema || '.' || object_name || ';' || CHR(10) || result;
    END IF;
    
    RETURN result;
END update_object;
/

CREATE OR REPLACE FUNCTION delete_object (object_type IN VARCHAR2, object_name IN VARCHAR2, main_schema IN VARCHAR2) 
RETURN VARCHAR2 IS
BEGIN
    RETURN 'DROP ' || main_schema || '.' || object_type || ' ' || object_name || ';';
END delete_object;
/

BEGIN
    compare_schemas('C##DEV_SCHEMA', 'C##PROD_SCHEMA', 1);
END;
/
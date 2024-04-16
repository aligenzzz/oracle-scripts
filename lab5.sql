-- 1
CREATE TABLE TestTable1 (
    id NUMBER PRIMARY KEY,
    name VARCHAR2(100) NOT NULL,
    value NUMBER NOT NULL
);

CREATE TABLE TestTable2 (
    id NUMBER PRIMARY KEY,
    name VARCHAR2(100) NOT NULL,
    datetime TIMESTAMP NOT NULL
);

CREATE TABLE TestTable3 (
    id NUMBER PRIMARY KEY,
    name VARCHAR2(100) NOT NULL,
    fk_id INT,
    FOREIGN KEY (fk_id) REFERENCES TestTable2(id)
);


-- 2
CREATE TABLE LoggingForTestTable1 (
    id NUMBER PRIMARY KEY,

    operation VARCHAR2(50) NOT NULL,
    datetime TIMESTAMP NOT NULL,

    new_id NUMBER,
    old_id NUMBER,
    new_name VARCHAR2(100),
    old_name VARCHAR2(100),
    new_value NUMBER,
    old_value NUMBER
);

CREATE TABLE LoggingForTestTable2 (
    id NUMBER PRIMARY KEY,

    operation VARCHAR2(50) NOT NULL,
    datetime TIMESTAMP NOT NULL,

    new_id NUMBER,
    old_id NUMBER,
    new_name VARCHAR2(100),
    old_name VARCHAR2(100),
    new_datetime TIMESTAMP,
    old_datetime TIMESTAMP
);

CREATE TABLE LoggingForTestTable3 (
    id NUMBER PRIMARY KEY,

    operation VARCHAR2(50) NOT NULL,
    datetime TIMESTAMP NOT NULL,

    new_id NUMBER,
    old_id NUMBER,
    new_name VARCHAR2(100),
    old_name VARCHAR2(100),
    new_fk_id INT,
    old_fk_id INT
);

CREATE OR REPLACE TRIGGER TestTable1_trigger
BEFORE INSERT OR UPDATE OR DELETE 
ON TestTable1 FOR EACH ROW
DECLARE
    log_id NUMBER;
BEGIN
    SELECT NVL(MAX(ID), 0) + 1 INTO log_id FROM LoggingForTestTable1;

    IF INSERTING THEN
        INSERT INTO LoggingForTestTable1 (id, operation, datetime, new_id, old_id, new_name, old_name, new_value, old_value) 
        VALUES (log_id, 'INSERT', SYSTIMESTAMP, :NEW.id, NULL, :NEW.name, NULL, :NEW.value, NULL);
    ELSIF UPDATING THEN
        INSERT INTO LoggingForTestTable1 (id, operation, datetime, new_id, old_id, new_name, old_name, new_value, old_value) 
        VALUES (log_id, 'UPDATE', SYSTIMESTAMP, :NEW.id, :OLD.id, :NEW.name, :OLD.name, :NEW.value, :OLD.value);
    ELSIF DELETING THEN
        INSERT INTO LoggingForTestTable1 (id, operation, datetime, new_id, old_id, new_name, old_name, new_value, old_value) 
        VALUES (log_id, 'DELETE', SYSTIMESTAMP, NULL, :OLD.id, NULL, :OLD.name, NULL, :OLD.value);
    END IF;
END;
/

CREATE OR REPLACE TRIGGER TestTable2_trigger
BEFORE INSERT OR UPDATE OR DELETE 
ON TestTable2 FOR EACH ROW
DECLARE
    log_id NUMBER;
BEGIN
    SELECT NVL(MAX(ID), 0) + 1 INTO log_id FROM LoggingForTestTable2;

    IF INSERTING THEN
        INSERT INTO LoggingForTestTable2 (id, operation, datetime, new_id, old_id, new_name, old_name, new_datetime, old_datetime) 
        VALUES (log_id, 'INSERT', SYSTIMESTAMP, :NEW.id, NULL, :NEW.name, NULL, :NEW.datetime, NULL);
    ELSIF UPDATING THEN
        INSERT INTO LoggingForTestTable2 (id, operation, datetime, new_id, old_id, new_name, old_name, new_datetime, old_datetime) 
        VALUES (log_id, 'UPDATE', SYSTIMESTAMP, :NEW.id, :OLD.id, :NEW.name, :OLD.name, :NEW.datetime, :OLD.datetime);
    ELSIF DELETING THEN
        INSERT INTO LoggingForTestTable2 (id, operation, datetime, new_id, old_id, new_name, old_name, new_datetime, old_datetime) 
        VALUES (log_id, 'DELETE', SYSTIMESTAMP, NULL, :OLD.id, NULL, :OLD.name, NULL, :OLD.datetime);
    END IF;
END;
/

CREATE OR REPLACE TRIGGER TestTable3_trigger
BEFORE INSERT OR UPDATE OR DELETE 
ON TestTable3 FOR EACH ROW
DECLARE
    log_id NUMBER;
BEGIN
    SELECT NVL(MAX(ID), 0) + 1 INTO log_id FROM LoggingForTestTable3;

    IF INSERTING THEN
        INSERT INTO LoggingForTestTable3 (id, operation, datetime, new_id, old_id, new_name, old_name, new_fk_id, old_fk_id) 
        VALUES (log_id, 'INSERT', SYSTIMESTAMP, :NEW.id, NULL, :NEW.name, NULL, :NEW.fk_id, NULL);
    ELSIF UPDATING THEN
        INSERT INTO LoggingForTestTable3 (id, operation, datetime, new_id, old_id, new_name, old_name, new_fk_id, old_fk_id) 
        VALUES (log_id, 'UPDATE', SYSTIMESTAMP, :NEW.id, :OLD.id, :NEW.name, :OLD.name, :NEW.fk_id, :OLD.fk_id);
    ELSIF DELETING THEN
        INSERT INTO LoggingForTestTable3 (id, operation, datetime, new_id, old_id, new_name, old_name, new_fk_id, old_fk_id) 
        VALUES (log_id, 'DELETE', SYSTIMESTAMP, NULL, :OLD.id, NULL, :OLD.name, NULL, :OLD.fk_id);
    END IF;
END;
/


-- 3
CREATE OR REPLACE PACKAGE restore_package AS
    PROCEDURE restore_data_in_TestTable1(p_datetime VARCHAR);
    PROCEDURE restore_data_in_TestTable1(p_interval INTERVAL DAY TO SECOND);
    PROCEDURE restore_data_in_TestTable2(p_datetime VARCHAR);
    PROCEDURE restore_data_in_TestTable2(p_interval INTERVAL DAY TO SECOND);
    PROCEDURE restore_data_in_TestTable3(p_datetime VARCHAR);
    PROCEDURE restore_data_in_TestTable3(p_interval INTERVAL DAY TO SECOND);
END restore_package;
/

CREATE OR REPLACE PACKAGE BODY restore_package AS
    PROCEDURE restore_data_in_TestTable1(p_datetime VARCHAR) AS
    BEGIN
        FOR action IN (SELECT * FROM LoggingForTestTable1 WHERE p_datetime < datetime ORDER BY id DESC)
        LOOP
            IF action.operation = 'INSERT' THEN
                DELETE FROM TestTable1 
                WHERE id = action.new_id;
            END IF;
            
            IF action.operation = 'UPDATE' THEN
                UPDATE TestTable1 
                SET id = action.old_id, name = action.old_name, value = action.old_value
                WHERE id = action.new_id;
            END IF;
            
            IF action.operation = 'DELETE' THEN
                INSERT INTO TestTable1 (id, name, value) 
                VALUES (action.old_id, action.old_name, action.old_value);
            END IF;
        END LOOP;

        DELETE FROM LoggingForTestTable1 
        WHERE p_datetime < datetime;
    END restore_data_in_TestTable1;

    PROCEDURE restore_data_in_TestTable1(p_interval INTERVAL DAY TO SECOND) AS
        v_current_time TIMESTAMP;
        v_target_time TIMESTAMP;
    BEGIN
        SELECT CURRENT_TIMESTAMP INTO v_current_time FROM DUAL;
        v_target_time := v_current_time - p_interval - INTERVAL '0 03:00:00.000000' DAY TO SECOND;
        restore_data_in_TestTable1(TO_CHAR(v_target_time));
    END restore_data_in_TestTable1;


    PROCEDURE restore_data_in_TestTable2(p_datetime VARCHAR) AS
    BEGIN
        FOR action IN (SELECT * FROM LoggingForTestTable2 WHERE p_datetime < datetime ORDER BY id DESC)
        LOOP
            IF action.operation = 'INSERT' THEN
                DELETE FROM TestTable2 
                WHERE id = action.new_id;
            END IF;
            
            IF action.operation = 'UPDATE' THEN
                UPDATE TestTable2 
                SET id = action.old_id, name = action.old_name, datetime = action.old_datetime
                WHERE id = action.new_id;
            END IF;
            
            IF action.operation = 'DELETE' THEN
                INSERT INTO TestTable2 (id, name, datetime) 
                VALUES (action.old_id, action.old_name, action.old_datetime);
            END IF;
        END LOOP;

        DELETE FROM LoggingForTestTable2 
        WHERE p_datetime < datetime;
    END restore_data_in_TestTable2;

    PROCEDURE restore_data_in_TestTable2(p_interval INTERVAL DAY TO SECOND) AS
        v_current_time TIMESTAMP;
        v_target_time TIMESTAMP;
    BEGIN
        SELECT CURRENT_TIMESTAMP INTO v_current_time FROM DUAL;
        v_target_time := v_current_time - p_interval - INTERVAL '0 03:00:00.000000' DAY TO SECOND;
        restore_data_in_TestTable2(TO_CHAR(v_target_time));
    END restore_data_in_TestTable2;


    PROCEDURE restore_data_in_TestTable3(p_datetime VARCHAR) AS
    BEGIN
        FOR action IN (SELECT * FROM LoggingForTestTable3 WHERE p_datetime < datetime ORDER BY id DESC)
        LOOP
            IF action.operation = 'INSERT' THEN
                DELETE FROM TestTable3 
                WHERE id = action.new_id;
            END IF;
            
            IF action.operation = 'UPDATE' THEN
                UPDATE TestTable3 
                SET id = action.old_id, name = action.old_name, fk_id = action.old_fk_id
                WHERE id = action.new_id;
            END IF;
            
            IF action.operation = 'DELETE' THEN
                INSERT INTO TestTable3 (id, name, fk_id) 
                VALUES (action.old_id, action.old_name, action.old_fk_id);
            END IF;
        END LOOP;

        DELETE FROM LoggingForTestTable3
        WHERE p_datetime < datetime;
    END restore_data_in_TestTable3;

    PROCEDURE restore_data_in_TestTable3(p_interval INTERVAL DAY TO SECOND) AS
        v_current_time TIMESTAMP;
        v_target_time TIMESTAMP;
    BEGIN
        SELECT CURRENT_TIMESTAMP INTO v_current_time FROM DUAL;
        v_target_time := v_current_time - p_interval - INTERVAL '0 03:00:00.000000' DAY TO SECOND;
        restore_data_in_TestTable3(TO_CHAR(v_target_time));
    END restore_data_in_TestTable3;
END restore_package;
/


-- 4
CREATE OR REPLACE DIRECTORY my_directory AS '/opt/oracle';
GRANT READ, WRITE ON DIRECTORY my_directory TO PUBLIC; 

CREATE OR REPLACE PACKAGE create_report_package AS
    FUNCTION create_report(title VARCHAR2, insert_count NUMBER, update_count NUMBER, delete_count NUMBER) RETURN VARCHAR2; 
    PROCEDURE create_report_for_TestTable1(p_datetime TIMESTAMP);
    PROCEDURE create_report_for_TestTable1;
    PROCEDURE create_report_for_TestTable2(p_datetime TIMESTAMP);
    PROCEDURE create_report_for_TestTable2;
    PROCEDURE create_report_for_TestTable3(p_datetime TIMESTAMP);
    PROCEDURE create_report_for_TestTable3;
END create_report_package;
/

CREATE OR REPLACE PACKAGE BODY create_report_package AS
    FUNCTION create_report (title IN VARCHAR2, insert_count IN NUMBER, update_count IN NUMBER, delete_count IN NUMBER) 
    RETURN VARCHAR2 IS
        result VARCHAR(4000);
    BEGIN
        result := '<!DOCTYPE html>' || CHR(10) ||
                  '<html lang="en">' || CHR(10) ||
                  '<head>' || CHR(10) ||
                  '    <meta charset="UTF-8">' || CHR(10) ||
                  '    <meta name="viewport" content="width=device-width, initial-scale=1.0">' || CHR(10) ||
                  '    <title>Report</title>' || CHR(10) ||
                  '    <style>' || CHR(10) ||
                  '        table {' || CHR(10) ||
                  '            border-collapse: collapse;' || CHR(10) ||
                  '            width: 50%;' || CHR(10) ||
                  '        }' || CHR(10) ||
                  '        th, td {' || CHR(10) ||
                  '            border: 1px solid black;' || CHR(10) ||
                  '            padding: 8px;' || CHR(10) ||
                  '            text-align: left;' || CHR(10) ||
                  '        }' || CHR(10) ||
                  '        th {' || CHR(10) ||
                  '            background-color: #f2f2f2;' || CHR(10) ||
                  '        }' || CHR(10) ||
                  '    </style>' || CHR(10) ||
                  '</head>' || CHR(10) ||
                  '<body>' || CHR(10) ||
                  '<h2>' || title || '</h2>' || CHR(10) ||
                  '<table>' || CHR(10) ||
                  '    <tr>' || CHR(10) ||
                  '        <th>Operation</th>' || CHR(10) ||
                  '        <th>Count</th>' || CHR(10) ||
                  '    </tr>' || CHR(10) ||
                  '    <tr>' || CHR(10) ||
                  '        <td>INSERT</td>' || CHR(10) ||
                  '        <td>' || insert_count || '</td>' || CHR(10) ||
                  '    </tr>' || CHR(10) ||
                  '    <tr>' || CHR(10) ||
                  '        <td>UPDATE</td>' || CHR(10) ||
                  '        <td>' || update_count || '</td>' || CHR(10) ||
                  '    </tr>' || CHR(10) ||
                  '    <tr>' || CHR(10) ||
                  '        <td>DELETE</td>' || CHR(10) ||
                  '        <td>' || delete_count || '</td>' || CHR(10) ||
                  '    </tr>' || CHR(10) ||
                  '</table>' || CHR(10) ||
                  '</body>' || CHR(10) ||
                  '</html>' || CHR(10);

        DBMS_OUTPUT.PUT_LINE(result);
        RETURN result;
    END create_report;


    PROCEDURE create_report_for_TestTable1(p_datetime TIMESTAMP) AS
        v_file_handle UTL_FILE.FILE_TYPE;
        report VARCHAR2(4000);
        title VARCHAR2(100);
        insert_count NUMBER;
        update_count NUMBER;
        delete_count NUMBER;
        result VARCHAR(4000);
    BEGIN
        title := 'TestTable1 since ' || p_datetime;
        SELECT COUNT(*) INTO insert_count FROM LoggingForTestTable1 WHERE operation = 'INSERT' AND p_datetime <= datetime;
        SELECT COUNT(*) INTO update_count FROM LoggingForTestTable1 WHERE operation = 'UPDATE' AND p_datetime <= datetime;
        SELECT COUNT(*) INTO delete_count FROM LoggingForTestTable1 WHERE operation = 'DELETE' AND p_datetime <= datetime;

        result := create_report(title, insert_count, update_count, delete_count);

        v_file_handle := UTL_FILE.FOPEN('MY_DIRECTORY', 'report.html', 'W');
        UTL_FILE.PUT_LINE(v_file_handle, result);
        UTL_FILE.FCLOSE(v_file_handle);
    END create_report_for_TestTable1;

    PROCEDURE create_report_for_TestTable1 AS
        v_file_handle UTL_FILE.FILE_TYPE;
        v_file_text CLOB;
        v_pattern VARCHAR2(100) := 'since ([^<]+)';
        v_match VARCHAR2(100);
        v_line_number NUMBER := 1;
    BEGIN
        v_file_handle := UTL_FILE.FOPEN('MY_DIRECTORY', 'report.html', 'r');    
        LOOP
            UTL_FILE.GET_LINE(v_file_handle, v_file_text);
            IF v_line_number = 23 THEN
                EXIT;
            END IF;
            v_line_number := v_line_number + 1;
        END LOOP;  
        UTL_FILE.FCLOSE(v_file_handle);
        
        v_match := REGEXP_SUBSTR(v_file_text, v_pattern, 1, 1, NULL, 1);
        create_report_for_TestTable1(v_match);
    END create_report_for_TestTable1;


    PROCEDURE create_report_for_TestTable2(p_datetime TIMESTAMP) AS
        v_file_handle UTL_FILE.FILE_TYPE;
        report VARCHAR2(4000);
        title VARCHAR2(100);
        insert_count NUMBER;
        update_count NUMBER;
        delete_count NUMBER;
        result VARCHAR(4000);
    BEGIN
        title := 'TestTable2 since ' || p_datetime;
        SELECT COUNT(*) INTO insert_count FROM LoggingForTestTable2 WHERE operation = 'INSERT' AND p_datetime <= datetime;
        SELECT COUNT(*) INTO update_count FROM LoggingForTestTable2 WHERE operation = 'UPDATE' AND p_datetime <= datetime;
        SELECT COUNT(*) INTO delete_count FROM LoggingForTestTable2 WHERE operation = 'DELETE' AND p_datetime <= datetime;

        result := create_report(title, insert_count, update_count, delete_count);

        v_file_handle := UTL_FILE.FOPEN('MY_DIRECTORY', 'report.html', 'W');
        UTL_FILE.PUT_LINE(v_file_handle, result);
        UTL_FILE.FCLOSE(v_file_handle);
    END create_report_for_TestTable2;

    PROCEDURE create_report_for_TestTable2 AS
        v_file_handle UTL_FILE.FILE_TYPE;
        v_file_text CLOB;
        v_pattern VARCHAR2(100) := 'since ([^<]+)';
        v_match VARCHAR2(100);
        v_line_number NUMBER := 1;
    BEGIN
        v_file_handle := UTL_FILE.FOPEN('MY_DIRECTORY', 'report.html', 'r');    
        LOOP
            UTL_FILE.GET_LINE(v_file_handle, v_file_text);
            IF v_line_number = 23 THEN
                EXIT;
            END IF;
            v_line_number := v_line_number + 1;
        END LOOP;  
        UTL_FILE.FCLOSE(v_file_handle);
        
        v_match := REGEXP_SUBSTR(v_file_text, v_pattern, 1, 1, NULL, 1);
        create_report_for_TestTable2(v_match);
    END create_report_for_TestTable2;


    PROCEDURE create_report_for_TestTable3(p_datetime TIMESTAMP) AS
        v_file_handle UTL_FILE.FILE_TYPE;
        report VARCHAR2(4000);
        title VARCHAR2(100);
        insert_count NUMBER;
        update_count NUMBER;
        delete_count NUMBER;
        result VARCHAR(4000);
    BEGIN
        title := 'TestTable3 since ' || p_datetime;
        SELECT COUNT(*) INTO insert_count FROM LoggingForTestTable3 WHERE operation = 'INSERT' AND p_datetime <= datetime;
        SELECT COUNT(*) INTO update_count FROM LoggingForTestTable3 WHERE operation = 'UPDATE' AND p_datetime <= datetime;
        SELECT COUNT(*) INTO delete_count FROM LoggingForTestTable3 WHERE operation = 'DELETE' AND p_datetime <= datetime;

        result := create_report(title, insert_count, update_count, delete_count);

        v_file_handle := UTL_FILE.FOPEN('MY_DIRECTORY', 'report.html', 'W');
        UTL_FILE.PUT_LINE(v_file_handle, result);
        UTL_FILE.FCLOSE(v_file_handle);
    END create_report_for_TestTable3;

    PROCEDURE create_report_for_TestTable3 AS
        v_file_handle UTL_FILE.FILE_TYPE;
        v_file_text CLOB;
        v_pattern VARCHAR2(100) := 'since ([^<]+)';
        v_match VARCHAR2(100);
        v_line_number NUMBER := 1;
    BEGIN
        v_file_handle := UTL_FILE.FOPEN('MY_DIRECTORY', 'report.html', 'r');  
        LOOP
            UTL_FILE.GET_LINE(v_file_handle, v_file_text);
            IF v_line_number = 23 THEN
                EXIT;
            END IF;
            v_line_number := v_line_number + 1;
        END LOOP;  
        UTL_FILE.FCLOSE(v_file_handle);
        
        v_match := REGEXP_SUBSTR(v_file_text, v_pattern, 1, 1, NULL, 1);
        create_report_for_TestTable3(v_match);
    END create_report_for_TestTable3;
END create_report_package;
/


-- for testing
BEGIN
    INSERT INTO TestTable1 VALUES (1, 'test1.1', 11);
    INSERT INTO TestTable1 VALUES (2, 'test1.2', 11);
    INSERT INTO TestTable1 VALUES (3, 'test1.3', 11);
    INSERT INTO TestTable1 VALUES (4, 'test1.4', 11);

    UPDATE TestTable1
    SET value = 12
    WHERE value = 11;

    DELETE FROM TestTable1 WHERE value = 12;
END;
/

BEGIN
    restore_package.restore_data_in_TestTable1('16.04.24 08:16:56,307977000');
END;
/

BEGIN
    restore_package.restore_data_in_TestTable1(INTERVAL '0 00:01:40.000000' DAY TO SECOND);
END;
/

BEGIN
    create_report_package.create_report_for_TestTable1('15.04.24 20:38:09,819437000');
END;
/

BEGIN
    INSERT INTO TestTable1 VALUES (5, 'test1.5', 11);
    create_report_package.create_report_for_TestTable1;
END;
/


BEGIN
    INSERT INTO TestTable2 VALUES (1, 'test1.1', '15.04.24 20:38:09,819437000');
    INSERT INTO TestTable2 VALUES (2, 'test1.2', '15.04.24 20:38:09,819437000');
    INSERT INTO TestTable2 VALUES (3, 'test1.3', '15.04.24 20:38:09,819437000');
    INSERT INTO TestTable2 VALUES (4, 'test1.4', '15.04.24 20:38:09,819437000');

    UPDATE TestTable2
    SET datetime = '15.04.24 20:39:09,819437000'
    WHERE datetime = '15.04.24 20:38:09,819437000';

    DELETE FROM TestTable2 WHERE datetime = '15.04.24 20:39:09,819437000';
END;
/

BEGIN
    restore_package.restore_data_in_TestTable2('16.04.24 08:16:56,307977000');
END;
/

BEGIN
    restore_package.restore_data_in_TestTable2(INTERVAL '0 00:01:40.000000' DAY TO SECOND);
END;
/

BEGIN
    create_report_package.create_report_for_TestTable2('16.04.24 07:49:24,315889000');
END;
/

BEGIN
    INSERT INTO TestTable2 VALUES (5, 'test1.5', '15.04.24 20:38:09,819437000');
    create_report_package.create_report_for_TestTable2;
END;
/


BEGIN
    INSERT INTO TestTable2 VALUES (1, 'test1.1', '15.04.24 20:38:09,819437000');
    INSERT INTO TestTable2 VALUES (2, 'test1.2', '15.04.24 20:38:09,819437000');

    INSERT INTO TestTable3 VALUES (1, 'test1.1', 1);
    INSERT INTO TestTable3 VALUES (2, 'test1.2', 1);
    INSERT INTO TestTable3 VALUES (3, 'test1.3', 1);
    INSERT INTO TestTable3 VALUES (4, 'test1.4', 1);

    UPDATE TestTable3
    SET fk_id = 2
    WHERE fk_id = 1;

    DELETE FROM TestTable3 WHERE fk_id = 2;
END;
/

BEGIN
    restore_package.restore_data_in_TestTable3('16.04.24 08:16:56,307977000');
END;
/

BEGIN
    restore_package.restore_data_in_TestTable3(INTERVAL '0 00:01:40.000000' DAY TO SECOND);
END;
/

BEGIN
    create_report_package.create_report_for_TestTable3('16.04.24 07:54:24,315889000');
END;
/

BEGIN
    INSERT INTO TestTable3 VALUES (5, 'test1.5', 1);
    create_report_package.create_report_for_TestTable3;
END;
/

BEGIN
    restore_package.restore_data_in_TestTable2(INTERVAL '0 01:00:00.000000' DAY TO SECOND);
END;
/

-- 0
CREATE TABLE trigger_status (
    trigger_name VARCHAR2(100),
    is_enabled   NUMBER(1)
);

INSERT INTO trigger_status(trigger_name, is_enabled) VALUES ('check_update_new_data_in_groups', 1);
INSERT INTO trigger_status(trigger_name, is_enabled) VALUES ('update_group_count', 1);

CREATE OR REPLACE PROCEDURE disable_trigger(current_trigger_name IN VARCHAR2) IS
BEGIN
    UPDATE trigger_status
    SET is_enabled = 0
    WHERE trigger_name = current_trigger_name;
END;
/

CREATE OR REPLACE PROCEDURE enable_trigger(current_trigger_name IN VARCHAR2) IS
BEGIN
    UPDATE trigger_status
    SET is_enabled = 1
    WHERE trigger_name = current_trigger_name;
END;
/

CREATE OR REPLACE FUNCTION is_trigger_enabled(current_trigger_name IN VARCHAR2) RETURN BOOLEAN IS
    current_status NUMBER(1);
BEGIN
    SELECT is_enabled INTO current_status
    FROM trigger_status
    WHERE trigger_name = current_trigger_name;

    RETURN current_status = 1;
END;
/


-- 1
CREATE TABLE STUDENTS (
    ID NUMBER PRIMARY KEY,
    NAME VARCHAR2(100),
    GROUP_ID NUMBER
);

COMMENT ON COLUMN STUDENTS.ID IS 'Student code';
COMMENT ON COLUMN STUDENTS.NAME IS 'Student name';
COMMENT ON COLUMN STUDENTS.GROUP_ID IS 'Group code';

CREATE TABLE GROUPS (
    ID NUMBER PRIMARY KEY,
    NAME VARCHAR2(100),
    C_VAL NUMBER
);

COMMENT ON COLUMN GROUPS.ID IS 'Group code';
COMMENT ON COLUMN GROUPS.NAME IS 'Group name';
COMMENT ON COLUMN GROUPS.C_VAL IS 'The number of students in the group';


-- 2
CREATE OR REPLACE TRIGGER check_insert_new_data_in_students
BEFORE INSERT ON STUDENTS
FOR EACH ROW
DECLARE
    record_count NUMBER;
BEGIN
    IF :NEW.ID IS NULL THEN
        SELECT NVL(MAX(ID), 0) + 1 INTO :NEW.ID FROM STUDENTS;
    ELSE
        SELECT COUNT(*) INTO record_count FROM STUDENTS WHERE ID = :NEW.ID;
        IF record_count > 0 THEN
            RAISE_APPLICATION_ERROR(-20001, 'ID is not UNIQUE!');
        END IF;  
    END IF;

    IF :NEW.NAME IS NULL THEN
        RAISE_APPLICATION_ERROR(-20001, 'NAME is NULL!');
    END IF;

    IF :NEW.GROUP_ID IS NULL THEN
        RAISE_APPLICATION_ERROR(-20001, 'GROUP_ID is NULL!');
    ELSE
        SELECT COUNT(*) INTO record_count FROM GROUPS WHERE ID = :NEW.GROUP_ID;
        IF record_count = 0 THEN
            RAISE_APPLICATION_ERROR(-20001, 'Group with this GROUP_ID does not exist!');
        END IF; 
    END IF;
END;
/

CREATE OR REPLACE TRIGGER check_update_new_data_in_students
BEFORE UPDATE ON STUDENTS
FOR EACH ROW
DECLARE
    record_count NUMBER;
BEGIN
    IF :NEW.ID IS NOT NULL THEN
        SELECT COUNT(*) INTO record_count FROM STUDENTS WHERE ID = :NEW.ID;
        IF record_count > 0 THEN
            RAISE_APPLICATION_ERROR(-20001, 'ID is not UNIQUE!');
        END IF;  
    END IF;

    IF :NEW.GROUP_ID IS NOT NULL THEN
        SELECT COUNT(*) INTO record_count FROM GROUPS WHERE ID = :NEW.GROUP_ID;
        IF record_count IS NULL THEN
            RAISE_APPLICATION_ERROR(-20001, 'Group with this GROUP_ID does not exist!');
        END IF; 
    END IF;
END;
/

CREATE OR REPLACE TRIGGER check_insert_new_data_in_groups
BEFORE INSERT ON GROUPS
FOR EACH ROW
DECLARE
    record_count NUMBER;
BEGIN
    IF :NEW.ID IS NULL THEN
        SELECT NVL(MAX(ID), 0) + 1 INTO :NEW.ID FROM GROUPS;
    ELSE
        SELECT COUNT(*) INTO record_count FROM GROUPS WHERE ID = :NEW.ID;
        IF record_count > 0 THEN
            RAISE_APPLICATION_ERROR(-20001, 'ID is not UNIQUE!');
        END IF;  
    END IF;

    IF :NEW.NAME IS NULL THEN
        RAISE_APPLICATION_ERROR(-20001, 'NAME is NULL!');
    ELSE
        SELECT COUNT(*) INTO record_count FROM GROUPS WHERE NAME = :NEW.NAME;
        IF record_count > 0 THEN
            RAISE_APPLICATION_ERROR(-20001, 'NAME is not UNIQUE!');
        END IF;  
    END IF;

    IF :NEW.C_VAL < 0 THEN
        RAISE_APPLICATION_ERROR(-20001, 'C_VAL is negative!');
    END IF;
END;
/

CREATE OR REPLACE TRIGGER check_update_new_data_in_groups
BEFORE UPDATE ON GROUPS
FOR EACH ROW
DECLARE
    record_count NUMBER;
    is_enabled NUMBER;
BEGIN
    SELECT is_trigger_enabled('check_update_new_data_in_groups') INTO is_enabled FROM DUAL;
    IF is_enabled = 1 THEN
        IF :NEW.ID IS NOT NULL THEN
            SELECT COUNT(*) INTO record_count FROM GROUPS WHERE ID = :NEW.ID;
            IF record_count > 0 THEN
                RAISE_APPLICATION_ERROR(-20001, 'ID is not UNIQUE!');
            END IF;  
        END IF;
    
        IF :NEW.NAME IS NOT NULL THEN
            SELECT COUNT(*) INTO record_count FROM GROUPS WHERE NAME = :NEW.NAME;
            IF record_count > 0 THEN
                RAISE_APPLICATION_ERROR(-20001, 'NAME is not UNIQUE!');
            END IF;  
        END IF;
    
        IF :NEW.C_VAL < 0 THEN
            RAISE_APPLICATION_ERROR(-20001, 'C_VAL is negative!');
        END IF;
    END IF;
END;
/


-- 3
CREATE OR REPLACE TRIGGER groups_students_cascade_delete
BEFORE DELETE ON GROUPS
FOR EACH ROW
BEGIN
    disable_trigger('update_group_count');

    FOR student IN (SELECT ID FROM STUDENTS WHERE GROUP_ID = :OLD.ID)
    LOOP
        DELETE FROM STUDENTS WHERE ID = student.ID;
    END LOOP;

    enable_trigger('update_group_count');
END;
/


-- 4
CREATE TABLE STUDENTS_LOGS (
    ID NUMBER PRIMARY KEY,
    ACTION VARCHAR2(20),
    STUDENT_ID NUMBER,
    NAME VARCHAR2(100),
    GROUP_ID NUMBER,
    LOG_DATE TIMESTAMP
);

CREATE OR REPLACE TRIGGER add_students_logs_actions
AFTER INSERT OR UPDATE OR DELETE ON STUDENTS
FOR EACH ROW
DECLARE
    max_id NUMBER;
BEGIN
    SELECT NVL(MAX(ID), 0) + 1 INTO max_id FROM STUDENTS_LOGS;

    IF INSERTING THEN
        INSERT INTO STUDENTS_LOGS (ID, ACTION, STUDENT_ID, NAME, GROUP_ID, LOG_DATE)
        VALUES (max_id, 'INSERT', :NEW.ID, :NEW.NAME, :NEW.GROUP_ID, SYSTIMESTAMP);
    ELSIF UPDATING THEN
        INSERT INTO STUDENTS_LOGS (ID, ACTION, STUDENT_ID, NAME, GROUP_ID, LOG_DATE)
        VALUES (max_id, 'UPDATE', :NEW.ID, :NEW.NAME, :NEW.GROUP_ID, SYSTIMESTAMP);
    ELSIF DELETING THEN
        INSERT INTO STUDENTS_LOGS (ID, ACTION, STUDENT_ID, NAME, GROUP_ID, LOG_DATE)
        VALUES (max_id, 'DELETE', :OLD.ID, :OLD.NAME, :OLD.GROUP_ID, SYSTIMESTAMP);
    END IF;
END;
/


-- 5
CREATE OR REPLACE PROCEDURE restore_students_data (restore_date TIMESTAMP)
AS
BEGIN
    DELETE FROM STUDENTS;

    FOR student IN (SELECT STUDENT_ID, NAME, GROUP_ID
        FROM (
            SELECT STUDENT_ID, NAME, GROUP_ID,
                ROW_NUMBER() OVER (PARTITION BY STUDENT_ID ORDER BY LOG_DATE DESC) AS row_number
            FROM STUDENTS_LOGS
            WHERE LOG_DATE <= restore_date
        )
        WHERE row_number = 1)
    LOOP
            INSERT INTO STUDENTS (ID, NAME, GROUP_ID) VALUES
            (student.STUDENT_ID, student.NAME, student.GROUP_ID);
    END LOOP;
    COMMIT;
END;
/


-- 6
CREATE OR REPLACE TRIGGER update_group_count
AFTER INSERT OR UPDATE OR DELETE ON STUDENTS
DECLARE
    is_enabled NUMBER;
BEGIN
    SELECT is_trigger_enabled('update_group_count') INTO is_enabled FROM DUAL;
    IF is_enabled = 1 THEN
        disable_trigger('check_update_new_data_in_groups');
        
        FOR group_ IN (SELECT GROUP_ID, COUNT(*) AS student_count FROM STUDENTS GROUP BY GROUP_ID)
        LOOP
            UPDATE GROUPS
            SET C_VAL = group_.student_count
            WHERE ID = group_.GROUP_ID;
        END LOOP;

        enable_trigger('check_update_new_data_in_groups');
    END IF;
END;
/


-- Test
BEGIN
    INSERT INTO GROUPS (NAME) VALUES ('GROUP 1');
    INSERT INTO GROUPS (NAME) VALUES ('GROUP 2');
    INSERT INTO GROUPS (NAME) VALUES ('GROUP 3');

    INSERT INTO STUDENTS (NAME, GROUP_ID) VALUES ('Student 1.1', 1);
    INSERT INTO STUDENTS (NAME, GROUP_ID) VALUES ('Student 1.2', 1);
    INSERT INTO STUDENTS (NAME, GROUP_ID) VALUES ('Student 1.3', 1);
    INSERT INTO STUDENTS (NAME, GROUP_ID) VALUES ('Student 2.1', 2);
    INSERT INTO STUDENTS (NAME, GROUP_ID) VALUES ('Student 2.2', 2);
    INSERT INTO STUDENTS (NAME, GROUP_ID) VALUES ('Student 2.3', 2);
    INSERT INTO STUDENTS (NAME, GROUP_ID) VALUES ('Student 3.1', 3);
    INSERT INTO STUDENTS (NAME, GROUP_ID) VALUES ('Student 3.2', 3);
    INSERT INTO STUDENTS (NAME, GROUP_ID) VALUES ('Student 3.3', 3);
END;

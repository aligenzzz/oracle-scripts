-- 1
CREATE TABLE MyTable (
    id NUMBER PRIMARY KEY,
    value NUMBER NOT NULL
);


-- 2
DECLARE
    loop_counter NUMBER := 1;
BEGIN 
    WHILE loop_counter <= 10000 LOOP
        INSERT INTO MyTable (id, value)
        VALUES (loop_counter, FLOOR(DBMS_RANDOM.VALUE(1, 1000)));
        loop_counter := loop_counter + 1;
    END LOOP;
    COMMIT;
END;
-- for cleaning table
DELETE FROM MyTable;


-- 3
CREATE OR REPLACE FUNCTION check_values_odd_even
RETURN VARCHAR2 IS
    even_values_count NUMBER := 0;
    odd_values_count NUMBER := 0;
BEGIN
    SELECT COUNT(*) INTO even_values_count
    FROM MyTable
    WHERE MOD(value, 2) = 0;

    SELECT COUNT(*) INTO odd_values_count
    FROM MyTable
    WHERE MOD(value, 2) = 1; 

    IF even_values_count > odd_values_count THEN
        RETURN 'TRUE';
    ELSIF even_values_count < odd_values_count THEN
        RETURN 'FALSE';
    ELSE
        RETURN 'EQUAL';
    END IF;
END check_values_odd_even;
-- for executing function
SELECT check_values_odd_even() AS result FROM DUAL;


-- 4 
CREATE OR REPLACE FUNCTION generate_insert_command(
    record_id IN NUMBER
) RETURN VARCHAR2 IS
    insert_command VARCHAR2(1000);
    record_value NUMBER;
BEGIN
    SELECT value INTO record_value FROM MyTable WHERE id = record_id;
    insert_command := 'INSERT INTO MyTable(id, value) VALUES (' || record_id || ', ' || record_value || ')';
    DBMS_OUTPUT.PUT_LINE(insert_command);
    RETURN insert_command;
END generate_insert_command;
-- for executing function
SELECT generate_insert_command(1) AS result FROM DUAL;


-- 5
CREATE OR REPLACE PROCEDURE insert_value(
    record_id IN NUMBER,
    record_value IN NUMBER
) AS
BEGIN
    INSERT INTO MyTable (id, value)
    VALUES (record_id, record_value);
    COMMIT;
END insert_value;

CREATE OR REPLACE PROCEDURE update_value(
    record_id IN NUMBER,
    record_value IN NUMBER
) AS
BEGIN
    UPDATE MyTable
    SET value = record_value
    WHERE id = record_id;
    COMMIT;
END update_value;

CREATE OR REPLACE PROCEDURE delete_value(
    record_id IN NUMBER
) AS
BEGIN
    DELETE FROM MyTable
    WHERE id = record_id;
    COMMIT;
END delete_value;

BEGIN
    insert_value(1, 100); 
    update_value(1, 200); 
    delete_value(1);
END;


-- 6
CREATE OR REPLACE FUNCTION calculate_annual_reward(
    monthly_salary IN NUMBER,
    annual_bonus_percent IN NUMBER
) RETURN NUMBER IS
    result NUMBER;
BEGIN
    IF monthly_salary <= 0 OR annual_bonus_percent < 0 THEN
        RAISE_APPLICATION_ERROR(-20001, 'Invalid input values');
    END IF;

    result := (1 + annual_bonus_percent / 100.0) * 12.0 * monthly_salary;

    RETURN result;
EXCEPTION
    WHEN OTHERS THEN
        RETURN NULL;
END calculate_annual_reward;
-- for executing function
SELECT calculate_annual_reward(10, 23) AS result FROM DUAL;
<?php

use Phinx\Migration\AbstractMigration;

// This is untested, as it relies upon checking what/how things have to be quoted and what PHP will attempt
// to interpolate as variables.

class LoggingForPerformanceTuning extends AbstractMigration
{
    /**
     * Change Method.
     *
     * Write your reversible migrations using this method.
     *
     * More information on writing migrations is available here:
     * http://docs.phinx.org/en/latest/migrations.html#the-abstractmigration-class
     *
     * The following commands can be used in this method and Phinx will
     * automatically reverse them when rolling back:
     *
     *    createTable
     *    renameTable
     *    addColumn
     *    renameColumn
     *    addIndex
     *    addForeignKey
     *
     * Remember to call "create()" or "update()" and NOT "save()" when working
     * with the Table class.
     */
    public function up()
    {
        $createDB = "CREATE DATABASE IF NOT EXISTS ps_history";
        $useDB = "USE ps_history";
        $createPhinxLog = "CREATE TABLE IF NOT EXISTS `ps_history`.`phinxlog` (
              `version` bigint(20) NOT NULL,
              `start_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
              `end_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
              PRIMARY KEY (`version`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8";
        $psSetup = "CREATE PROCEDURE ps_history.setup()
                MODIFIES SQL DATA
                SQL SECURITY DEFINER
                BEGIN
                    DECLARE v_done BOOLEAN DEFAULT FALSE;
                    DECLARE v_table VARCHAR(64);
                    DECLARE v_sql TEXT;
                    DECLARE table_cur CURSOR
                    FOR
                    SELECT CONCAT('CREATE TABLE ps_history.', TABLE_NAME, '( ', GROUP_CONCAT(CONCAT(COLUMN_NAME, ' ', COLUMN_TYPE, IF(CHARACTER_SET_NAME IS NOT NULL,CONCAT(' CHARACTER SET ', CHARACTER_SET_NAME),''),if(COLLATION_NAME IS NOT NULL,CONCAT(' COLLATE ', COLLATION_NAME),'')) ORDER BY ORDINAL_POSITION SEPARATOR ',\n') , ',server_id int unsigned,\nts datetime(5),KEY(ts) ) PARTITION BY KEY(ts) PARTITIONS 7') as create_tbl,
                           TABLE_NAME
                      FROM INFORMATION_SCHEMA.COLUMNS
                     WHERE TABLE_SCHEMA='performance_schema'
                     GROUP BY TABLE_NAME;

                    DECLARE CONTINUE HANDLER FOR SQLSTATE '02000' SET v_done=TRUE;

                    SET group_concat_max_len := @@max_allowed_packet;

                    SET v_done = FALSE;
                    OPEN table_cur;
                    tableLoop: LOOP

                      FETCH table_cur
                        INTO v_sql,
                             v_table;

                      IF v_done THEN
                        CLOSE table_cur;
                        LEAVE tableLoop;
                      END IF;

                      SET @v_sql := CONCAT('DROP TABLE IF EXISTS ps_history.', v_table,'');
                      PREPARE drop_stmt FROM @v_sql;
                      EXECUTE drop_stmt;
                      DEALLOCATE PREPARE drop_stmt;

                      SET @v_sql := v_sql;
                      PREPARE create_stmt FROM @v_sql;
                      EXECUTE create_stmt;
                      DEALLOCATE PREPARE create_stmt;
                    END LOOP;

                    CREATE TABLE ps_history.psh_settings(variable varchar(64), key(variable), value varchar(64)) engine = InnoDB;
                    INSERT INTO ps_history.psh_settings VALUES ('interval', '30');
                    INSERT INTO ps_history.psh_settings VALUES ('retention_period', '1 WEEK');
                    CREATE TABLE ps_history.psh_last_refresh(last_refreshed_at DATETIME(6) NOT NULL) engine=InnoDB;
                END";
        $truncate = "CREATE PROCEDURE ps_history.truncate_tables()
            MODIFIES SQL DATA
            SQL SECURITY DEFINER
            BEGIN
                DECLARE v_done BOOLEAN DEFAULT FALSE;
                DECLARE v_sql TEXT;

                DECLARE table_cur CURSOR FOR
                SELECT CONCAT('TRUNCATE TABLE ps_history.', table_name)
                  FROM INFORMATION_SCHEMA.TABLES
                 WHERE table_schema = 'ps_history'
                   AND table_name NOT LIKE 'psh%';

                DECLARE CONTINUE HANDLER FOR SQLSTATE '02000' SET v_done=TRUE;

                SET v_done = FALSE;
                OPEN table_cur;
                tableLoop: LOOP

                    FETCH table_cur
                     INTO v_sql;

                    IF v_done THEN
                        CLOSE table_cur;
                        LEAVE tableLoop;
                    END IF;

                    SET @v_sql := v_sql;
                    PREPARE truncate_stmt FROM @v_sql;
                    EXECUTE truncate_stmt;
                    DEALLOCATE PREPARE truncate_stmt;

                END LOOP;

            END";
        $historyCleanUp = "
            CREATE PROCEDURE ps_history.cleanup_history(v_interval VARCHAR(64))
            MODIFIES SQL DATA
            SQL SECURITY DEFINER
            BEGIN
                DECLARE v_done BOOLEAN DEFAULT FALSE;
                DECLARE v_sql TEXT;

                DECLARE table_cur CURSOR FOR
                SELECT CONCAT('DELETE FROM ps_history.', table_name, ' WHERE ts < NOW() - INTERVAL ', v_interval)
                  FROM INFORMATION_SCHEMA.TABLES
                 WHERE table_schema = 'ps_history'
                   AND table_name NOT LIKE 'psh%';

                DECLARE CONTINUE HANDLER FOR SQLSTATE '02000' SET v_done=TRUE;

                SET v_done = FALSE;
                OPEN table_cur;
                tableLoop: LOOP

                    FETCH table_cur
                     INTO v_sql;

                    IF v_done THEN
                        CLOSE table_cur;
                        LEAVE tableLoop;
                    END IF;

                    SET @v_sql := v_sql;
                    PREPARE truncate_stmt FROM @v_sql;
                    EXECUTE truncate_stmt;
                    DEALLOCATE PREPARE truncate_stmt;
                END LOOP;
            END
        ";
        $autoCleanup = "
            CREATE PROCEDURE ps_history.auto_cleanup_history()
            MODIFIES SQL DATA
            SQL SECURITY DEFINER
            BEGIN
                DECLARE v_retention_period VARCHAR(64);

                SELECT value
                  INTO v_retention_period
                  FROM ps_history.psh_settings
                 WHERE variable = 'retention_period';

                CALL ps_history.cleanup_history(v_retention_period);

            END";
        $setCollectInterval = "
            CREATE PROCEDURE ps_history.set_collect_interval(v_interval INT)
            MODIFIES SQL DATA
            SQL SECURITY DEFINER
            BEGIN
                START TRANSACTION;
                UPDATE ps_history.psh_settings SET value = v_interval WHERE variable = 'interval';
                SELECT 'Updated interval setting' as message;
                COMMIT;
            END"
        ;
        $setRetentionHistory = "
            CREATE PROCEDURE ps_history.set_retention_period(v_retention_period VARCHAR(64))
            MODIFIES SQL DATA
            SQL SECURITY DEFINER
            BEGIN
                START TRANSACTION;
                CALL ps_history.test_retention_period(v_retention_period);
                UPDATE ps_history.psh_settings SET value = v_retention_period WHERE variable = 'retention_period';
                SELECT 'Updated retention period setting' as message;
                COMMIT;
            END
        ";
        $this->execute($createDB);
        $this->execute($useDB);
        $this->execute($createPhinxLog);
        $this->execute($psSetup);
        $this->execute($truncate);
        $this->execute($historyCleanUp);
        $this->execute($autoCleanup);
        $this->execute($setCollectInterval);
        $this->execute($setRetentionHistory);

        $this->execute("
        CREATE PROCEDURE ps_history.collect()
        MODIFIES SQL DATA
        SQL SECURITY DEFINER
        BEGIN
            DECLARE v_done BOOLEAN DEFAULT FALSE;
            DECLARE v_sql TEXT;
            DECLARE v_count INT;
            DECLARE v_created_table BOOLEAN DEFAULT FALSE;
            DECLARE v_i INTEGER DEFAULT 0;
            DECLARE v_col INTEGER DEFAULT 0;
            DECLARE v_table VARCHAR(64);
            DECLARE v_collist TEXT;
            DECLARE v_max INT DEFAULT 0;

            DECLARE table_cur CURSOR FOR
            SELECT table_name,
                   COUNT(*) cnt
              FROM INFORMATION_SCHEMA.COLUMNS
             WHERE table_schema = 'performance_schema'
             GROUP BY table_name
             ORDER BY count(*) DESC;

            DECLARE CONTINUE HANDLER FOR SQLSTATE '02000' SET v_done=TRUE;

            SET group_concat_max_len := @@max_allowed_packet;

            SELECT GET_LOCK('ps_snapshot_lock',0) INTO @have_lock;
            IF @have_lock = 1 THEN

                SET v_done = FALSE;
                OPEN table_cur;
                tableLoop: LOOP

                    FETCH table_cur
                     INTO v_table,
                          v_count;

                    IF v_done THEN
                        CLOSE table_cur;
                        LEAVE tableLoop;
                    END IF;

                    IF NOT v_created_table THEN
                        SET v_max := v_count;
                        SET v_sql = '';
                        SET v_created_table = TRUE;
                        SET v_col = 1;
        	        SET v_i := v_count;
                        WHILE(v_i >= 1) DO
                            IF v_sql != '' THEN
                                SET v_sql := CONCAT(v_sql, ',\n');
                            END IF;

                            SET v_sql = CONCAT(v_sql,'col',v_col,' TEXT');
                            SET v_i := v_i - 1;
                            SET v_col := v_col + 1;
                        END WHILE;

                        SET v_sql = CONCAT('CREATE TEMPORARY TABLE ps_history.snapshot(table_name varchar(64), server_id INT UNSIGNED, ts DATETIME(6), KEY(table_name),',v_sql,')');

                        SET @v_sql := v_sql;
                        PREPARE create_stmt FROM @v_sql;
                        EXECUTE create_stmt;
                        DEALLOCATE PREPARE create_stmt;
                        SET v_sql = '';
                    END IF;

                    IF v_sql != '' THEN
                        SET v_sql := CONCAT(v_sql, ' UNION ALL ');
                    END IF;

                    SELECT GROUP_CONCAT(column_name ORDER BY ORDINAL_POSITION SEPARATOR ', ')
                      INTO v_collist
                      FROM INFORMATION_SCHEMA.COLUMNS
                     WHERE table_schema = 'performance_schema'
                       AND table_name = v_table
                     GROUP BY table_name;

                    IF v_count < v_max THEN
                        SET v_collist := CONCAT(v_collist, REPEAT(\",NULL\", v_max - v_count));
                    END IF;

                    SET v_sql := CONCAT(v_sql, '(SELECT \'',v_table,'\',@@server_id,NOW(6),', v_collist,' FROM performance_schema.', v_table, ')');

                END LOOP;

                SET @v_sql := CONCAT('INSERT INTO ps_history.snapshot\n', v_sql);
                PREPARE insert_stmt FROM @v_sql;
                EXECUTE insert_stmt;
                DEALLOCATE PREPARE insert_stmt;

                SET v_done = FALSE;
                OPEN table_cur;
                tableLoop2: LOOP

                    FETCH table_cur
                      INTO v_table,
                           v_count;

                    IF v_done THEN
                        CLOSE table_cur;
                        LEAVE tableLoop2;
                    END IF;

                    SET v_i := 1;
                    SET v_sql = '';
                    WHILE(v_i <= v_count) DO
                        IF v_sql != '' THEN
                            SET v_sql := CONCAT(v_sql, ', ');
                        END IF;
                        SET v_sql := CONCAT(v_sql, 'col', v_i);
                        SET v_i := v_i + 1;
                    END WHILE;

                    SET @v_sql = CONCAT('INSERT INTO ps_history.', v_table, ' SELECT ', v_sql, ',server_id, ts FROM ps_history.snapshot where table_name = \'', v_table, '\'');
                    PREPARE insert_stmt FROM @v_sql;
                    EXECUTE insert_stmt;
                    DEALLOCATE PREPARE insert_stmt;

                END LOOP;

                DROP TABLE ps_history.snapshot;

                DELETE FROM ps_history.psh_last_refresh;
                INSERT INTO ps_history.psh_last_refresh VALUES (now());

                CALL ps_history.auto_cleanup_history();

            END IF;

            END"
        );

        $this->execute("
            CREATE PROCEDURE ps_history.collect_at_interval()
            MODIFIES SQL DATA
            SQL SECURITY DEFINER
            BEGIN
                DECLARE v_last_refresh DATETIME DEFAULT NULL;
                DECLARE v_seconds INT DEFAULT 0;
                DECLARE v_refresh_interval INT DEFAULT 30;

                SELECT value
                  INTO v_refresh_interval
                  FROM ps_history.psh_settings
                 WHERE variable = 'interval';

                SELECT IF(max(last_refreshed_at) IS NULL, NOW() - INTERVAL v_refresh_interval SECOND,max(last_refreshed_at))
                  INTO v_last_refresh
                  FROM ps_history.psh_last_refresh ;

                SET v_seconds = TO_SECONDS(NOW()) - TO_SECONDS(v_last_refresh);

                IF v_seconds >= v_refresh_interval THEN
                   CALL ps_history.collect();
                END IF;

            END");

            $this->execute("CREATE PROCEDURE ps_history.test_retention_period(v_interval VARCHAR(64))
            MODIFIES SQL DATA
            SQL SECURITY DEFINER
            BEGIN
                DECLARE v_bad BOOLEAN DEFAULT FALSE;
                DECLARE CONTINUE HANDLER FOR SQLSTATE '42000' SET v_bad=TRUE;
                SET @v_sql = CONCAT('SELECT NOW() - INTERVAL ', v_interval, ' INTO @discard FROM DUAL');
                PREPARE test_stmt FROM @v_sql;
                IF v_bad THEN
                    SIGNAL SQLSTATE '99999'
                    SET MESSAGE_TEXT = 'Invalid retention period.  Should be 1 DAY, 1 WEEK, 7200 SECOND, etc';
                END IF;
                EXECUTE test_stmt;
                DEALLOCATE PREPARE test_stmt;
            END");
        $this->execute("CREATE EVENT ps_history.snapshot_performance_schema
        ON SCHEDULE
        EVERY 1 SECOND
        ON COMPLETION PRESERVE
        ENABLE
        COMMENT 'Collect global performance_schema information'
        DO
        CALL ps_history.collect_at_interval()");

        $this->execute("call ps_history.setup()");
        $this->execute("CREATE TRIGGER trg_before_delete before DELETE ON ps_history.psh_settings
        FOR EACH ROW
        BEGIN
            SIGNAL SQLSTATE '99999'
            SET MESSAGE_TEXT = 'You may not delete rows in this table';
        END");

        $this->execute("
            CREATE TRIGGER trg_before_insert before INSERT ON ps_history.psh_settings
            FOR EACH ROW
            BEGIN
                SIGNAL SQLSTATE '99999'
                SET MESSAGE_TEXT = 'You may not insert rows in this table';
            END"
        );

        $this->execute("CREATE TRIGGER trg_before_update before UPDATE ON ps_history.psh_settings
        FOR EACH ROW
        BEGIN
            IF new.variable != 'interval' AND new.variable != 'retention_period' THEN
                SIGNAL SQLSTATE '99999'
                SET MESSAGE_TEXT = 'Only the interval and retention_period variables are supported at this time';
            END IF;
            IF new.variable = 'interval' AND CAST(new.value AS SIGNED) < 1 THEN
                SIGNAL SQLSTATE '99999'
                SET MESSAGE_TEXT = 'Interval must be greater than or equal to 1';
            END IF;


        END");

    }
    public function down()
    {
        $this->execute("DROP DATABASE IF EXISTS ps_history");
    }
}

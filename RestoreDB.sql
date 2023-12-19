
-- �������-��������: https://github.com/Tavalik/SQL_TScripts/tree/master

-- ���� ������
DECLARE @DBName as nvarchar(40) = 'db_name'
-- ����, �� �������� ���������� ������� ������ ��������� �����, � ������� '20160315 12:00:00'							
DECLARE @BackupTime as datetime = GETDATE()
-- ��� ��������� �������, ��� �������� ���������� �����									
DECLARE @profile_name as nvarchar(100) = ''
-- ���������� ��������� ����������� �����, ����������� ������ ";"				
DECLARE @recipients as nvarchar(500) = ''

-- ��������� ����������	
DECLARE @SQLString NVARCHAR(4000)
DECLARE @backupfile NVARCHAR(500)
DECLARE @physicalName NVARCHAR(500), @logicalName NVARCHAR(500)
DECLARE @error as int
DECLARE @subject as NVARCHAR(100)
DECLARE @finalmassage as NVARCHAR(1000)

-- ���� �������
USE [master]

-- ������ ��������� �������, ���� ����� ��� ����
IF OBJECT_ID('tempdb.dbo.#BackupFiles') IS NOT NULL DROP TABLE #BackupFiles
IF OBJECT_ID('tempdb.dbo.#BackupFilesFinal') IS NOT NULL DROP TABLE #BackupFilesFinal

-- ������� ������ � ���� ��������� ������ �������
SELECT
	backupset.backup_start_date,
	backupset.backup_set_uuid,
	backupset.differential_base_guid,
	backupset.[type] as btype,
	backupmediafamily.physical_device_name
INTO #BackupFiles	
FROM msdb.dbo.backupset AS backupset
    INNER JOIN msdb.dbo.backupmediafamily AS backupmediafamily 
	ON backupset.media_set_id = backupmediafamily.media_set_id
WHERE backupset.database_name = @DBName 
	and backupset.backup_start_date < @BackupTime
	and backupset.is_copy_only = 1 -- ���� "������ ��������� �����������"
	and backupset.is_snapshot = 0 -- ���� "�� snapshot"
	and (backupset.description is null or backupset.description not like 'Image-level backup') -- ������ �� Veeam Backup & Replication
	and device_type <> 7
ORDER BY 
	backupset.backup_start_date DESC

-- ������ ��������� ������ �����
SELECT TOP 1
	BackupFiles.backup_start_date,
	BackupFiles.physical_device_name,
	BackupFiles.backup_set_uuid	
INTO #BackupFilesFinal	 
FROM #BackupFiles AS BackupFiles
WHERE btype = 'D'
ORDER BY backup_start_date ASC

IF (SELECT COUNT(*) FROM #BackupFilesFinal) = 0
	SET @physicalName = ''
ELSE
	SET @physicalName = (SELECT TOP 1
		physical_device_name
	FROM #BackupFilesFinal)

IF @physicalName = ''
	-- ���� �������� ������� �� �������, �� ������ �������� ����� �� �������
	BEGIN
		SET @subject = '������ �������������� ���� ������ ' + @DBName
		SET @finalmassage = '�� ������� ������ ��������� ����� ��� ���� ������ ' + @DBName
	END
ELSE
	BEGIN

	-- ���������� ����������� �����
	SET @SQLString = N'ALTER DATABASE [' + @DBName + '] SET SINGLE_USER WITH ROLLBACK IMMEDIATE'

	-- ������� � ��������� ���������� ����������
	PRINT @SQLString
	EXEC sp_executesql @SQLString
	SET @error = @@error
	IF @error <> 0
		BEGIN
			-- ���� ���� ������, �� ������������ ������ ����� �� �������
			SET @subject = '������ �������������� ���� ������ ' + @DBName
			SET @finalmassage = '������ ��������� ������������ ������ ��� ���� ������ ' + @DBName + CHAR(13) + CHAR(13)
				+ '��� ������: ' + CAST(@error as NVARCHAR(10)) + CHAR(13) + CHAR(13)
				+ '����� T-SQL: ' + CHAR(13) + @SQLString
		END
	ELSE
		BEGIN

		-- ��������� ������ �����
		SET @SQLString = 
		N'RESTORE DATABASE [' + @DBName + ']
		FROM DISK = N''' + @physicalName + ''' 
		WITH  
		FILE = 1,'

		-- ����������� ����� ���� ������ �� ��������
		-- ����� ���� �� ���� ������ ���� ������
		DECLARE fnc CURSOR LOCAL FAST_FORWARD FOR 
		(
			SELECT
				t_files.name,
				t_files.physical_name
			FROM
				sys.master_files as t_files
			WHERE 
				t_files.database_id = DB_ID(@DBName)
		)
		OPEN fnc;
		FETCH fnc INTO @logicalName, @physicalName;
		WHILE @@FETCH_STATUS=0
			BEGIN
				SET @SQLString = @SQLString + '
				MOVE N''' + @logicalName + ''' TO N''' + @physicalName + ''','
				FETCH fnc INTO @logicalName, @physicalName;
			END;
		CLOSE fnc;
		DEALLOCATE fnc;

		SET @SQLString = @SQLString + '
		NORECOVERY,
		REPLACE,
		STATS = 5'

		-- ������� � ��������� ���������� ����������
		PRINT @SQLString
		EXEC sp_executesql @SQLString
		SET @error = @@error
		IF @error <> 0
			BEGIN
				-- ���� ���� ������, �� ������������ ������ ����� �� �������
				SET @subject = '������ �������������� ���� ������ ' + @DBName
				SET @finalmassage = '������ �������������� ������ ��������� ����� ��� ���� ������ ' + @DBName + CHAR(13) + CHAR(13)
					+ '��� ������: ' + CAST(@error as NVARCHAR(10)) + CHAR(13) + CHAR(13)
					+ '����� T-SQL: ' + CHAR(13) + @SQLString
			END
		ELSE
			BEGIN

			-- 3. ��������� ���� � ����������� �����
			SET @SQLString = 
			N'RESTORE DATABASE ' + @DBName + '
			WITH RECOVERY'
			
			-- ������� � ��������� ���������� ����������
			PRINT @SQLString	
			EXEC sp_executesql @SQLString
			SET @error = @@error
			IF @error <> 0
				BEGIN
					-- ������ �������� ���� � ����������� �����
					SET @subject = '������ �������������� ���� ������ ' + @DBName
					SET @finalmassage = '������ �������� � ����������� ����� ���� ������ ' + @DBName + CHAR(13) + CHAR(13)
						+ '��� ������: ' + CAST(@error as NVARCHAR(10)) + CHAR(13) + CHAR(13)
						+ '����� T-SQL: ' + CHAR(13) + @SQLString
				END
			ELSE
				BEGIN

				-- ��������� ���� � ������� ������ ��������������
				SET @SQLString = 
					'ALTER DATABASE ' + @DBName + ' SET RECOVERY SIMPLE;'
				
				-- ������� � ��������� ���������� ����������
				PRINT @SQLString	
				EXEC sp_executesql @SQLString
				SET @error = @@error
				IF @error <> 0
					BEGIN
						-- ������ �������� ���� � ������� ������ �������������
						SET @subject = '������ �������������� ���� ������ ' + @DBName
						SET @finalmassage = '������ �������� � ������� ������ �������������� ���� ������ ' + @DBName + CHAR(13) + CHAR(13)
							+ '��� ������: ' + CAST(@error as NVARCHAR(10)) + CHAR(13) + CHAR(13)
							+ '����� T-SQL: ' + CHAR(13) + @SQLString
					END
				ELSE
					BEGIN

					-- ��������� ������ ���� ������
					SET @SQLString = 
						'DBCC SHRINKDATABASE(N''' + @DBName + ''');'
					
					-- ������� � ��������� ���������� ����������
					PRINT @SQLString
					EXEC sp_executesql @SQLString
					SET @error = @@error
					IF @error <> 0
						BEGIN
							-- ������ ������ ���� ������
							SET @subject = '������ �������������� ���� ������ ' + @DBName
							SET @finalmassage = '������ ������ ���� ������ ' + @DBName + CHAR(13) + CHAR(13)
								+ '��� ������: ' + CAST(@error as NVARCHAR(10)) + CHAR(13) + CHAR(13)
								+ '����� T-SQL: ' + CHAR(13) + @SQLString
						END
					ELSE
						BEGIN

						-- ����� ����������� �����
						SET @SQLString = N'ALTER DATABASE [' + @DBName + '] SET MULTI_USER'

						-- ������� � ��������� ���������� ����������
						PRINT @SQLString
						EXEC sp_executesql @SQLString
						SET @error = @@error
						IF @error <> 0
							BEGIN
								-- ���� ���� ������, �� ������������ ������ ����� �� �������
								SET @subject = '������ �������������� ���� ������ ' + @DBName
								SET @finalmassage = '������ ������ ������������ ������ ��� ���� ������ ' + @DBName + CHAR(13) + CHAR(13)
									+ '��� ������: ' + CAST(@error as NVARCHAR(10)) + CHAR(13) + CHAR(13)
									+ '����� T-SQL: ' + CHAR(13) + @SQLString
							END
						ELSE
							BEGIN
								-- �������� ���������� ���� ��������
								SET @subject = '�������� �������������� ���� ������ ' + @DBName
								SET @finalmassage = '�������� �������������� ���� ������ ' + @DBName + ' �� ��������� ����� ' + @physicalName + ' �� ������ ������� ' + Replace(CONVERT(nvarchar, @BackupTime, 126),':','-')
							END
						END
					END
				END
			END
		END
	END

-- ������� ��������� �������
drop table #BackupFiles
drop table #BackupFilesFinal

-- ���� ����� ������� ����������� �����, �������� ���������
IF @profile_name <> '' and @recipients <> ''
EXEC msdb.dbo.sp_send_dbmail
    @profile_name = @profile_name,
    @recipients = @recipients,
    @body = @finalmassage,
    @subject = @subject;

-- ������� ��������� � ����������
SELECT
	@subject as massage
	
GO
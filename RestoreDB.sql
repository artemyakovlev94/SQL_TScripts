
-- Оргинал-источник: https://github.com/Tavalik/SQL_TScripts/tree/master

-- База данных
DECLARE @DBName as nvarchar(40) = 'db_name'
-- Дата, на котороую собирается цепочка файлов резервных копий, в формате '20160315 12:00:00'							
DECLARE @BackupTime as datetime = GETDATE()
-- Имя почтового профиля, для отправки электонной почты									
DECLARE @profile_name as nvarchar(100) = ''
-- Получатели сообщений электронной почты, разделенные знаком ";"				
DECLARE @recipients as nvarchar(500) = ''

-- СЛУЖЕБНЫЕ ПЕРЕМЕННЫЕ	
DECLARE @SQLString NVARCHAR(4000)
DECLARE @backupfile NVARCHAR(500)
DECLARE @physicalName NVARCHAR(500), @logicalName NVARCHAR(500)
DECLARE @error as int
DECLARE @subject as NVARCHAR(100)
DECLARE @finalmassage as NVARCHAR(1000)

-- ТЕЛО СКРИПТА
USE [master]

-- Удалим временные таблицы, если вдруг они есть
IF OBJECT_ID('tempdb.dbo.#BackupFiles') IS NOT NULL DROP TABLE #BackupFiles
IF OBJECT_ID('tempdb.dbo.#BackupFilesFinal') IS NOT NULL DROP TABLE #BackupFilesFinal

-- Соберем данные о всех сдаланных раннее бэкапах
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
	and backupset.is_copy_only = 1 -- флаг "Только резервное копирование"
	and backupset.is_snapshot = 0 -- флаг "Не snapshot"
	and (backupset.description is null or backupset.description not like 'Image-level backup') -- Защита от Veeam Backup & Replication
	and device_type <> 7
ORDER BY 
	backupset.backup_start_date DESC

-- Найдем последний полный бэкап
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
	-- Если получить элемент не удалось, то полная резерная копия не найдена
	BEGIN
		SET @subject = 'ОШИБКА ВОССТАНОВЛЕНИЯ базы данных ' + @DBName
		SET @finalmassage = 'Не найдена полная резервная копия для базы данных ' + @DBName
	END
ELSE
	BEGIN

	-- Установить монопольный режим
	SET @SQLString = N'ALTER DATABASE [' + @DBName + '] SET SINGLE_USER WITH ROLLBACK IMMEDIATE'

	-- Выводим и выполняем полученную инструкцию
	PRINT @SQLString
	EXEC sp_executesql @SQLString
	SET @error = @@error
	IF @error <> 0
		BEGIN
			-- Если были ошибки, то восстановить полную копию не удалось
			SET @subject = 'ОШИБКА ВОССТАНОВЛЕНИЯ базы данных ' + @DBName
			SET @finalmassage = 'Ошибка установки монопольного режима для базы данных ' + @DBName + CHAR(13) + CHAR(13)
				+ 'Код ошибки: ' + CAST(@error as NVARCHAR(10)) + CHAR(13) + CHAR(13)
				+ 'Текст T-SQL: ' + CHAR(13) + @SQLString
		END
	ELSE
		BEGIN

		-- Загружаем полный бэкап
		SET @SQLString = 
		N'RESTORE DATABASE [' + @DBName + ']
		FROM DISK = N''' + @physicalName + ''' 
		WITH  
		FILE = 1,'

		-- Переименуем файлы базы данных на исходные
		-- Новый цикл по всем файлам базы данных
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

		-- Выводим и выполняем полученную инструкцию
		PRINT @SQLString
		EXEC sp_executesql @SQLString
		SET @error = @@error
		IF @error <> 0
			BEGIN
				-- Если были ошибки, то восстановить полную копию не удалось
				SET @subject = 'ОШИБКА ВОССТАНОВЛЕНИЯ базы данных ' + @DBName
				SET @finalmassage = 'Ошибка восстановления полной резервной копии для базы данных ' + @DBName + CHAR(13) + CHAR(13)
					+ 'Код ошибки: ' + CAST(@error as NVARCHAR(10)) + CHAR(13) + CHAR(13)
					+ 'Текст T-SQL: ' + CHAR(13) + @SQLString
			END
		ELSE
			BEGIN

			-- 3. Переводим базу в оперативный режим
			SET @SQLString = 
			N'RESTORE DATABASE ' + @DBName + '
			WITH RECOVERY'
			
			-- Выводим и выполняем полученную инструкцию
			PRINT @SQLString	
			EXEC sp_executesql @SQLString
			SET @error = @@error
			IF @error <> 0
				BEGIN
					-- Ошибка перевода базы в оперативный режим
					SET @subject = 'ОШИБКА ВОССТАНОВЛЕНИЯ базы данных ' + @DBName
					SET @finalmassage = 'Ошибка перевода в оперативный режим базы данных ' + @DBName + CHAR(13) + CHAR(13)
						+ 'Код ошибки: ' + CAST(@error as NVARCHAR(10)) + CHAR(13) + CHAR(13)
						+ 'Текст T-SQL: ' + CHAR(13) + @SQLString
				END
			ELSE
				BEGIN

				-- Переводим базу в простую модель восстановления
				SET @SQLString = 
					'ALTER DATABASE ' + @DBName + ' SET RECOVERY SIMPLE;'
				
				-- Выводим и выполняем полученную инструкцию
				PRINT @SQLString	
				EXEC sp_executesql @SQLString
				SET @error = @@error
				IF @error <> 0
					BEGIN
						-- Ошибка перевода базы в простую модель восстановлеия
						SET @subject = 'ОШИБКА ВОССТАНОВЛЕНИЯ базы данных ' + @DBName
						SET @finalmassage = 'Ошибка перевода в простую модель восстановления базы данных ' + @DBName + CHAR(13) + CHAR(13)
							+ 'Код ошибки: ' + CAST(@error as NVARCHAR(10)) + CHAR(13) + CHAR(13)
							+ 'Текст T-SQL: ' + CHAR(13) + @SQLString
					END
				ELSE
					BEGIN

					-- Запускаем сжатие базы данных
					SET @SQLString = 
						'DBCC SHRINKDATABASE(N''' + @DBName + ''');'
					
					-- Выводим и выполняем полученную инструкцию
					PRINT @SQLString
					EXEC sp_executesql @SQLString
					SET @error = @@error
					IF @error <> 0
						BEGIN
							-- Ошбика сжатия базы данных
							SET @subject = 'ОШИБКА ВОССТАНОВЛЕНИЯ базы данных ' + @DBName
							SET @finalmassage = 'Ошибка сжатия базы данных ' + @DBName + CHAR(13) + CHAR(13)
								+ 'Код ошибки: ' + CAST(@error as NVARCHAR(10)) + CHAR(13) + CHAR(13)
								+ 'Текст T-SQL: ' + CHAR(13) + @SQLString
						END
					ELSE
						BEGIN

						-- Снять монопольный режим
						SET @SQLString = N'ALTER DATABASE [' + @DBName + '] SET MULTI_USER'

						-- Выводим и выполняем полученную инструкцию
						PRINT @SQLString
						EXEC sp_executesql @SQLString
						SET @error = @@error
						IF @error <> 0
							BEGIN
								-- Если были ошибки, то восстановить полную копию не удалось
								SET @subject = 'ОШИБКА ВОССТАНОВЛЕНИЯ базы данных ' + @DBName
								SET @finalmassage = 'Ошибка снятия монопольного режима для базы данных ' + @DBName + CHAR(13) + CHAR(13)
									+ 'Код ошибки: ' + CAST(@error as NVARCHAR(10)) + CHAR(13) + CHAR(13)
									+ 'Текст T-SQL: ' + CHAR(13) + @SQLString
							END
						ELSE
							BEGIN
								-- Успешное выполнение всех операций
								SET @subject = 'Успешное восстановление базы данных ' + @DBName
								SET @finalmassage = 'Успешное восстановление базы данных ' + @DBName + ' из резервной копии ' + @physicalName + ' на момент времени ' + Replace(CONVERT(nvarchar, @BackupTime, 126),':','-')
							END
						END
					END
				END
			END
		END
	END

-- Удаляем временные таблицы
drop table #BackupFiles
drop table #BackupFilesFinal

-- Если задан профиль электронной почты, отправим сообщение
IF @profile_name <> '' and @recipients <> ''
EXEC msdb.dbo.sp_send_dbmail
    @profile_name = @profile_name,
    @recipients = @recipients,
    @body = @finalmassage,
    @subject = @subject;

-- Выводим сообщение о результате
SELECT
	@subject as massage
	
GO
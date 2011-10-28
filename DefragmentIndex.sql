/*
	【インデックス デフラグ】
	概要：
		全てのインデックス(※1)の断片化情報を元に、断片化率が酷くデータが多い順にデフラグを
		実行します。
*/
	--------------------------------------------------------------------------
	-- 【宣言】
	--------------------------------------------------------------------------
	-- <オプション設定>
	SET NOCOUNT ON

	-- <定数宣言>
	DECLARE @LOWER_LIMIT_AVGFRAGINPERCENT	FLOAT = 5.0				-- デフラグ対象とする断片化割合下限値
	DECLARE @LOWER_LIMIT_PAGECOUNT			FLOAT = 10.0			-- デフラグ対象とするページ数下限値
	DECLARE @EXEC_REBUILD_AVGFRAGINPERCENT	FLOAT = 20.0			-- 再構成対象とする断片化割合下限値


	-- <変数宣言>
	DECLARE @BATCH_PRG_CODE					NVARCHAR(50)
	DECLARE @BATCH_PRG_EXE_UNIQUE_ID		NVARCHAR(16)
	DECLARE @EXEC_TERMS						NVARCHAR(3000)

	DECLARE @SQL							NVARCHAR(4000)
	DECLARE @PRINT							NVARCHAR(4000)
        
	DECLARE @DATABASE						SYSNAME
	DECLARE @TABLE							SYSNAME
	DECLARE @INDEX							SYSNAME
	DECLARE @COLUMN							SYSNAME
        
	DECLARE @AVGFRAGINPERCENT				FLOAT
	DECLARE @PAGECOUNT						BIGINT
	DECLARE @ALLOWPAGELOCKS					BIT
        
	DECLARE @HEAPINDEX						SYSNAME
	DECLARE @HEAPINDEXCOLUMNS				NVARCHAR(1000)
	DECLARE @COMMA							NVARCHAR(2)
	DECLARE @OPTION 						NVARCHAR(20)
        
	DECLARE @DATABASEID						SMALLINT
	DECLARE @TABLEID						INT
	DECLARE @INDEXID						INT


	--------------------------------------------------------------------------
	-- 【初期処理】
	--------------------------------------------------------------------------
BEGIN TRY

	-- <初期値設定>
	SET @BATCH_PRG_CODE = 'DefragIdx'
	SET @HEAPINDEX	= N'_IDX_Temp_HeapIndex__'


	-- <一時テーブル作成>
	IF (Object_Id('tempdb..#INDEXESSTATUS', 'u') IS NOT NULL)
	BEGIN
		DROP TABLE [#INDEXESSTATUS]
	END

	CREATE TABLE [#INDEXESSTATUS]
	(
		 [DATABASE_ID]					SMALLINT
		,[OBJECT_ID]					INT
		,[INDEX_ID]						INT
		,[DATABASENAME]					SYSNAME
		,[OBJECTNAME]					SYSNAME
		,[INDEXNAME]					SYSNAME
		,[AVGFRAGINPERCENT]				FLOAT
		,[PAGECOUNT]					BIGINT
		,[ALLOWPAGELOCKS]				BIT
	)

	IF (Object_Id('tempdb..#INDEXES', 'u') IS NOT NULL)
	BEGIN
		DROP TABLE [#INDEXES]
	END

	CREATE TABLE [#INDEXES]
	(
		 [DATABASE_ID]					SMALLINT
		,[OBJECT_ID]					INT
		,[INDEX_ID]						INT
		,[DATABASENAME]					SYSNAME
		,[OBJECTNAME]					SYSNAME
		,[INDEXNAME]					SYSNAME
		,[ALLOWPAGELOCKS]				BIT
	)

	IF (Object_Id('tempdb..#INDEXESCOLUMNS', 'U') IS NOT NULL)
	BEGIN
		DROP TABLE [#INDEXESCOLUMNS]
	END

	CREATE TABLE [#INDEXESCOLUMNS]
	(
		 [INDEXCOLUMNID]				SMALLINT
		,[COLUMNNAME]					SYSNAME
	)



	--------------------------------------------------------------------------
	-- 【インデックス断片化情報取得】
	--------------------------------------------------------------------------
	-- <インデックス情報取得>
	DECLARE DATABASESCURSOR	CURSOR	LOCAL FAST_FORWARD READ_ONLY
	FOR
		SELECT
			 RTRIM([NAME])
		FROM [master].[dbo].[SYSDATABASES]

	OPEN DATABASESCURSOR

	FETCH NEXT FROM DATABASESCURSOR
	INTO
		 @DATABASE

	WHILE (@@FETCH_STATUS = 0)
	BEGIN
		PRINT CONVERT( NVARCHAR, GETDATE(), 120 ) + ' >   データベース [' + @DATABASE + '] のインデックス情報を取得中'


		SET	@SQL	= N'USE [' + @DATABASE + N']'						+ CHAR(13)		-- ※ CHAR(13)は改行コードです。PRINTを使うと改行されて表示されます
					+ N'SELECT	DB_ID(),'								+ CHAR(13)
					+ N'		I.OBJECT_ID,'							+ CHAR(13)
					+ N'		I.INDEX_ID,'							+ CHAR(13)
					+ N'		DB_NAME(),'								+ CHAR(13)
					+ N'		O.NAME,'								+ CHAR(13)
					+ N'		ISNULL(I.NAME, N''''),'					+ CHAR(13)
					+ N'		I.ALLOW_PAGE_LOCKS'						+ CHAR(13)
					+ N'FROM	SYS.INDEXES		AS I'					+ CHAR(13)
					+ N'INNER JOIN SYS.OBJECTS	AS O'					+ CHAR(13)
					+ N'	ON	O.OBJECT_ID = I.OBJECT_ID'				+ CHAR(13)
					+ N'WHERE	O.TYPE			= ''U'''				+ CHAR(13)
					+ N'	AND	I.IS_DISABLED	= 0'					+ CHAR(13)

		INSERT INTO [#INDEXES]
		(
			 [DATABASE_ID]
			,[OBJECT_ID]
			,[INDEX_ID]
			,[DATABASENAME]
			,[OBJECTNAME]
			,[INDEXNAME]
			,[ALLOWPAGELOCKS]
		)
		EXEC SP_EXECUTESQL @SQL

		FETCH NEXT FROM DATABASESCURSOR
		INTO
			 @DATABASE
	END

	CLOSE DATABASESCURSOR
	DEALLOCATE DATABASESCURSOR


	-- <断片化しているインデックス抽出>
	INSERT INTO [#INDEXESSTATUS]
	(
		 [DATABASE_ID]
		,[OBJECT_ID]
		,[INDEX_ID]
		,[DATABASENAME]
		,[OBJECTNAME]
		,[INDEXNAME]
		,[AVGFRAGINPERCENT]
		,[PAGECOUNT]
		,[ALLOWPAGELOCKS]
	)
	SELECT
		 [I].[DATABASE_ID]
		,[I].[OBJECT_ID]
		,[I].[INDEX_ID]
		,[I].[DATABASENAME]
		,[I].[OBJECTNAME]
		,[I].[INDEXNAME]
		,[IPS].[AVG_FRAGMENTATION_IN_PERCENT]
		,[IPS].[PAGE_COUNT]
		,[I].[ALLOWPAGELOCKS]
	FROM [#INDEXES] AS [I]
	INNER JOIN [SYS].[DM_DB_INDEX_PHYSICAL_STATS]( NULL, NULL, NULL, NULL, N'LIMITED' )	AS [IPS]
		ON	[IPS].[DATABASE_ID] 		= [I].[DATABASE_ID]
		AND	[IPS].[OBJECT_ID]			= [I].[OBJECT_ID]
		AND	[IPS].[INDEX_ID]			= [I].[INDEX_ID]
	WHERE
			[IPS].[ALLOC_UNIT_TYPE_DESC]			NOT IN ( 'LOB_DATA' )
		AND	[IPS].[AVG_FRAGMENTATION_IN_PERCENT]	IS NOT NULL
		AND	[IPS].[AVG_FRAGMENTATION_IN_PERCENT]	> 0.0



	--------------------------------------------------------------------------
	-- 【デフラグメント】
	--------------------------------------------------------------------------
	Print Convert( NVarChar, GetDate(), 120 ) + ' >   デフラグ処理開始'

	-- <デフラグ対象インデックスを断片化・データ量順にソート>
	DECLARE	DEFRAGINDEXESCURSOR	CURSOR	LOCAL FAST_FORWARD READ_ONLY
	FOR
		SELECT
			 [DATABASE_ID]
			,[OBJECT_ID]
			,[INDEX_ID]
			,[DATABASENAME]
			,[OBJECTNAME]
			,[INDEXNAME]
			,[AVGFRAGINPERCENT]
			,[PAGECOUNT]
			,[ALLOWPAGELOCKS]
		FROM [#INDEXESSTATUS]
		WHERE
				[AVGFRAGINPERCENT]				> @LOWER_LIMIT_AVGFRAGINPERCENT
			AND	[PAGECOUNT]						> @LOWER_LIMIT_PAGECOUNT
		ORDER BY
			 [PAGECOUNT] * [AVGFRAGINPERCENT]			DESC
			,[DATABASENAME]
			,[OBJECTNAME]
			,[INDEXNAME]

	OPEN DEFRAGINDEXESCURSOR

	FETCH NEXT FROM DEFRAGINDEXESCURSOR
	INTO
		 @DATABASEID
		,@TABLEID
		,@INDEXID
		,@DATABASE
		,@TABLE
		,@INDEX
		,@AVGFRAGINPERCENT
		,@PAGECOUNT
		,@ALLOWPAGELOCKS

	WHILE (@@FETCH_STATUS = 0)
	BEGIN
		IF ( @AVGFRAGINPERCENT <= @LOWER_LIMIT_AVGFRAGINPERCENT)
		BEGIN
		-- <論理的断片化が下限設定未満の場合、デフラグ対象としない>
			PRINT CONVERT( NVARCHAR, GETDATE(), 120 ) + ' > SKIP [' + @TABLE + '] ' + ' AVG-FRAG-IN-PERCENT : ' + CAST(@AVGFRAGINPERCENT AS NVARCHAR)
		END
		ELSE
		BEGIN
			IF (@INDEX = N'')
			BEGIN
			-- <ヒープデータの場合>
				-- ** 初期化 **
				TRUNCATE TABLE [#INDEXESCOLUMNS]

				-- ** プライマリキーの有無チェック **
				SET @SQL	= N'USE [' + @DATABASE + N']'										+ CHAR(13)
							+ N'SELECT	INDEX_COLUMN_ID, C.NAME'								+ CHAR(13)
							+ N'FROM	SYS.INDEXES				AS A'							+ CHAR(13)
							+ N'INNER JOIN SYS.INDEX_COLUMNS	AS B'							+ CHAR(13)
							+ N'	ON	B.OBJECT_ID = A.OBJECT_ID'								+ CHAR(13)
							+ N'	AND	B.INDEX_ID = A.INDEX_ID'								+ CHAR(13)
							+ N'INNER JOIN SYS.COLUMNS			AS C'							+ CHAR(13)
							+ N'	ON	C.OBJECT_ID = B.OBJECT_ID'								+ CHAR(13)
							+ N'	AND	C.COLUMN_ID = B.COLUMN_ID'								+ CHAR(13)
							+ N'WHERE	A.OBJECT_ID = OBJECT_ID(''' + @TABLE + N''')'			+ CHAR(13)
							+ N'	AND	A.IS_PRIMARY_KEY = 1'									+ CHAR(13)

				INSERT INTO [#INDEXESCOLUMNS]
				(
					 [INDEXCOLUMNID]
					,[COLUMNNAME]
				)
				EXEC SP_EXECUTESQL @SQL

				IF NOT EXISTS ( SELECT * FROM [#INDEXESCOLUMNS] )
				BEGIN
					-- NOT FOUND PK.
					PRINT CONVERT( NVARCHAR, GETDATE(), 120 ) + ' > SKIP [' + @DATABASE + ']..[' + @TABLE + '] ON HEAPDATA / NOT FOUND PK.'

					-- NOT IMPLEMENT.
				END
				ELSE
				BEGIN
					-- ** プライマリキーがある場合、プライマリーキーでクラスタインデックスを作成する **
					SET	@HEAPINDEXCOLUMNS	= N''
					SET @COMMA				= N' '

					DECLARE COLUMNCURSOR	CURSOR LOCAL READ_ONLY FAST_FORWARD
						FOR	SELECT	[COLUMNNAME]
							FROM	[#INDEXESCOLUMNS]
							ORDER BY [INDEXCOLUMNID]

					OPEN COLUMNCURSOR

					FETCH NEXT FROM COLUMNCURSOR
						INTO @COLUMN

					WHILE @@FETCH_STATUS = 0
					BEGIN
						SET	@HEAPINDEXCOLUMNS	= @HEAPINDEXCOLUMNS + @COMMA + N'[' + RTRIM(@COLUMN) + N']'
						SET	@COMMA = N' ,'

						FETCH NEXT FROM COLUMNCURSOR
							INTO @COLUMN
					END
					
					CLOSE COLUMNCURSOR
					DEALLOCATE COLUMNCURSOR

					SET @SQL	= N'USE [' + @DATABASE + N']'		+ CHAR(13)
								+ N'BEGIN TRANSACTION'				+ CHAR(13)
								+ N'BEGIN TRY'						+ CHAR(13)
								+ N'	CREATE CLUSTERED INDEX [' + @HEAPINDEX + @TABLE + '] ON [' + @TABLE + '] ( ' + @HEAPINDEXCOLUMNS + ' )' + CHAR(13)
								+ N'	DROP INDEX [' + @TABLE + '].[' + @HEAPINDEX + @TABLE + ']' + CHAR(13)
								+ N'	COMMIT TRANSACTION'			+ CHAR(13)
								+ N'END TRY'						+ CHAR(13)
								+ N'BEGIN CATCH'					+ CHAR(13)
								+ N'	ROLLBACK TRANSACTION'		+ CHAR(13)
								+ N'END CATCH'						+ CHAR(13)

					PRINT CONVERT( NVARCHAR, GETDATE(), 120 )
							+ ' >   [' + @DATABASE + ']..[' + @TABLE + '] / [' + @INDEX + ']  CREATE CLUSTERD INDEX, AND DROP INDEX.'
							+ ' AVG-FRAG-IN-PERCENT : ' + CAST(@AVGFRAGINPERCENT AS NVARCHAR)
							+ ' PAGE-COUNT : ' + CAST(@PAGECOUNT AS NVARCHAR)

					EXEC SP_EXECUTESQL @SQL
				END
			END
			ELSE
			BEGIN
			-- <デフラグ処理>
				IF	( @EXEC_REBUILD_AVGFRAGINPERCENT < @AVGFRAGINPERCENT OR @ALLOWPAGELOCKS = 0 )
				BEGIN
					SET @OPTION = N'REBUILD WITH ( SORT_IN_TEMPDB = ON )'
				END
				ELSE
				BEGIN
					SET @OPTION = N'REORGANIZE'
				END

				SET @SQL	= N'USE [' + @DATABASE + N']' + CHAR(13)
							+ N'ALTER INDEX [' + @INDEX + '] ON [dbo].[' + @TABLE + '] ' + @OPTION

				PRINT CONVERT( NVARCHAR, GETDATE(), 120 )
						+ ' >   [' + @DATABASE + '].[dbo].[' + @TABLE + '] / [' + @INDEX + '] ALTER INDEX - ' + @OPTION
						+ ' AVG-FRAG-IN-PERCENT : ' + CAST(@AVGFRAGINPERCENT AS NVARCHAR)
						+ ' PAGE-COUNT : ' + CAST(@PAGECOUNT AS NVARCHAR)

				EXEC SP_EXECUTESQL @SQL
			END

			-- 解消率確認
			SELECT
				 @AVGFRAGINPERCENT = [AVG_FRAGMENTATION_IN_PERCENT]
			FROM [SYS].[DM_DB_INDEX_PHYSICAL_STATS]( @DATABASEID, @TABLEID, @INDEXID, NULL, N'LIMITED' )

			PRINT CONVERT( NVARCHAR, GETDATE(), 120 ) + ' >     => AVG-FRAG-IN-PERCENT : ' + CAST(@AVGFRAGINPERCENT AS NVARCHAR)
			

			-- <トランザクションログをシュリンク>
--			SET @SQL	= N'USE [' + @DATABASE + N']' + CHAR(13)
--						+ N'DBCC SHRINKFILE (N''' + @DATABASE + '_Log'' , 0, TRUNCATEONLY) '
--
--			EXEC SP_EXECUTESQL @SQL

		END


		FETCH NEXT FROM DEFRAGINDEXESCURSOR
		INTO
			 @DATABASEID
			,@TABLEID
			,@INDEXID
			,@DATABASE
			,@TABLE
			,@INDEX
			,@AVGFRAGINPERCENT
			,@PAGECOUNT
			,@ALLOWPAGELOCKS
	END



	--------------------------------------------------------------------------
	-- 【終了処理】
	--------------------------------------------------------------------------
END_PROC:
	-- <デアロケート>
	CLOSE DEFRAGINDEXESCURSOR
	DEALLOCATE DEFRAGINDEXESCURSOR


	DROP TABLE [#INDEXESSTATUS]
	DROP TABLE [#INDEXES]
	DROP TABLE [#INDEXESCOLUMNS]

END TRY



--------------------------------------------------------------------------
-- 【異常処理】
--------------------------------------------------------------------------
BEGIN CATCH
	-- <カーソル変数デアロケート>
	IF CURSOR_STATUS('variable', 'DATABASESCURSOR') > -1
	BEGIN
		CLOSE DATABASESCURSOR
		DEALLOCATE DATABASESCURSOR
	END
	IF CURSOR_STATUS('variable', 'DEFRAGINDEXESCURSOR') > -1
	BEGIN
		CLOSE DEFRAGINDEXESCURSOR
		DEALLOCATE DEFRAGINDEXESCURSOR
	END
	IF CURSOR_STATUS('variable', 'COLUMNCURSOR') > -1
	BEGIN
		CLOSE COLUMNCURSOR
		DEALLOCATE COLUMNCURSOR
	END


	-- <例外発生>
    DECLARE @ErrorMessage               NVARCHAR(4000)
    DECLARE @ErrorSeverity              INT
    DECLARE @ErrorState                 INT
    DECLARE @ErrorProcedure             NVARCHAR(4000)
    DECLARE @ErrorLine                  INT

	DECLARE @RaiseMessage               NVARCHAR(4000)

    SET @ErrorMessage               = ERROR_MESSAGE()
    SET @ErrorSeverity              = ERROR_SEVERITY()
    SET @ErrorState                 = ERROR_STATE()
    SET @ErrorProcedure             = ERROR_PROCEDURE()
    SET @ErrorLine                  = ERROR_LINE()

    IF @ErrorSeverity = 16
    BEGIN
        -- チェックエラー(業務エラー)の場合、メッセージのみを表示
        SET @RaiseMessage    = @ErrorMessage
    END
    ELSE
    BEGIN
        SET @RaiseMessage    = ISNULL(@ErrorProcedure, '') + ' LINE:' + CONVERT(NVARCHAR, @ErrorLine) + ' (' + @ErrorMessage + ')'
    END

	RAISERROR(@RaiseMessage, @ErrorSeverity, @ErrorState)
END CATCH


GO

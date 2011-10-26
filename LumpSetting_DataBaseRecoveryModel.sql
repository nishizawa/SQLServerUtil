/*	【データベースの復旧モデルを一括で変更する】
*/

	--------------------------------------------------------------------------
	-- 【宣言】
	--------------------------------------------------------------------------
	--<オプション設定>
	SET NOCOUNT ON

	-- <パラメータ>
	DECLARE @RECOVERY_MODEL_TYPE			NVARCHAR(1) = 'F'				-- F:完全、B:一括ログ、S:シンプル

	-- <変数宣言>
	DECLARE @ERROR_MESSAGE					NVARCHAR(4000)

	DECLARE @SQL							NVARCHAR(4000)
	DECLARE @DATABASE						NVARCHAR(100)
	DECLARE @RECOVERY_MODEL					NVARCHAR(20)



	--------------------------------------------------------------------------
	-- 【初期処理】
	--------------------------------------------------------------------------
BEGIN TRY
	-- <パラメータチェック>
	IF (@RECOVERY_MODEL_TYPE IS NULL OR @RECOVERY_MODEL_TYPE = '')
	BEGIN
		SET @ERROR_MESSAGE	= N'必須パラメータ、[復旧モデル区分]が設定されていません。'
		RAISERROR(@ERROR_MESSAGE, 16, 1)
	END
	IF (@RECOVERY_MODEL_TYPE NOT IN ('F', 'B', 'S'))
	BEGIN
		SET @ERROR_MESSAGE	= N'[復旧モデル区分]は、F, B, Sのいずれかを設定してください。'
		RAISERROR(@ERROR_MESSAGE, 16, 2)
	END


	-- <復旧モデル設定>
	SET @RECOVERY_MODEL			= CASE @RECOVERY_MODEL_TYPE
								  WHEN 'F' THEN 'FULL'
								  WHEN 'B' THEN 'BULK_LOGGED'
								  WHEN 'S' THEN 'SIMPLE'
								  END

	--------------------------------------------------------------------------
	-- 【一括変換】
	--------------------------------------------------------------------------
	-- <復旧モデルを完全に戻す>
	DECLARE DATABASESCURSOR	CURSOR	LOCAL FAST_FORWARD READ_ONLY
	FOR
		SELECT
			 RTRIM([NAME])
		FROM [master].[dbo].[SYSDATABASES]
		WHERE
				[NAME] <> 'tempdb'

	OPEN DATABASESCURSOR

	FETCH NEXT FROM DATABASESCURSOR
	INTO
		 @DATABASE

	WHILE (@@FETCH_STATUS = 0)
	BEGIN
		SET @SQL  = N'ALTER DATABASE [' + @DATABASE + N'] SET RECOVERY ' + @RECOVERY_MODEL + N' WITH NO_WAIT' + CHAR(13)

		EXEC SP_EXECUTESQL @SQL
		
		PRINT 'データベース 「' + @DATABASE + '」の復旧モデルを「' + @RECOVERY_MODEL + '」に変更しました。'


		FETCH NEXT FROM DATABASESCURSOR
		INTO
			 @DATABASE
	END



	--------------------------------------------------------------------------
	-- 【終了処理】
	--------------------------------------------------------------------------
	-- <デアロケート>
	CLOSE DATABASESCURSOR
	DEALLOCATE DATABASESCURSOR

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

	DECLARE @ErrorMessage					NVARCHAR(4000)
	DECLARE @ErrorSeverity					INT
	DECLARE @ErrorState						INT
	DECLARE @ErrorProcedure					NVARCHAR(4000)
	DECLARE @ErrorLine						INT

	DECLARE @RaiseMessage					NVARCHAR(4000)

	SET @ErrorMessage		= ERROR_MESSAGE()
	SET @ErrorSeverity		= ERROR_SEVERITY()
	SET @ErrorState			= ERROR_STATE()
    SET @ErrorProcedure		= ERROR_PROCEDURE()
	SET @ErrorLine			= ERROR_LINE()

	SET @RaiseMessage		= CASE WHEN @ErrorSeverity BETWEEN 10 AND 16
							  THEN @ErrorMessage
							  ELSE ISNULL(@ErrorProcedure, '') + ' LINE:' + CONVERT(NVARCHAR, @ErrorLine) + ' (' + @ErrorMessage + ')'
							  END

	RAISERROR(@RaiseMessage, @ErrorSeverity, @ErrorState)
END CATCH


GO


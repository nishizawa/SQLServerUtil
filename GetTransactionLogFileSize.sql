/*		【ログファイルサイズ トレース】
	概要：
	  ログファイルサイズのサイズを採取する
*/

	--------------------------------------------------------------------------
	-- 【宣言】
	--------------------------------------------------------------------------
	-- <オプション設定>
	SET NOCOUNT ON


	-- <変数宣言>
	DECLARE @SYS_DATE						DATETIME


	DECLARE @tran_log_space_usage AS TABLE
	(
		 [DATABASENAME]						NVARCHAR(100)
		,[LOGSIZE]							DECIMAL(12,3)
		,[LOGSPACEUSED]						DECIMAL(6,3)
		,[STATUS]							INT
	)


	--------------------------------------------------------------------------
	-- 【初期処理】
	--------------------------------------------------------------------------
BEGIN TRY
	-- <初期値設定>
	SET @SYS_DATE			= GETDATE()


	--------------------------------------------------------------------------
	-- 【取得】
	--------------------------------------------------------------------------
	-- <ログサイズ情報取得>

	INSERT INTO @tran_log_space_usage 
	EXEC('DBCC SQLPERF (LOGSPACE)');



	SELECT
		 CONVERT(DATE, @SYS_DATE)
		,[DATABASENAME]
		,[LOGSIZE]
		,[LOGSPACEUSED]
	FROM @tran_log_space_usage



	--------------------------------------------------------------------------
	-- 【終了処理】
	--------------------------------------------------------------------------
	-- <デアロケート>

END TRY



	--------------------------------------------------------------------------
	-- 【異常処理】
	--------------------------------------------------------------------------
BEGIN CATCH
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
							  ELSE ISNULL(@ErrorProcedure, '') + ' LINE:' + CONVERT(NVARCHAR, @ErrorLine) + ' (' + CAST(ERROR_NUMBER() AS NVARCHAR) + '：' + @ErrorMessage + ')'
							  END

	SET @ErrorSeverity		= 10

	RAISERROR(@RaiseMessage, @ErrorSeverity, @ErrorState)
END CATCH


GO

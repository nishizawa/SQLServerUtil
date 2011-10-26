/*		�y���O�t�@�C���T�C�Y �g���[�X�z
	�T�v�F
	  ���O�t�@�C���T�C�Y�̃T�C�Y���̎悷��
*/

	--------------------------------------------------------------------------
	-- �y�錾�z
	--------------------------------------------------------------------------
	-- <�I�v�V�����ݒ�>
	SET NOCOUNT ON


	-- <�ϐ��錾>
	DECLARE @SYS_DATE						DATETIME


	DECLARE @tran_log_space_usage AS TABLE
	(
		 [DATABASENAME]						NVARCHAR(100)
		,[LOGSIZE]							DECIMAL(12,3)
		,[LOGSPACEUSED]						DECIMAL(6,3)
		,[STATUS]							INT
	)


	--------------------------------------------------------------------------
	-- �y���������z
	--------------------------------------------------------------------------
BEGIN TRY
	-- <�����l�ݒ�>
	SET @SYS_DATE			= GETDATE()


	--------------------------------------------------------------------------
	-- �y�擾�z
	--------------------------------------------------------------------------
	-- <���O�T�C�Y���擾>

	INSERT INTO @tran_log_space_usage 
	EXEC('DBCC SQLPERF (LOGSPACE)');



	SELECT
		 CONVERT(DATE, @SYS_DATE)
		,[DATABASENAME]
		,[LOGSIZE]
		,[LOGSPACEUSED]
	FROM @tran_log_space_usage



	--------------------------------------------------------------------------
	-- �y�I�������z
	--------------------------------------------------------------------------
	-- <�f�A���P�[�g>

END TRY



	--------------------------------------------------------------------------
	-- �y�ُ폈���z
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
							  ELSE ISNULL(@ErrorProcedure, '') + ' LINE:' + CONVERT(NVARCHAR, @ErrorLine) + ' (' + CAST(ERROR_NUMBER() AS NVARCHAR) + '�F' + @ErrorMessage + ')'
							  END

	SET @ErrorSeverity		= 10

	RAISERROR(@RaiseMessage, @ErrorSeverity, @ErrorState)
END CATCH


GO

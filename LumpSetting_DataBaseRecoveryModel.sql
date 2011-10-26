/*	�y�f�[�^�x�[�X�̕������f�����ꊇ�ŕύX����z
*/

	--------------------------------------------------------------------------
	-- �y�錾�z
	--------------------------------------------------------------------------
	--<�I�v�V�����ݒ�>
	SET NOCOUNT ON

	-- <�p�����[�^>
	DECLARE @RECOVERY_MODEL_TYPE			NVARCHAR(1) = 'F'				-- F:���S�AB:�ꊇ���O�AS:�V���v��

	-- <�ϐ��錾>
	DECLARE @ERROR_MESSAGE					NVARCHAR(4000)

	DECLARE @SQL							NVARCHAR(4000)
	DECLARE @DATABASE						NVARCHAR(100)
	DECLARE @RECOVERY_MODEL					NVARCHAR(20)



	--------------------------------------------------------------------------
	-- �y���������z
	--------------------------------------------------------------------------
BEGIN TRY
	-- <�p�����[�^�`�F�b�N>
	IF (@RECOVERY_MODEL_TYPE IS NULL OR @RECOVERY_MODEL_TYPE = '')
	BEGIN
		SET @ERROR_MESSAGE	= N'�K�{�p�����[�^�A[�������f���敪]���ݒ肳��Ă��܂���B'
		RAISERROR(@ERROR_MESSAGE, 16, 1)
	END
	IF (@RECOVERY_MODEL_TYPE NOT IN ('F', 'B', 'S'))
	BEGIN
		SET @ERROR_MESSAGE	= N'[�������f���敪]�́AF, B, S�̂����ꂩ��ݒ肵�Ă��������B'
		RAISERROR(@ERROR_MESSAGE, 16, 2)
	END


	-- <�������f���ݒ�>
	SET @RECOVERY_MODEL			= CASE @RECOVERY_MODEL_TYPE
								  WHEN 'F' THEN 'FULL'
								  WHEN 'B' THEN 'BULK_LOGGED'
								  WHEN 'S' THEN 'SIMPLE'
								  END

	--------------------------------------------------------------------------
	-- �y�ꊇ�ϊ��z
	--------------------------------------------------------------------------
	-- <�������f�������S�ɖ߂�>
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
		
		PRINT '�f�[�^�x�[�X �u' + @DATABASE + '�v�̕������f�����u' + @RECOVERY_MODEL + '�v�ɕύX���܂����B'


		FETCH NEXT FROM DATABASESCURSOR
		INTO
			 @DATABASE
	END



	--------------------------------------------------------------------------
	-- �y�I�������z
	--------------------------------------------------------------------------
	-- <�f�A���P�[�g>
	CLOSE DATABASESCURSOR
	DEALLOCATE DATABASESCURSOR

END TRY



--------------------------------------------------------------------------
-- �y�ُ폈���z
--------------------------------------------------------------------------
BEGIN CATCH
	-- <�J�[�\���ϐ��f�A���P�[�g>
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


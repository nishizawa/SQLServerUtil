/*	�y�p�[�e�B�V�����쐬�z
	�T�v�F
		�t�@�C���O���[�v�A�t�@�C���A���ʃp�[�e�B�V�����֐����쐬����
		���ʃp�[�e�B�V�����֐��́A���t�^�E�����^�iYYYYMM)�ɑΉ�
*/


	-- <�錾>
	SET NOCOUNT ON

	-- <�p�����[�^>
	DECLARE @DB_NAME								NVARCHAR(50) = 'SAMPLE_DB'
	DECLARE @MONTH_FROM								NVARCHAR(6) = '201104'					-- �쐬����(��)
	DECLARE @MONTH_TO								NVARCHAR(6) = '201203'					-- �쐬����(��)

	DECLARE @FILE_GROUP_PREFIX						NVARCHAR(50) = 'FG_'
	DECLARE @FILE_PATH								NVARCHAR(512) = 'C:\Program Files\Microsoft SQL Server\MSSQL10.SAMPLE_DB\MSSQL\DATA\'
	DECLARE @FILE_EXTENSION							NVARCHAR(512) = '.ndf'
	DECLARE @SIZE									NVARCHAR(128) = '100MB'

	DECLARE @DATE_FUNCTION_NAME						NVARCHAR(128) = 'PF_DATE'
	DECLARE @MONTH_FUNCTION_NAME					NVARCHAR(128) = 'PF_MONTH'

	DECLARE @DATE_SCHEME_NAME						NVARCHAR(128) = 'PS_DATE'
	DECLARE @MONTH_SCHEME_NAME						NVARCHAR(128) = 'PS_MONTH'


	-- <�ϐ��錾>
	DECLARE @MONTH									NVARCHAR(6)
	DECLARE @FILE_GROUP_NAME						NVARCHAR(128)
	DECLARE @FILE_NAME								NVARCHAR(128)
	DECLARE @FILE_FULL_PATH							NVARCHAR(512)

	DECLARE @FIRST_FLG								NVARCHAR(1)
	DECLARE @CRS_MONTH								NVARCHAR(6)
	DECLARE @CRS_FILE_GROUP_NAME					NVARCHAR(128)
	DECLARE @MAX_MONTH								NVARCHAR(6)
	DECLARE @MIN_MONTH								NVARCHAR(6)

	DECLARE @SQL									NVARCHAR(MAX)
	DECLARE @ERROR_MESSAGE							NVARCHAR(4000)

	DECLARE @REGIST_MONTH_LIST			AS TABLE
	(
		 [MONTH]			NVARCHAR(6)
	)

BEGIN TRY
	------------------------------------------------------------------
	-- �y���������z
	------------------------------------------------------------------
	-- <DB�ύX>
	SET @SQL = ''
	SET @SQL = @SQL + ' USE ' + @DB_NAME
	EXEC SP_EXECUTESQL @SQL


	-- <�����l�ݒ�>


	-- <�`�F�b�N>
	IF (@MONTH_FROM > @MONTH_TO)
	BEGIN
		SET @ERROR_MESSAGE = '�쐬����(��)�́A(��)�ȍ~��ݒ肵�Ă��������B'
		RAISERROR(@ERROR_MESSAGE, 16, 1)
	END


	------------------------------------------------------------------
	-- �y�쐬�z
	------------------------------------------------------------------
	SET @MONTH = @MONTH_FROM
	
	WHILE (@MONTH <= @MONTH_TO)
	BEGIN
		-- <�t�@�C���O���[�v�쐬>
		-- �t�@�C���O���[�v���ݒ�
		SET @FILE_GROUP_NAME = @FILE_GROUP_PREFIX + @MONTH
		
		-- �쐬�N�G��
		SET @SQL = ''
		SET @SQL = @SQL + ' USE ' + @DB_NAME + CHAR(13)		-- �� CHAR(13)�͉��s�R�[�h�ł��BPRINT���g���Ɖ��s����ĕ\������܂�
		SET @SQL = @SQL + ' ALTER DATABASE ' + @DB_NAME + CHAR(13)
		SET @SQL = @SQL + ' ADD FILEGROUP ' + @FILE_GROUP_NAME + CHAR(13)
		
		EXEC SP_EXECUTESQL @SQL

--		PRINT @SQL
		PRINT '�t�@�C���O���[�v�i' + @FILE_GROUP_NAME + ')���쐬���܂����B'


		-- <�t�@�C���쐬>
		-- �t�@�C�����ݒ�
		SET @FILE_NAME = @DB_NAME + '_' + @MONTH
		SET @FILE_FULL_PATH = @FILE_PATH + @FILE_NAME + @FILE_EXTENSION
		
		-- �쐬�N�G��
		SET @SQL = ''
		SET @SQL = @SQL + ' USE ' + @DB_NAME + CHAR(13)
		SET @SQL = @SQL + ' ALTER DATABASE ' + @DB_NAME + CHAR(13)
		SET @SQL = @SQL + ' ADD FILE ' + CHAR(13)
		SET @SQL = @SQL + ' ( ' + CHAR(13)
		SET @SQL = @SQL + '      NAME = ''' + @FILE_NAME + '''' + CHAR(13)
		SET @SQL = @SQL + '     ,FILENAME = ''' + @FILE_FULL_PATH + '''' + CHAR(13)
		SET @SQL = @SQL + '     ,SIZE = ' + @SIZE + CHAR(13)
		SET @SQL = @SQL + ' ) ' + CHAR(13)
		SET @SQL = @SQL + ' TO FILEGROUP ' + @FILE_GROUP_NAME + CHAR(13)

		EXEC SP_EXECUTESQL @SQL

--		PRINT @SQL
		PRINT '�t�@�C���i' + @FILE_NAME + ')���쐬���܂����B'

		-- <�o�^���x�i�[>
		INSERT INTO @REGIST_MONTH_LIST
		(
			 [MONTH]
		)
		VALUES
		(
			 @MONTH
		)


		SET @MONTH = CONVERT(CHAR(6), DATEADD(m, 1, @MONTH + '01'), 112)
	END


	------------------------------------------------------------------
	-- �y�p�[�e�B�V�����֐��E�p�[�e�B�V�����\���폜�z
	------------------------------------------------------------------
	-- �쐬�ς݂̏ꍇ�A�͈͂��}�[�W���č폜����
	IF EXISTS (SELECT * FROM sys.partition_functions WHERE name = @DATE_FUNCTION_NAME)
	BEGIN
		-- <�o�^�ς݂̌��x���}�[�W>
		INSERT INTO @REGIST_MONTH_LIST
		(
			 [MONTH]
		)
		SELECT
		     CONVERT(CHAR(6), [range].[value], 112) AS [MONTH]
		FROM sys.partition_range_values AS [range]
		INNER JOIN sys.partition_functions AS [func]
		    ON  [func].[function_id] = [range].[function_id]
		WHERE
		        [func].[name] = @DATE_FUNCTION_NAME
		
		-- <�\�����폜>
		SET @SQL = ''
		SET @SQL = @SQL + ' USE ' + @DB_NAME + CHAR(13)
		SET @SQL = @SQL + ' DROP PARTITION SCHEME ' + @DATE_SCHEME_NAME + CHAR(13)
 		
		EXEC SP_EXECUTESQL @SQL

		-- <�֐����폜>
		SET @SQL = ''
		SET @SQL = @SQL + ' USE ' + @DB_NAME + CHAR(13)
		SET @SQL = @SQL + ' DROP PARTITION FUNCTION ' + @DATE_FUNCTION_NAME + CHAR(13)
 		
		EXEC SP_EXECUTESQL @SQL
	END


	------------------------------------------------------------------
	-- �y�p�[�e�B�V�����֐��쐬�z
	------------------------------------------------------------------
	-- <���t�p�[�e�B�V�����쐬�N�G��>
	SELECT
		 @MAX_MONTH = MAX([MONTH])
		,@MIN_MONTH = MIN([MONTH])
	FROM @REGIST_MONTH_LIST

	DECLARE @MONTH_CRS AS CURSOR
	SET @FIRST_FLG = '1'
	SET @MONTH_CRS = CURSOR
	FOR
		SELECT DISTINCT
			 [MONTH]
		FROM @REGIST_MONTH_LIST
		WHERE
				[MONTH] NOT IN (@MAX_MONTH, @MIN_MONTH)					-- �p�[�e�B�V�����Ȃ̂ŁA���[�̋��E�͐ݒ肵�Ȃ�
		ORDER BY
			 [MONTH]

	SET @SQL = ''
	SET @SQL = @SQL + ' USE ' + @DB_NAME + CHAR(13)
	SET @SQL = @SQL + ' CREATE PARTITION FUNCTION ' + @DATE_FUNCTION_NAME + CHAR(13)
	SET @SQL = @SQL + ' (DATETIME) AS RANGE RIGHT FOR ' + CHAR(13)
	SET @SQL = @SQL + ' VALUES ' + CHAR(13)
	SET @SQL = @SQL + ' ( ' + CHAR(13)

	OPEN @MONTH_CRS
	FETCH NEXT FROM @MONTH_CRS
	INTO
		 @CRS_MONTH

	WHILE (@@FETCH_STATUS = 0)
	BEGIN
		IF (@FIRST_FLG = '1')
		BEGIN
			SET @SQL = @SQL + '  '
			SET @FIRST_FLG = '0'
		END
		ELSE
		BEGIN
			SET @SQL = @SQL + ' ,'
		END

		SET @SQL = @SQL + '''' + CONVERT(NVARCHAR, CONVERT(DATETIME, @CRS_MONTH + '01'), 111) + '''' + CHAR(13)

		FETCH NEXT FROM @MONTH_CRS
		INTO
			 @CRS_MONTH
	END
	CLOSE @MONTH_CRS
	SET @SQL = @SQL + ' ) ' + CHAR(13)

	PRINT @SQL
	EXEC SP_EXECUTESQL @SQL


	-- <���p�[�e�B�V�����쐬�N�G��>
	SET @FIRST_FLG = '1'
	SET @MONTH_CRS = CURSOR
	FOR
		SELECT DISTINCT
			 [MONTH]
		FROM @REGIST_MONTH_LIST
		WHERE
				[MONTH] NOT IN (@MAX_MONTH, @MIN_MONTH)					-- �p�[�e�B�V�����Ȃ̂ŁA���[�̋��E�͐ݒ肵�Ȃ�
		ORDER BY
			 [MONTH]

	SET @SQL = ''
	SET @SQL = @SQL + ' USE ' + @DB_NAME + CHAR(13)
	SET @SQL = @SQL + ' CREATE PARTITION FUNCTION ' + @MONTH_FUNCTION_NAME + CHAR(13)
	SET @SQL = @SQL + ' (NVARCHAR(6)) AS RANGE RIGHT FOR ' + CHAR(13)
	SET @SQL = @SQL + ' VALUES ' + CHAR(13)
	SET @SQL = @SQL + ' ( ' + CHAR(13)

	OPEN @MONTH_CRS
	FETCH NEXT FROM @MONTH_CRS
	INTO
		 @CRS_MONTH

	WHILE (@@FETCH_STATUS = 0)
	BEGIN
		IF (@FIRST_FLG = '1')
		BEGIN
			SET @SQL = @SQL + '  '
			SET @FIRST_FLG = '0'
		END
		ELSE
		BEGIN
			SET @SQL = @SQL + ' ,'
		END

		SET @SQL = @SQL + '''' + @CRS_MONTH + '''' + CHAR(13)

		FETCH NEXT FROM @MONTH_CRS
		INTO
			 @CRS_MONTH
	END
	CLOSE @MONTH_CRS
	SET @SQL = @SQL + ' ) ' + CHAR(13)

	PRINT @SQL
	EXEC SP_EXECUTESQL @SQL



	------------------------------------------------------------------
	-- �y�p�[�e�B�V�����\���쐬�z
	------------------------------------------------------------------
	-- <���t�\���쐬�N�G��>
	SET @FIRST_FLG = '1'
	SET @MONTH_CRS = CURSOR
	FOR
		SELECT DISTINCT
			 [MONTH]
		FROM @REGIST_MONTH_LIST
		ORDER BY
			 [MONTH]

	SET @SQL = ''
	SET @SQL = @SQL + ' USE ' + @DB_NAME + CHAR(13)
	SET @SQL = @SQL + ' CREATE PARTITION SCHEME ' + @DATE_SCHEME_NAME + CHAR(13)
	SET @SQL = @SQL + ' AS PARTITION ' + @DATE_FUNCTION_NAME + CHAR(13)
	SET @SQL = @SQL + ' TO ( ' + CHAR(13)

	OPEN @MONTH_CRS
	FETCH NEXT FROM @MONTH_CRS
	INTO
		 @CRS_MONTH

	WHILE (@@FETCH_STATUS = 0)
	BEGIN
		IF (@FIRST_FLG = '1')
		BEGIN
			SET @SQL = @SQL + '  '
			SET @FIRST_FLG = '0'
		END
		ELSE
		BEGIN
			SET @SQL = @SQL + ' ,'
		END

		SET @CRS_FILE_GROUP_NAME = @FILE_GROUP_PREFIX + @CRS_MONTH
		SET @SQL = @SQL + '''' + @CRS_FILE_GROUP_NAME + '''' + CHAR(13)

		FETCH NEXT FROM @MONTH_CRS
		INTO
			 @CRS_MONTH
	END
	CLOSE @MONTH_CRS
	SET @SQL = @SQL + ' ) ' + CHAR(13)

	PRINT @SQL
	EXEC SP_EXECUTESQL @SQL


	-- <���\���쐬�N�G��>
	SET @FIRST_FLG = '1'
	SET @MONTH_CRS = CURSOR
	FOR
		SELECT DISTINCT
			 [MONTH]
		FROM @REGIST_MONTH_LIST
		ORDER BY
			 [MONTH]

	SET @SQL = ''
	SET @SQL = @SQL + ' USE ' + @DB_NAME + CHAR(13)
	SET @SQL = @SQL + ' CREATE PARTITION SCHEME ' + @MONTH_SCHEME_NAME + CHAR(13)
	SET @SQL = @SQL + ' AS PARTITION ' + @MONTH_FUNCTION_NAME + CHAR(13)
	SET @SQL = @SQL + ' TO ( ' + CHAR(13)

	OPEN @MONTH_CRS
	FETCH NEXT FROM @MONTH_CRS
	INTO
		 @CRS_MONTH

	WHILE (@@FETCH_STATUS = 0)
	BEGIN
		IF (@FIRST_FLG = '1')
		BEGIN
			SET @SQL = @SQL + '  '
			SET @FIRST_FLG = '0'
		END
		ELSE
		BEGIN
			SET @SQL = @SQL + ' ,'
		END

		SET @CRS_FILE_GROUP_NAME = @FILE_GROUP_PREFIX + @CRS_MONTH
		SET @SQL = @SQL + '''' + @CRS_FILE_GROUP_NAME + '''' + CHAR(13)

		FETCH NEXT FROM @MONTH_CRS
		INTO
			 @CRS_MONTH
	END
	CLOSE @MONTH_CRS
	SET @SQL = @SQL + ' ) ' + CHAR(13)

	PRINT @SQL
	EXEC SP_EXECUTESQL @SQL



	------------------------------------------------------------------
	-- �y�I�������z
	------------------------------------------------------------------
	DEALLOCATE @MONTH_CRS


END TRY 

BEGIN CATCH
	IF CURSOR_STATUS('variable', '@MONTH_CRS') > -1
	BEGIN
		CLOSE @MONTH_CRS
		DEALLOCATE @MONTH_CRS
	END


	------------------------------------------------------------------------------
	-- �y��O�����z
	------------------------------------------------------------------------------
	DECLARE @ErrorMessage				NVARCHAR(4000)
	DECLARE @ErrorSeverity				INT
	DECLARE @ErrorState					INT
	DECLARE @ErrorProcedure				NVARCHAR(4000)
	DECLARE @ErrorLine					INT

	DECLARE @RaiseMessage				NVARCHAR(4000)

	SET @ErrorMessage		= ERROR_MESSAGE()
	SET @ErrorSeverity		= ERROR_SEVERITY()
	SET @ErrorState			= ERROR_STATE()
    SET @ErrorProcedure		= ERROR_PROCEDURE()
	SET @ErrorLine			= ERROR_LINE()

	SET @RaiseMessage		= ISNULL(@ErrorProcedure, '') + ' LINE:' + CONVERT(NVARCHAR, @ErrorLine) + ' (' + @ErrorMessage + ')'

	RAISERROR(@RaiseMessage, @ErrorSeverity, @ErrorState)
END CATCH




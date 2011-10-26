-- プロセスリスト

SELECT
    [s].[session_id]                         AS [Session ID]
   ,CONVERT(CHAR(1), [s].[is_user_process])  AS [User Process]
   ,[s].[login_name]                         AS [Login]
   ,ISNULL(db_name([p].[dbid]), N'')         AS [Database]
   ,ISNULL([t].[task_state], N'')            AS [Task State]
   ,ISNULL([r].[command], N'')               AS [Command]
   ,ISNULL([s].[program_name], N'')          AS [Application]
   ,ISNULL([w].[wait_duration_ms], 0)        AS [Wait Time (ms)]
   ,ISNULL([w].[wait_type], N'')             AS [Wait Type]
   ,ISNULL([w].[resource_description], N'')  AS [Wait Resource]
   ,ISNULL(CONVERT (varchar, [w].[blocking_session_id]), '') AS [Blocked By]
   ,CASE WHEN r2.session_id IS NOT NULL AND (r.blocking_session_id = 0 OR r.session_id IS NULL) THEN '1' 
    ELSE ''
    END  AS [Head Blocker]
   ,[s].[cpu_time]                           AS [Total CPU (ms)]
   ,([s].[reads] + [s].[writes]) * 8 / 1024  AS [Total Physical I/O (MB)]
   ,[s].[memory_usage] * 8192 / 1024         AS [Memory Use (KB)]
   ,ISNULL([r].[open_transaction_count],0)   AS [Open Transactions]
   ,[s].[login_time]                         AS [Login Time]
   ,[s].[last_request_start_time]            AS [Last Request Start Time]
   ,ISNULL([s].[host_name], N'')             AS [Host Name]
   ,ISNULL([c].[client_net_address], N'')    AS [Net Address]
   ,ISNULL([t].[exec_context_id], 0)         AS [Execution Context ID]
   ,ISNULL([r].[request_id], 0)              AS [Request ID]
   ,ISNULL([g].[name], N'')                  AS [Workload Group]
FROM [sys].[dm_exec_sessions] AS [s]
LEFT OUTER JOIN [sys].[dm_exec_connections] AS [c]
    ON  [s].[session_id] = [c].[session_id]
LEFT OUTER JOIN [sys].[dm_exec_requests] AS [r]
    ON   [s].[session_id] = [r].[session_id]
LEFT OUTER JOIN [sys].[dm_os_tasks] AS [t]
    ON   [r].[session_id] = [t].[session_id]
    AND  [r].[request_id] = [t].[request_id]
LEFT OUTER JOIN 
(
    SELECT
         *
        ,ROW_NUMBER() OVER (PARTITION BY [waiting_task_address] ORDER BY [wait_duration_ms] DESC) AS [row_num]
    FROM [sys].[dm_os_waiting_tasks] 
) AS [w]
    ON  [t].[task_address] = [w].[waiting_task_address]
    AND [w].[row_num] = 1
LEFT OUTER JOIN [sys].[dm_exec_requests] AS [r2]
    ON  [s].[session_id] = [r2].[blocking_session_id]
LEFT OUTER JOIN [sys].[dm_resource_governor_workload_groups] AS [g]
    ON  [g].[group_id] = [s].[group_id]
LEFT OUTER JOIN [sys].[sysprocesses] AS [p]
    ON  [s].[session_id] = [p].[spid]
ORDER BY
    [s].[session_id];

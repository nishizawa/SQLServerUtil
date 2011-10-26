declare @dbid int
select @dbid = db_id()

/*
SELECT O.name,I.name,IUS.*
FROM sys.dm_db_index_usage_stats AS IUS
INNER JOIN sys.objects AS O
	ON	O.object_id = IUS.object_id
INNER JOIN SYS.INDEXES AS I
	ON	I.object_id = IUS.object_id
	AND	I.index_id = IUS.index_id
WHERE database_id = DB_ID(@dbid)
ORDER BY 1,2
*/

-- 使用頻度
select
	'【使用頻度】' AS Title
union select
	'内容：使用頻度（シーク、スキャン、lookupがあると使用されているということ)が低く、更新度（書込負荷）が高い順にリストアップ'
union select
	'対処法：使用頻度がなく、更新があるものは削除候補'

select
	 object_name(s.object_id) as [テーブル名]
	,s.object_id AS [テーブルID]
	, i.name AS [インデックス名]
	, i.index_id AS [インデックスID]
	, case i.type
	  when 0 then 'ヒープ'
	  when 1 then 'クラスタ'
	  when 2 then '非クラスタ'
	  when 3 then 'XML'
	  when 4 then '空間'
	  else ''
	  end as [インデックスタイプ]
	, case when i.is_primary_key = '1'
	  then 'PK'
	  else ''
	  end AS [プライマリキー]
    , s.user_seeks AS [インデックスシーク数]
    , s.user_scans AS [インデックススキャン数]
    , s.user_lookups AS [インデックスlookup数]
    , isnull(convert(nvarchar
		  ,case when isnull(s.last_system_scan, '9998/12/31') < isnull(s.last_system_seek,'9998/12/31')
		   then
			case when isnull(s.last_system_lookup,'9998/12/31') < isnull(s.last_system_scan,'9998/12/31')
			then s.last_system_lookup
			else s.last_system_scan
			end
		  else
			case when isnull(s.last_system_lookup,'9998/12/31') < isnull(s.last_system_seek,'9998/12/31')
			then s.last_system_lookup
			else s.last_system_seek
			end
		  end
		 ,111
	  ),'') as [最終参照日]
    , s.user_updates AS [インデックス更新数]
    , s.system_updates AS [インデックス更新数(SYSTEM)]
from sys.dm_db_index_usage_stats s
,sys.indexes i
where
		database_id = @dbid 
	and objectproperty(s.object_id,'IsUserTable') = 1
	and i.object_id = s.object_id
	and i.index_id = s.index_id
order by
 (user_seeks + user_scans + user_lookups) asc,user_updates desc
 

/*
--- コスト
select
	 object_name(s.object_id) as [テーブル名]
	,s.object_id AS [テーブルID]
	, i.name AS [インデックス名]
	, i.index_id AS [インデックスID]
	, case i.type
	  when 0 then 'ヒープ'
	  when 1 then 'クラスタ'
	  when 2 then '非クラスタ'
	  when 3 then 'XML'
	  when 4 then '空間'
	  else ''
	  end as [インデックスタイプ]
	, case when i.is_primary_key = '1'
	  then 'PK'
	  else ''
	  end AS [プライマリキー]
	,reads=user_seeks + user_scans + user_lookups
	,writes =  user_updates
from sys.dm_db_index_usage_stats s
,sys.indexes i
where
		objectproperty(s.object_id,'IsUserTable') = 1
	and s.object_id = i.object_id
	and i.index_id = s.index_id
	and s.database_id = @dbid
order by reads desc
*/

-- 競合
select
	'【競合①】' AS Title
union select
	'内容：行ロックによる待機頻度、待機時間をリストアップ'
union select
	'対処法：発生度や発生源を追加調査する'
	 
Select
	 object_name(s.object_id) as [テーブル名]
	,s.object_id AS [テーブルID]
	, i.name AS [インデックス名]
	, i.index_id AS [インデックスID]
	, case i.type
	  when 0 then 'ヒープ'
	  when 1 then 'クラスタ'
	  when 2 then '非クラスタ'
	  when 3 then 'XML'
	  when 4 then '空間'
	  else ''
	  end as [インデックスタイプ]
	, case when i.is_primary_key = '1'
	  then 'PK'
	  else ''
	  end AS [プライマリキー]
--	, partition_number
	, s.row_lock_count as [行ロック累積数]
	, s.row_lock_wait_count as [行ロック待機累積数]
	, cast (100.0 * s.row_lock_wait_count / (1 + s.row_lock_count) as numeric(15,2)) as [待機頻度 (%)]
	, s.row_lock_wait_in_ms as [行ロック待機累積時間 (ms)]
	, cast (1.0 * s.row_lock_wait_in_ms / (1 + s.row_lock_wait_count) as numeric(15,2)) as [平均待機時間 (ms)]
	
	, s.page_lock_wait_count as [ページロック待機累積数]
	, cast (100.0 * s.page_lock_wait_count / (1 + s.page_lock_count) as numeric(15,2)) as [待機頻度 (%)]
	, s.page_lock_wait_in_ms as [ページロック待機累積時間 (ms)]
	, cast (1.0 * s.page_lock_wait_in_ms / (1 + s.page_lock_wait_count) as numeric(15,2)) as [平均待機時間 (ms)]
	
from sys.dm_db_index_operational_stats (@dbid, NULL, NULL, NULL) s
,     sys.indexes i
where
		objectproperty(s.object_id,'IsUserTable') = 1
	and i.object_id = s.object_id
	and i.index_id = s.index_id
	and 
	(
			s.row_lock_wait_count > 0
	)
order by row_lock_wait_count desc


select
	'【競合②】' AS Title
union select
	'内容：ラッチによる待機頻度、待機時間をリストアップ'
union select
	'対処法：発生度や発生源を追加調査する'

Select
	 object_name(s.object_id) as [テーブル名]
	,s.object_id AS [テーブルID]
	, i.name AS [インデックス名]
	, i.index_id AS [インデックスID]
	, case i.type
	  when 0 then 'ヒープ'
	  when 1 then 'クラスタ'
	  when 2 then '非クラスタ'
	  when 3 then 'XML'
	  when 4 then '空間'
	  else ''
	  end as [インデックスタイプ]
	, case when i.is_primary_key = '1'
	  then 'PK'
	  else ''
	  end AS [プライマリキー]
--	, partition_number
	, s.page_latch_wait_count as [ページラッチ待機累積数]
	, s.page_latch_wait_in_ms as [ページラッチ待機累積時間 (ms)]
	, cast (1.0 * s.page_latch_wait_in_ms / (1 + s.page_latch_wait_count) as numeric(15,2)) as [平均待機時間 (ms)]
	
	, s.page_IO_latch_wait_count as [ページIOラッチ待機累積数]
	, s.page_IO_latch_wait_in_ms as [ページIOラッチ待機累積時間 (ms)]
	, cast (1.0 * s.page_io_latch_wait_in_ms / (1 + s.page_io_latch_wait_count) as numeric(15,2)) as [平均待機時間 (ms)]
	
	, s.index_lock_promotion_count AS [エスカレーション回数]
from sys.dm_db_index_operational_stats (@dbid, NULL, NULL, NULL) s
,     sys.indexes i
where
		objectproperty(s.object_id,'IsUserTable') = 1
	and i.object_id = s.object_id
	and i.index_id = s.index_id
	and 
	(
			s.page_latch_wait_count > 0
		OR	s.index_lock_promotion_count > 0
	)
order by row_lock_wait_count desc

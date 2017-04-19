SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
SET NOCOUNT ON
IF OBJECT_ID('tempdb..#MessageCountsCompare') IS NOT NULL DROP TABLE #MessageCountsCompare;
IF OBJECT_ID('tempdb..#MessageCounts') IS NOT NULL 
	BEGIN
		SELECT * INTO #MessageCountsCompare FROM #MessageCounts
		DROP TABLE #MessageCounts;
	END

CREATE TABLE #MessageCounts ( SnapshotDateTime DATETIME, Host VARCHAR(50 ), Active INT , ReadyToRun INT , Total INT );
DECLARE @Host VARCHAR (max)   
DECLARE @Table VARCHAR (max)  
DECLARE @cmd NVARCHAR (max)

DECLARE HostCursor CURSOR FOR 
SELECT [Name] FROM [BizTalkMgmtDb].[dbo] .[adm_Host] WHERE [Name] NOT IN ('BizTalkServerApplication', 'BizTalkServerIsolatedHost') ORDER BY 1 
OPEN HostCursor 
FETCH NEXT FROM HostCursor INTO @Host 
WHILE @@FETCH_STATUS = 0 
	BEGIN 
	SET @cmd = ';WITH ' + @Host + '_lastTouched (nID,uidWorkID,uidMessageID,uidSubscriptionID,dtLastTouched,snPriority,uidClassID,dtStartWindow,dtEndWindow,
		dtValid,uidServiceID,uidInstanceID,uidAppInstanceID,uidPortID,snPartRetrieval,fOrderedDelivery,nRetryCount,nReserved,
		uidProcessID,uidActivationID,fFirstMessage,fOptimize)
		AS(
			  SELECT nID,uidWorkID,uidMessageID,uidSubscriptionID, CAST(dtLastTouched AS SMALLDATETIME),snPriority,uidClassID,dtStartWindow,dtEndWindow,
			  dtValid,uidServiceID,uidInstanceID,uidAppInstanceID,uidPortID,snPartRetrieval,fOrderedDelivery,nRetryCount,nReserved,
			  uidProcessID,uidActivationID,fFirstMessage,fOptimize
			  FROM BizTalkMsgBoxDb.dbo.' + @Host + 'Q (NOLOCK) )
		, ' + @Host + '_QueuedUp ( QueuedUp, ID )
		AS (  SELECT COUNT(1), 1
			  FROM ' + @Host + '_lastTouched LT
			  WHERE LT.uidProcessID IS NOT NULL)
		, ' + @Host + '_NotQueuedUp ( NotQueuedUp, ID )
		AS (  SELECT COUNT(1), 1
			  FROM ' + @Host + '_lastTouched LT
			  WHERE LT.uidProcessID IS NULL )
		INSERT INTO #MessageCounts
		SELECT GetDate() AS [SnapshotTime]
			  , ''' + @Host + ''' AS [Host]
			  , QueuedUp AS [' + @Host + '_Active]
			  , (SELECT NotQueuedUp FROM ' + @Host + '_NotQueuedUp) AS [' + @Host + '_ReadyToRun]
			  , QueuedUp + (SELECT NotQueuedUp FROM ' + @Host + '_NotQueuedUp) AS [' + @Host + '_Total]
		FROM ' + @Host + '_QueuedUp'
	--PRINT(@cmd)
	EXEC (@cmd )
FETCH NEXT FROM HostCursor INTO @Host 
END
 
CLOSE HostCursor  
DEALLOCATE HostCursor

IF OBJECT_ID('tempdb..#MessageCountsCompare') IS NULL
	BEGIN
		SELECT c.SnapshotDateTime,
            c.Host ,
            c.Active ,
            c.ReadyToRun ,
            c.Total 
		FROM #MessageCounts c
	END
ELSE 
	BEGIN
		SELECT c.SnapshotDateTime,
            c.Host ,
            c.Active ,
            c.ReadyToRun ,
            c.Total ,
            c.[Total] - comp.[Total] AS [DiffVsLastRun],
            CASE WHEN c.[Total] - comp.[Total] < 0 THEN '(-)' 
					WHEN c.[Total] - comp.[Total] = 0 THEN ''
              ELSE '+' END AS '+/(-)'
			,CONVERT(time, DATEADD(ms, datediff(second,comp.SnapshotDateTime,c.SnapshotDateTime) * 1000, 0)) AS [HH:MM:SS_Ago]
		FROM #MessageCounts c
			INNER JOIN #MessageCountsCompare comp on comp.Host = c.Host
	END
/****** Object:  StoredProcedure [dbo].[UpsertLog]    Script Date: 05/04/2021 15:51:42 ******/
SET ANSI_NULLS ON
GO
PRINT 'INACTIVE SCRIPT'
RETURN
select distinct(requestid) from ProcessingLog



SET QUOTED_IDENTIFIER ON
GO


CREATE FUNCTION ChkPathHash ( @PathHash NVARCHAR(50) )
  RETURNS INT
  AS
  BEGIN
      DECLARE @Present int = 0
        IF EXISTS( Select PathHash From [dbo].[ProcessingLog] Where PathHash=@PathHash) Set @Present = 1   
      RETURN(@Present)
  END
GO



USE [seneca-log-db]
GO

SELECT [dbo].[ChkPathHash] ('///1P+STjeUoxHMXxmTvCA==')
GO



/****** Script for SelectTopNRows command from SSMS  ******/
SELECT [Id]
      ,[PathHash]
      ,[RequestId]
      ,[Path]
      ,[ProcessingDateTime]
      ,[NumberOfAttempts]
      ,[ResultType]
      ,[Acls]
      ,[ETag]
  FROM [dbo].[ProcessingLog] where requestid='c1de3957-45b0-4ed2-ab31-5231cf1dc43c'

  select distinct(resulttype) from [dbo].[ProcessingLog]

--truncate table [dbo].[ProcessingLog]
--truncate table [dbo].[PageRequestRecord]
CREATE PROCEDURE [dbo].[UpsertLog]  @PathHash NVARCHAR(50), @RequestId NVARCHAR(50), @Path NVARCHAR(1024), @ResultType nvarchar(10), @Acls nvarchar(max), @ETag nvarchar(20), @ContentLength bigint AS 
SET NOCOUNT ON 

MERGE [dbo].ProcessingLog AS [Target]
USING (SELECT PathHash = @PathHash) AS [Source] 
    ON [Target].PathHash = [Source].PathHash  --- specifies the condition
WHEN MATCHED THEN
  UPDATE SET [Target].[ProcessingDateTime] = GetDate(), [Target].[NumberOfAttempts] = [Target].[NumberOfAttempts] + 1, [Target].[ResultType] = @ResultType, [Target].[Acls] = @Acls, [Target].[ETag] = @ETag, [Target].[ContentLength] = @ContentLength  --UPDATE STATEMENT
WHEN NOT MATCHED THEN
  INSERT (PathHash, RequestId, [Path], [ProcessingDateTime],[NumberOfAttempts],[ResultType], Acls, Etag, ContentLength) VALUES (@PathHash, @RequestId, @Path, GetDate(), 1, @ResultType, @Acls, @ETag, @ContentLength); --INSERT STATEMENT
 
GO

Insert into ProcessingLog (PathHash, RequestId, Path, ProcessingDateTime, NumberOfAttempts, ResultType, Acls, ETag)  VALUES ('pathhash', 'manual', 'parent32/0000_Depth31_Branch1','2021-04-03 15:12:53.424', 4, 'FILE', 'this is the acls', 'ETag')

delete from ProcessingLog where requestid='manual'


DECLARE	@return_value int

EXEC	@return_value = [dbo].[UpsertLog]
		@PathHash = 'PathHash1',
		@RequestId = N'manual',
		@Path = N'parent32/0000_Depth31_Branch1',
		@ResultType = 'DIRECTORY',
		@Acls = 'ACLACLACL',
		@ETag = 'testetag'

SELECT	'Return Value' = @return_value

select * from ProcessingLog where PathHash='UWjAeF8Mq7OY4lQFS9ZKFw=='
select  Top (20)* from ProcessingLog where requestid='55df6613-1d7d-453c-ae01-e4ee4ba15fe5'
select max(path) from ProcessingLog where requestid='55a628ee-a79f-4382-971b-c1858ffed2ac'

select requestid, [ResultType], count(*) as [Count], min(ProcessingDateTime) as [Start], max(ProcessingDateTime) as [End], max(NumberOfAttempts) as [Max Attempts], 
sum(NumberOfAttempts) as [Total Attempts], DATEDIFF(minute, min(ProcessingDateTime), max(ProcessingDateTime)) AS 'Duration(Min)'  
from ProcessingLog 
where requestid='1b9809f8-b4e8-44e6-ac62-fb46dc31d32d' 
group by requestid, [ResultType]
-- 092ebe30-c87a-4210-939b-5a082425d3c9 parent1 test#1
-- 9fcfdad1-0be1-4e3a-8c8a-b5b4763202ef parent32 test#1
-- 55df6613-1d7d-453c-ae01-e4ee4ba15fe5 parent16 test#1
-- 1fb35b57-5adf-40e6-8f9a-7b7ff8e29539 parent32 test#2
-- 3748c132-f1fa-4d4a-973e-c20faaf71ae9 parent1 test#2
-- b4df4d8b-b4a9-4d8c-a1c8-99e67b327db2 parent16 test#2
-- 1b9809f8-b4e8-44e6-ac62-fb46dc31d32d parent32 test#3
-- e499a7ca-6673-460e-9698-d65d6b2a60ed parent1 test#3
-- 03031b7d-61b0-40f7-a9f3-79f1eb4e6708 parent16 test#3

select distinct(requestid) from ProcessingLog
/****** Object:  Table [dbo].[ProcessingLog1]    Script Date: 05/04/2021 16:57:51 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[ProcessingLog](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[PathHash] [nvarchar](50) not null,
	[RequestId] [nvarchar](50) NOT NULL,
	[Path] [nvarchar](1024) NOT NULL,
	[ProcessingDateTime] [datetime2](7) NOT NULL,
	[NumberOfAttempts] INT,
	[ResultType] [nvarchar](10) not null,
	[Acls] [nvarchar](max) null,
	[ETag] [nvarchar](20) null,
	[ContentLength] bigint null

PRIMARY KEY CLUSTERED 
(
	[PathHash] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE ProcessingLog
ADD ContentLength bigint;


CREATE TABLE [dbo].[PageRequestRecord](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[PageRequestKey] [nvarchar](50) not null,
PRIMARY KEY CLUSTERED 
(
	[PageRequestKey] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

CREATE FUNCTION ChkPage ( @PageRequestKey NVARCHAR(50) )
  RETURNS INT
  AS
  BEGIN
      DECLARE @Present int = 0
        IF EXISTS( Select PageRequestKey From [dbo].[PageRequestRecord] Where PageRequestKey=@PageRequestKey) Set @Present = 1   
      RETURN(@Present)
  END
GO

CREATE PROCEDURE [dbo].[LogPageCompletion]  @PageRequestKey NVARCHAR(50) AS 
SET NOCOUNT ON 

MERGE [dbo].[PageRequestRecord] AS [Target]
USING (SELECT PageRequestKey = @PageRequestKey) AS [Source] 
    ON [Target].PageRequestKey = [Source].PageRequestKey  --- specifies the condition
WHEN NOT MATCHED THEN
  INSERT (PageRequestKey) VALUES (@PageRequestKey); --INSERT STATEMENT
GO

USE [seneca-log-db]
GO


DECLARE	@return_value int
EXEC	@return_value = [dbo].[LogPageCompletion]
		@PageRequestKey = 'PageRequestKey7'
SELECT	'Return Value' = @return_value

select count(*) from PageRequestRecord 
select *  from PageRequestRecord 
--delete from PageRequestRecord
SELECT [dbo].ChkPage ('PageRequestKey7')


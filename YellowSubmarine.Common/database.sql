/****** Object:  StoredProcedure [dbo].[UpsertLog]    Script Date: 05/04/2021 15:51:42 ******/
SET ANSI_NULLS ON
GO

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

delete from ProcessingLog where requestid='5214b7e1-5491-4432-a24c-e3ccd0fdb62f'


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
select  Top (20)* from ProcessingLog where requestid='5b84e540-a84d-491e-affe-b631263a7c5e'
select max(path) from ProcessingLog where requestid='5b84e540-a84d-491e-affe-b631263a7c5e'

select count(*) as [Count], min(ProcessingDateTime) as [Start], max(ProcessingDateTime) as [End], max(NumberOfAttempts) as [Max Attempts], sum(NumberOfAttempts) as [Total Attempts] 
from ProcessingLog 
where requestid='d28884ec-2185-466e-9543-ea43de10073b'

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


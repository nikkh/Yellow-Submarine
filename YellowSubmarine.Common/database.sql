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

SELECT [dbo].[ChkPathHash] ('///1P+STjeUoxHMXxmTvCvA==')
GO



/****** Script for SelectTopNRows command from SSMS  ******/
SELECT Top(1) [Id]
      ,[PathHash]
      ,[RequestId]
      ,[Path]
      ,[ProcessingDateTime]
      ,[NumberOfAttempts]
      ,[ResultType]
      ,[Acls]
      ,[ETag]
  FROM [dbo].[ProcessingLog] where requestid='2ba056df-e387-4fb9-856a-e569bbe252b4'

  select distinct(resulttype) from [dbo].[ProcessingLog]

DECLARE	@return_value int
EXEC	@return_value = [dbo].[CheckPathHash] @PathHash = N'//07G9oFivHPwsfYEnkYVQ=='
SELECT	'Return Value' = @return_value
GO

truncate table [dbo].[ProcessingLog]

CREATE PROCEDURE [dbo].[UpsertLog]  @PathHash NVARCHAR(50), @RequestId NVARCHAR(50), @Path NVARCHAR(1024), @ResultType nvarchar(10), @Acls nvarchar(max), @ETag nvarchar(20) AS 
SET NOCOUNT ON 

MERGE [dbo].ProcessingLog AS [Target]
USING (SELECT PathHash = @PathHash) AS [Source] 
    ON [Target].PathHash = [Source].PathHash  --- specifies the condition
WHEN MATCHED THEN
  UPDATE SET [Target].[ProcessingDateTime] = GetDate(), [Target].[NumberOfAttempts] = [Target].[NumberOfAttempts] + 1, [Target].[ResultType] = @ResultType, [Target].[Acls] = @Acls, [Target].[ETag] =@ETag  --UPDATE STATEMENT
WHEN NOT MATCHED THEN
  INSERT (PathHash, RequestId, [Path], [ProcessingDateTime],[NumberOfAttempts],[ResultType], Acls, Etag) VALUES (@PathHash, @RequestId, @Path, GetDate(), 1, @ResultType, @Acls, @ETag); --INSERT STATEMENT
 
GO

Insert into ProcessingLog (PathHash, RequestId, Path, ProcessingDateTime, NumberOfAttempts, ResultType, Acls, ETag)  VALUES ('pathhash', 'manual', 'parent32/0000_Depth31_Branch1','2021-04-03 15:12:53.424', 4, 'FILE', 'this is the acls', 'ETag')

delete from ProcessingLog


DECLARE	@return_value int

EXEC	@return_value = [dbo].[UpsertLog]
		@PathHash = 'PathHash1',
		@RequestId = N'manual',
		@Path = N'parent32/0000_Depth31_Branch1',
		@ResultType = 'DIRECTORY',
		@Acls = 'ACLACLACL',
		@ETag = 'testetag'

SELECT	'Return Value' = @return_value

GO
select * from ProcessingLog where requestid='2ba056df-e387-4fb9-856a-e569bbe252b4'

select count(*), min(ProcessingDateTime), max(ProcessingDateTime), max(NumberOfAttempts) from ProcessingLog where requestid='2ba056df-e387-4fb9-856a-e569bbe252b4'
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
	[ETag] [nvarchar](20) null
PRIMARY KEY CLUSTERED 
(
	[PathHash] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO


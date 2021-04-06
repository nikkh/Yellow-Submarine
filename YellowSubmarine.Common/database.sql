/****** Object:  StoredProcedure [dbo].[UpsertLog]    Script Date: 05/04/2021 15:51:42 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[UpsertLog]  @RequestId NVARCHAR(50), @Path NVARCHAR(1024) AS 
SET NOCOUNT ON 

MERGE [dbo].ProcessingLog AS [Target]
USING (SELECT RequestId = @RequestId, [Path] = @Path) AS [Source] 
    ON [Target].RequestId = [Source].RequestId and [Target].[Path] = [Source].[Path] --- specifies the condition
WHEN MATCHED THEN
  UPDATE SET [Target].[ProcessingDateTime] = GetDate(), [Target].[NumberOfAttempts] = [Target].[NumberOfAttempts] + 1 --UPDATE STATEMENT
WHEN NOT MATCHED THEN
  INSERT (RequestId, [Path], [ProcessingDateTime],[NumberOfAttempts]) VALUES (@RequestId, @Path, GetDate(), 1); --INSERT STATEMENT
 
GO

Insert into ProcessingLog (RequestId, Path, ProcessingDateTime, NumberOfAttempts)  VALUES ('manual', 'parent32/0000_Depth31_Branch1','2021-04-03 15:12:53.424', 4)

select * from ProcessingLog

DECLARE	@return_value int

EXEC	@return_value = [dbo].[UpsertLog]
		@RequestId = N'manual',
		@Path = N'parent32/0000_Depth31_Branch1'

SELECT	'Return Value' = @return_value

GO
/****** Object:  Table [dbo].[ProcessingLog1]    Script Date: 05/04/2021 16:57:51 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[ProcessingLog](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[RequestId] [nvarchar](50) NOT NULL,
	[Path] [nvarchar](1024) NOT NULL,
	[ProcessingDateTime] [datetime2](7) NOT NULL,
	[NumberOfAttempts] INT
PRIMARY KEY CLUSTERED 
(
	[RequestId] ASC,
	[Path] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO


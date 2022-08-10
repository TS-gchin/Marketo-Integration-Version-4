USE [PivotalED]
GO

/****** Object:  Table [dbo].[Pivotal_Application_Event_Log]    Script Date: 3/9/2017 12:49:29 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [dbo].[Marketo_Integration_Log](
	[Event_Id] [int] IDENTITY(1,1) NOT NULL,
	[Event_Type] [varchar](15) NOT NULL,
	[Description] [nvarchar](4000) NOT NULL,
	[Event_Date] [datetime] NOT NULL,
	[User_Name] [varchar](40) NULL,
	[Primary_Table] [varchar](200) NULL,
	[Record_Id] [varchar](40) NULL,
	[Component_Name] [varchar](100) NULL,
	[Class_Name] [varchar](100) NULL,
	[Method_Name] [varchar](8000) NULL,
	[Other] [text] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

GO

SET ANSI_PADDING OFF
GO



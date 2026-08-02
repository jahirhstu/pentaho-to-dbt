SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [etl].[etl_run_control](
	[etl_run_id] [int] IDENTITY(1,1) NOT NULL,
	[etl_name] [varchar](100) NOT NULL,
	[last_successful_run_time] [datetime2](3) NULL,
	[current_run_start_time] [datetime2](3) NULL,
	[current_run_end_time] [datetime2](3) NULL,
	[run_status] [varchar](30) NOT NULL,
	[records_inserted] [int] NOT NULL,
	[records_updated] [int] NOT NULL,
	[records_rejected] [int] NOT NULL,
	[error_message] [varchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
ALTER TABLE [etl].[etl_run_control] ADD PRIMARY KEY CLUSTERED 
(
	[etl_run_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
ALTER TABLE [etl].[etl_run_control] ADD  DEFAULT ((0)) FOR [records_inserted]
GO
ALTER TABLE [etl].[etl_run_control] ADD  DEFAULT ((0)) FOR [records_updated]
GO
ALTER TABLE [etl].[etl_run_control] ADD  DEFAULT ((0)) FOR [records_rejected]
GO

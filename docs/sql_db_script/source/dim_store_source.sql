SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dim].[dim_store_source](
	[store_id] [int] NOT NULL,
	[store_code] [varchar](30) NOT NULL,
	[store_name] [varchar](100) NOT NULL,
	[region_id] [int] NOT NULL,
	[is_active] [bit] NOT NULL,
	[source_updated_at] [datetime2](3) NOT NULL
) ON [PRIMARY]
GO
ALTER TABLE [dim].[dim_store_source] ADD PRIMARY KEY CLUSTERED 
(
	[store_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dim].[dim_product_source](
	[product_id] [int] NOT NULL,
	[product_code] [varchar](30) NOT NULL,
	[product_name] [varchar](100) NOT NULL,
	[category] [varchar](50) NOT NULL,
	[unit_price] [decimal](10, 2) NOT NULL,
	[is_active] [bit] NOT NULL,
	[source_updated_at] [datetime2](3) NOT NULL
) ON [PRIMARY]
GO
ALTER TABLE [dim].[dim_product_source] ADD PRIMARY KEY CLUSTERED 
(
	[product_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [staging].[stg_product_sales](
	[sales_id] [int] NOT NULL,
	[order_number] [varchar](40) NOT NULL,
	[product_id] [int] NOT NULL,
	[product_name] [varchar](100) NOT NULL,
	[category] [varchar](50) NOT NULL,
	[store_id] [int] NOT NULL,
	[sales_date] [date] NOT NULL,
	[quantity] [int] NOT NULL,
	[sales_amount] [decimal](12, 2) NOT NULL,
	[source_updated_at] [datetime2](3) NOT NULL,
	[etl_run_id] [int] NOT NULL,
	[loaded_at] [datetime2](3) NOT NULL
) ON [PRIMARY]
GO
ALTER TABLE [staging].[stg_product_sales] ADD PRIMARY KEY CLUSTERED 
(
	[sales_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO

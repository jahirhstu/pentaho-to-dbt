SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [reporting].[rpt_product_sales_summary](
	[product_id] [int] NOT NULL,
	[product_name] [varchar](100) NOT NULL,
	[category] [varchar](50) NOT NULL,
	[total_orders] [int] NOT NULL,
	[total_quantity] [int] NOT NULL,
	[total_sales_amount] [decimal](14, 2) NOT NULL,
	[first_sale_date] [date] NOT NULL,
	[last_sale_date] [date] NOT NULL,
	[etl_run_id] [int] NOT NULL,
	[created_at] [datetime2](3) NOT NULL,
	[updated_at] [datetime2](3) NOT NULL
) ON [PRIMARY]
GO
ALTER TABLE [reporting].[rpt_product_sales_summary] ADD PRIMARY KEY CLUSTERED 
(
	[product_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO

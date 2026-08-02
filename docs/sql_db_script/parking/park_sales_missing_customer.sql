SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [parking].[park_sales_missing_customer](
	[park_id] [int] IDENTITY(1,1) NOT NULL,
	[sales_id] [int] NOT NULL,
	[order_number] [varchar](40) NOT NULL,
	[customer_id] [int] NOT NULL,
	[product_id] [int] NOT NULL,
	[store_id] [int] NOT NULL,
	[sales_date] [date] NOT NULL,
	[quantity] [int] NOT NULL,
	[unit_price] [decimal](10, 2) NOT NULL,
	[discount_amount] [decimal](10, 2) NOT NULL,
	[source_updated_at] [datetime2](3) NOT NULL,
	[rejection_reason] [varchar](200) NOT NULL,
	[etl_run_id] [int] NOT NULL,
	[parked_at] [datetime2](3) NOT NULL,
	[is_resolved] [bit] NOT NULL,
	[resolved_at] [datetime2](3) NULL
) ON [PRIMARY]
GO
ALTER TABLE [parking].[park_sales_missing_customer] ADD PRIMARY KEY CLUSTERED 
(
	[park_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
CREATE UNIQUE NONCLUSTERED INDEX [UX_park_sales_missing_customer_active] ON [parking].[park_sales_missing_customer]
(
	[sales_id] ASC
)
WHERE ([is_resolved]=(0))
WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
ALTER TABLE [parking].[park_sales_missing_customer] ADD  DEFAULT ((0)) FOR [is_resolved]
GO

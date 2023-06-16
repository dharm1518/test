-- FUNCTION: public.sp_get_ba_t1(integer, date, date, text, text, integer)
-- DROP FUNCTION public.sp_get_ba_t1(integer, date, date, text, text, integer);
CREATE
OR REPLACE FUNCTION public.sp_get_ba_t1(
	client_id_arg integer,
	startdate_arg date,
	enddate_arg date,
	category_arg text,
	brand_arg text,
	channel_id_arg integer
) RETURNS SETOF json LANGUAGE 'plpgsql' COST 100 VOLATILE PARALLEL UNSAFE ROWS 1000 AS $ BODY $
/*
 Author : Dharmendra
 Dated: 2023-04-01
 Purpose: This sql fetches data for BA report
 Arguments: 
 
 client_id_arg -> clientID
 startdate_arg -> startDate ( closest to current date )
 enddate_arg -> endDate
 category_arg -> categoryName
 brand_arg -> brandName
 channel_id_arg -> channelID
 
 Notes: This query should run on Reporting DB
 select * from public.sp_get_ba_t1(65, '2023-04-30', '2023-04-01', 'vitamins & supplements','zingavita',1)
 */
declare pre_end_date date := enddate_arg - 7;

declare end_date_t1 date := enddate_arg - 1;

begin discard temp;

--report_date date:= 'select distinct crawl_date from cte_dq_data order by 1;'
--selecting last week start_date-------
/*select enddate_arg - 7 into pre_end_date;	*/
raise notice '%',
pre_end_date;

raise notice '%',
end_date_t1;

---VCPm PFM Master data---
create temp table cte_pfm_vcm as
select
	sku_title,
	client_id,
	channel_id,
	channel_sku_id,
	category,
	sub_category
from
	e4_entity_imported_foreign_tables.pfm_vcm_master vcm
where
	vcm.client_id = client_id_arg
	and channel_id = channel_id_arg
	and lower(vcm.country_code) =(
		select
			lower(country)
		from
			e4_entity_imported_foreign_tables.client_master
		where
			id = client_id_arg
	)
	and lower(vcm.category) = category_arg;

--we are assuming that brand names are correct in client sku mapping table.
--Table client_level_brand_sku_mapping
create temp table cte_get_sku_map as
select
	re.channel_sku_id,
	re.channel_id,
	trim(lower(brand)) as brand
from
	reporting.client_level_brand_sku_mapping re
where
	re.client_id = client_id_arg
	and re.channel_id = channel_id_arg;

--Brand sku competition Product data ---
create temp table comp_sku_map as
select
	client_id,
	channel_id,
	asin,
	prod_target,
	type
from
	reporting.comp_sku_list_month_level
where
	client_id = client_id_arg
	and channel_id = channel_id_arg
	and lower(country_code) =(
		select
			lower(country)
		from
			e4_entity_imported_foreign_tables.client_master
		where
			id = client_id_arg
	)
	and year = extract(
		year
		from
			enddate_arg
	)
	and month = extract(
		month
		from
			enddate_arg
	);

---Joining of PFM and client brand_sku mapping--			
create temp table cte_pfm_client_sku_map as
select
	vcm.*,
	trim(lower(brand)) as brand
from
	cte_pfm_vcm vcm
	inner join cte_get_sku_map re on (
		vcm.channel_sku_id = re.channel_sku_id
		and vcm.channel_id = re.channel_id
	);

---sku_analytics table--					
create temp table cte_get_sku_analytics as
select
	sa.sku,
	sa.channel_id,
	sa.crawl_date,
	dq_score,
	coalesce(organic_impressions, 0) as organic_impressions,
	sa.visibility_score,
	availability,
	availability_score,
	buyability_score,
	pricing_score,
	coalesce(upd, 0) as upd,
	coalesce(price, 0) as price,
	coalesce(paid_impressions, 0) as paid_impressions,
	coalesce(overall_impressions, 0) as overall_impressions,
	coalesce(revenue, 0) as revenue,
	bsr_2,
	rating,
	coalesce (rating_count, 0) rating_count,
	buybox,
	review
from
	reporting.sku_analytics sa
where
	crawl_date between startdate_arg - interval '28 days'
	and startdate_arg
	and sa.client_id = client_id_arg
	and sa.channel_id = channel_id_arg;

--select * from cte_get_sku_analytics
--enddate_arg::date - ((startdate_arg::date - enddate_arg::date) + 1)
--joining of sku_analytics and PFM table---			
create temp table cte_dq_data as
select
	sku_title,
	category,
	sub_category,
	pfm.brand,
	sa.sku,
	sa.channel_id,
	sa.crawl_date,
	dq_score,
	organic_impressions,
	sa.visibility_score,
	availability_score,
	buyability_score,
	pricing_score,
	upd as unit_per_day,
	price,
	paid_impressions,
	revenue,
	overall_impressions,
	bsr_2,
	availability,
	rating,
	rating_count,
	buybox,
	review
from
	cte_get_sku_analytics sa
	inner join cte_pfm_client_sku_map pfm on (
		sa.sku = pfm.channel_sku_id
		and sa.channel_id = pfm.channel_id
	);

/*select max(rating_count) from cte_dq_data where sku='B0BP2M7CCS' 
 and crawl_date = '2023-05-09'
 select distinct crawl_date from cte_dq_data order by 1*/
--- DQ calculation New temp table-----between pre_end_date and  enddate_arg -1
create temp table cte_dq_data_ as
SELECT
	sku,
	sub_category,
	COALESCE(
		avg(dq_score) FILTER (
			WHERE
				crawl_date >= enddate_arg
		),
		0
	) AS curr_avg_dq,
	COALESCE(
		avg(dq_score) FILTER (
			WHERE
				crawl_date BETWEEN pre_end_date
				AND enddate_arg - 1
		),
		0
	) AS prev_avg_dq,
	COALESCE(
		sum(organic_impressions) FILTER (
			WHERE
				crawl_date >= enddate_arg
		),
		0
	) AS curr_avg_org_impr,
	COALESCE(
		sum(organic_impressions) FILTER (
			WHERE
				crawl_date BETWEEN pre_end_date
				AND enddate_arg - 1
		),
		0
	) AS prev_avg_org_impr,
	COALESCE(
		sum(paid_impressions) FILTER (
			WHERE
				crawl_date >= enddate_arg
		),
		0
	) AS curr_avg_paid_impr,
	COALESCE(
		sum(paid_impressions) FILTER (
			WHERE
				crawl_date BETWEEN pre_end_date
				AND enddate_arg - 1
		),
		0
	) AS prev_avg_paid_impr,
	COALESCE(
		avg(visibility_score) FILTER (
			WHERE
				crawl_date >= enddate_arg
		),
		0
	) AS curr_avg_visibility_score,
	COALESCE(
		avg(visibility_score) FILTER (
			WHERE
				crawl_date BETWEEN pre_end_date
				AND enddate_arg - 1
		),
		0
	) AS prev_avg_visibility_score,
	COALESCE(
		avg(availability_score) FILTER (
			WHERE
				crawl_date >= enddate_arg
		),
		0
	) AS curr_avg_availability_score,
	COALESCE(
		avg(availability_score) FILTER (
			WHERE
				crawl_date BETWEEN pre_end_date
				AND enddate_arg - 1
		),
		0
	) AS prev_avg_availability_score,
	COALESCE(
		avg(buyability_score) FILTER (
			WHERE
				crawl_date >= enddate_arg
		),
		0
	) AS curr_avg_buyability_score,
	COALESCE(
		avg(buyability_score) FILTER (
			WHERE
				crawl_date BETWEEN pre_end_date
				AND enddate_arg - 1
		),
		0
	) AS prev_avg_buyability_score,
	COALESCE(
		avg(pricing_score) FILTER (
			WHERE
				crawl_date >= enddate_arg
		),
		0
	) AS curr_avg_pricing_score,
	COALESCE(
		avg(pricing_score) FILTER (
			WHERE
				crawl_date BETWEEN pre_end_date
				AND enddate_arg - 1
		),
		0
	) AS prev_avg_pricing_score
FROM
	cte_dq_data
WHERE
	brand = brand_arg
GROUP BY
	1,
	2;

--------------------------section 1---------------------------
create temp table cte_get_ms_sov as
SELECT
	COALESCE(
		(
			(
				SUM(revenue) FILTER (
					WHERE
						crawl_date >= enddate_arg
						AND brand = brand_arg
				)
			) / NULLIF(
				SUM(revenue) FILTER (
					WHERE
						crawl_date >= enddate_arg
				),
				0
			) :: decimal * 100
		) :: numeric(15, 2),
		0
	) AS curr_ms_value,
	COALESCE(
		(
			(
				SUM(unit_per_day) FILTER (
					WHERE
						crawl_date >= enddate_arg
						AND brand = brand_arg
				)
			) / NULLIF(
				SUM(unit_per_day) FILTER (
					WHERE
						crawl_date >= enddate_arg
				),
				0
			) :: decimal * 100
		) :: numeric(15, 2),
		0
	) AS curr_ms_vol,
	COALESCE(
		(
			(
				SUM(revenue) FILTER (
					WHERE
						crawl_date BETWEEN pre_end_date
						AND enddate_arg - 1
						AND brand = brand_arg
				)
			) / NULLIF(
				SUM(revenue) FILTER (
					WHERE
						crawl_date BETWEEN pre_end_date
						AND enddate_arg - 1
				),
				0
			) :: decimal * 100
		) :: numeric(15, 2),
		0
	) AS prev_ms_value,
	COALESCE(
		(
			(
				SUM(unit_per_day) FILTER (
					WHERE
						crawl_date BETWEEN pre_end_date
						AND enddate_arg - 1
						AND brand = brand_arg
				)
			) / NULLIF(
				SUM(unit_per_day) FILTER (
					WHERE
						crawl_date BETWEEN pre_end_date
						AND enddate_arg - 1
				),
				0
			) :: decimal * 100
		) :: numeric(15, 2),
		0
	) AS prev_ms_vol,
	COALESCE(
		(
			(
				SUM(paid_impressions) FILTER (
					WHERE
						crawl_date >= enddate_arg
						AND brand = brand_arg
				)
			) / NULLIF(
				SUM(paid_impressions) FILTER (
					WHERE
						crawl_date >= enddate_arg
				),
				0
			) :: decimal * 100
		) :: numeric(15, 2),
		0
	) AS curr_paid_sov,
	COALESCE(
		(
			(
				SUM(organic_impressions) FILTER (
					WHERE
						crawl_date >= enddate_arg
						AND brand = brand_arg
				)
			) / NULLIF(
				SUM(organic_impressions) FILTER (
					WHERE
						crawl_date >= enddate_arg
				),
				0
			) :: decimal * 100
		) :: numeric(15, 2),
		0
	) AS curr_org_sov,
	COALESCE(
		(
			(
				SUM(paid_impressions) FILTER (
					WHERE
						crawl_date BETWEEN pre_end_date
						AND enddate_arg - 1
						AND brand = brand_arg
				)
			) / NULLIF(
				SUM(paid_impressions) FILTER (
					WHERE
						crawl_date BETWEEN pre_end_date
						AND enddate_arg - 1
				),
				0
			) :: decimal * 100
		) :: numeric(15, 2),
		0
	) AS prev_paid_sov,
	COALESCE(
		(
			(
				SUM(organic_impressions) FILTER (
					WHERE
						crawl_date BETWEEN pre_end_date
						AND enddate_arg - 1
						AND brand = brand_arg
				)
			) / NULLIF(
				SUM(organic_impressions) FILTER (
					WHERE
						crawl_date BETWEEN pre_end_date
						AND enddate_arg - 1
				),
				0
			) :: decimal * 100
		) :: numeric(15, 2),
		0
	) AS prev_org_sov
FROM
	cte_dq_data;

--select * from cte_get_ms_sov   "s2.total_units" 
---Top 3 MS Brands--
create temp table ms_top_brand_ranks as
select
	*
from
	(
		select
			brand,
			dense_rank() over (
				order by
					COALESCE(sum(unit_per_day), 0) desc
			) ranks
		from
			cte_dq_data
		where
			brand not in(brand_arg)
		group by
			brand
	) t
where
	ranks <= 3;

---Top 3 SOV Brands--
create temp table sov_top_brand_ranks as
select
	*
from
	(
		select
			brand,
			dense_rank() over (
				order by
					COALESCE(sum(overall_impressions), 0) desc
			) ranks
		from
			cte_dq_data
		where
			brand not in(brand_arg)
		group by
			brand
	) t
where
	ranks <= 3;

create temp table cte_get_cat_total_units as
select
	crawl_date,
	COALESCE(sum(unit_per_day), 0) as total_units,
	COALESCE(sum(overall_impressions), 0) as total_impressions
from
	cte_dq_data
group by
	1;

create temp table sum_upd_brand as
select
	crawl_date,
	brand,
	COALESCE(sum(unit_per_day), 0) day_upd,
	COALESCE(sum(overall_impressions), 0) as day_impressions
from
	cte_dq_data
group by
	crawl_date,
	brand;

create temp table ms_date_percent as
select
	s1.crawl_date,
	brand,
	day_upd,
	COALESCE(total_units) total_units
from
	sum_upd_brand s1
	inner join cte_get_cat_total_units s2 on (s1.crawl_date = s2.crawl_date)
where
	s1.crawl_date >= enddate_arg
	and brand in (
		select
			brand
		from
			ms_top_brand_ranks
		union
		all
		select
			brand_arg
	) --group by 1,day_upd,total_units,brand	
order by
	1;

--Brand chart data--				
create temp table sov_date_percent as
select
	s1.crawl_date,
	brand,
	day_impressions,
	total_impressions
from
	sum_upd_brand s1
	inner join cte_get_cat_total_units s2 on (s1.crawl_date = s2.crawl_date)
where
	s1.crawl_date >= enddate_arg
	and brand in (
		select
			brand
		from
			sov_top_brand_ranks
		union
		all
		select
			brand_arg
	) --group by 1,day_impressions,total_impressions,brand	
order by
	1;

---waterfall_1_Brand wise--- curr_vol
/*create temp table waterfall_1_category_data as 
 select  category_arg as category
 ,(sum(unit_per_day)filter(where crawl_date >= enddate_arg))::numeric(15,2) as cat_curr_value
 ,(sum(unit_per_day)filter(where crawl_date< enddate_arg))::numeric(15,2) as cat_prev_value
 ,(sum(unit_per_day)filter(where crawl_date< enddate_arg))::numeric(15,2) - (sum(unit_per_day)filter(where crawl_date >= enddate_arg))::numeric(15,2) as cat_diff
 from cte_dq_data
 ;*/
create temp table waterfall_1_brand_data as
SELECT
	category_arg AS category,
	brand,
	COALESCE(
		SUM(day_upd) FILTER (
			WHERE
				crawl_date >= enddate_arg
		),
		0
	) AS current_upd,
	COALESCE(
		SUM(day_upd) FILTER (
			WHERE
				crawl_date BETWEEN pre_end_date
				AND enddate_arg - 1
		),
		0
	) AS prev_upd,
	COALESCE(
		(
			(
				(
					SUM(day_upd) FILTER (
						WHERE
							crawl_date >= enddate_arg
					)
				) - SUM(day_upd) FILTER (
					WHERE
						crawl_date BETWEEN pre_end_date
						AND enddate_arg - 1
				)
			) / NULLIF(
				SUM(day_upd) FILTER (
					WHERE
						crawl_date BETWEEN pre_end_date
						AND enddate_arg - 1
				),
				0
			) :: decimal * 100
		) :: numeric(15, 2),
		0
	) AS brand_change
FROM
	sum_upd_brand
WHERE
	brand IN (
		SELECT
			brand
		FROM
			ms_top_brand_ranks
		UNION
		ALL
		SELECT
			brand_arg
	)
GROUP BY
	1,
	2;

--select * from waterfall_1_brand_data
create temp table waterfall_1_other_data as
select
	category_arg as category,
	'others' as brand,
	0 -(
		select
			sum(brand_change)
		from
			waterfall_1_brand_data
	) :: numeric(15, 2) as ms_others;

create temp table waterfall_1_data as
select
	100 as cat_curr_value,
	100 as cat_prev_value,
	b.brand,
	brand_change,
	o.brand as other_brands,
	ms_others
from
	waterfall_1_brand_data b
	join waterfall_1_other_data o on o.category = b.category;

---------------------------section 3 ---DQ score for category---
create temp table dq_brand_cal as
SELECT
	COALESCE(
		(
			SUM(curr_avg_dq * curr_avg_org_impr) / NULLIF(SUM(curr_avg_org_impr), 0)
		) :: numeric(15, 2),
		0
	) AS curr_dq_score,
	COALESCE(
		(
			SUM(prev_avg_dq * prev_avg_org_impr) / NULLIF(SUM(prev_avg_org_impr), 0)
		) :: numeric(15, 2),
		0
	) AS prev_dq,
	COALESCE(
		(
			SUM(curr_avg_visibility_score * curr_avg_org_impr) / NULLIF(SUM(curr_avg_org_impr), 0)
		) :: numeric(15, 2),
		0
	) AS curr_visibility_score,
	COALESCE(
		(
			SUM(prev_avg_visibility_score * prev_avg_org_impr) / NULLIF(SUM(prev_avg_org_impr), 0)
		) :: numeric(15, 2),
		0
	) AS prev_visibility_score,
	COALESCE(
		(
			SUM(curr_avg_availability_score * curr_avg_org_impr) / NULLIF(SUM(curr_avg_org_impr), 0)
		) :: numeric(15, 2),
		0
	) AS curr_availability,
	COALESCE(
		(
			SUM(prev_avg_availability_score * prev_avg_org_impr) / NULLIF(SUM(prev_avg_org_impr), 0)
		) :: numeric(15, 2),
		0
	) AS prev_availability,
	COALESCE(
		(
			SUM(curr_avg_buyability_score * curr_avg_org_impr) / NULLIF(SUM(curr_avg_org_impr), 0)
		) :: numeric(15, 2),
		0
	) AS curr_buyability,
	COALESCE(
		(
			SUM(prev_avg_buyability_score * prev_avg_org_impr) / NULLIF(SUM(prev_avg_org_impr), 0)
		) :: numeric(15, 2),
		0
	) AS prev_buyability,
	COALESCE(
		(
			SUM(curr_avg_pricing_score * curr_avg_org_impr) / NULLIF(SUM(curr_avg_org_impr), 0)
		) :: numeric(15, 2),
		0
	) AS curr_pricing,
	COALESCE(
		(
			SUM(prev_avg_pricing_score * prev_avg_org_impr) / NULLIF(SUM(prev_avg_org_impr), 0)
		) :: numeric(15, 2),
		0
	) AS prev_pricing
FROM
	cte_dq_data_;

--where brand = brand_arg
--select *from dq_brand_cal
-------------------subcat data------------------------
----get top 3 subcat--
create temp table cat_vol_chan as
SELECT
	*
FROM
	(
		select
			sub_category,
			COALESCE(
				sum(unit_per_day) filter(
					where
						crawl_date >= enddate_arg
						and brand = brand_arg
				),
				0
			) as curr_vol,
			COALESCE(
				(
					(
						SUM(unit_per_day) FILTER (
							WHERE
								crawl_date >= enddate_arg
								AND brand = brand_arg
						)
					) / NULLIF(
						SUM(unit_per_day) FILTER (
							WHERE
								crawl_date >= enddate_arg
						),
						0
					) :: decimal * 100
				) :: numeric(15, 2),
				0
			) AS curr_ms_share,
			COALESCE(
				(
					(
						SUM(unit_per_day) FILTER (
							WHERE
								crawl_date BETWEEN pre_end_date
								AND enddate_arg - 1
								AND brand = brand_arg
						)
					) / NULLIF(
						SUM(unit_per_day) FILTER (
							WHERE
								crawl_date BETWEEN pre_end_date
								AND enddate_arg - 1
						),
						0
					) :: decimal * 100
				) :: numeric(15, 2),
				0
			) AS prev_ms_share,
			COALESCE(
				SUM(unit_per_day) FILTER (
					WHERE
						crawl_date >= enddate_arg
				),
				0
			) AS total_subcat_upd
		FROM
			cte_dq_data
		GROUP BY
			1
	) a
WHERE
	curr_vol IS NOT null
ORDER BY
	curr_vol desc
LIMIT
	3;

--get chart data for each subcat
create temp table sum_upd_subcat as
select
	crawl_date,
	sub_category,
	COALESCE(sum(unit_per_day), 0) day_upd
from
	cte_dq_data
where
	crawl_date >= enddate_arg
	and sub_category in(
		select
			distinct sub_category
		from
			cat_vol_chan
	)
group by
	1,
	2
order by
	sub_category,
	crawl_date;

--select * from sum_upd_subcat
--for middle box of each subcat--get denominator
create temp table sum_subcat_vol as
select
	sub_category,
	COALESCE(sum(day_upd), 0) sub_cat_vol
from
	sum_upd_subcat
where
	crawl_date >= enddate_arg
group by
	1;

--select * from sum_subcat_vol
create temp table brand_vol as
select
	brand,
	sub_category,
	COALESCE(sum(unit_per_day), 0) brand_vol
from
	cte_dq_data s1
where
	crawl_date >= enddate_arg
	and sub_category in(
		select
			distinct sub_category
		from
			cat_vol_chan
	)
group by
	1,
	2;

--select * from brand_vol where sub_category='core'
create temp table brand_cat_vol as
select
	bv.brand,
	bv.sub_category,
	COALESCE(
		(
			(bv.brand_vol) / NULLIF(ssv.sub_cat_vol, 0) :: decimal * 100
		) :: numeric(15, 2),
		0
	) AS sub_cat_brand_percent
from
	brand_vol AS bv
	JOIN sum_subcat_vol AS ssv ON (bv.sub_category = ssv.sub_category)
GROUP by
	1,
	2,
	ssv.sub_cat_vol,
	bv.brand_vol;

--select * from brand_cat_vol
--get top 5 brands
create temp table cte_get_top5_brands as
select
	*
from
	(
		select
			brand,
			sub_category,
			sub_cat_brand_percent,
			dense_rank () over (
				partition by sub_category
				order by
					sub_cat_brand_percent desc
			) as brand_rank
		from
			brand_cat_vol
	) b5
where
	brand_rank <= 5;

--waterfall_2_ subcategory--  
--create subcategory waterfall data
create temp table waterfall_2_subcategory_data as
select
	COALESCE(
		(
			(
				SUM(unit_per_day) FILTER (
					WHERE
						crawl_date >= enddate_arg
						AND brand = brand_arg
				)
			) / NULLIF(
				SUM(unit_per_day) FILTER (
					WHERE
						crawl_date >= enddate_arg
				),
				0
			) :: decimal * 100
		) :: numeric(15, 2),
		0
	) AS cat_curr_value,
	COALESCE(
		(
			(
				SUM(unit_per_day) FILTER (
					WHERE
						crawl_date BETWEEN pre_end_date
						AND enddate_arg - 1
						AND brand = brand_arg
				)
			) / NULLIF(
				SUM(unit_per_day) FILTER (
					WHERE
						crawl_date BETWEEN pre_end_date
						AND enddate_arg - 1
				),
				0
			) :: decimal * 100
		) :: numeric(15, 2),
		0
	) AS cat_prev_value
from
	cte_dq_data s1;

create temp table waterfall_2_subcat_data as
select
	sub_category,
	COALESCE(
		(
			(
				SUM(unit_per_day) FILTER (
					WHERE
						crawl_date >= enddate_arg
						AND brand = brand_arg
				)
			) / NULLIF(
				SUM(unit_per_day) FILTER (
					WHERE
						crawl_date >= enddate_arg
				),
				0
			) :: decimal * 100
		) :: numeric(15, 2),
		0
	) as subcat_curr_value,
	COALESCE(
		(
			(
				SUM(unit_per_day) FILTER (
					WHERE
						crawl_date BETWEEN pre_end_date
						AND enddate_arg - 1
						AND brand = brand_arg
				)
			) / NULLIF(
				SUM(unit_per_day) FILTER (
					WHERE
						crawl_date BETWEEN pre_end_date
						AND enddate_arg - 1
				),
				0
			) :: decimal * 100
		) :: numeric(15, 2),
		0
	) as subcat_prev_value
from
	cte_dq_data
group by
	1;

create temp table waterfall_2_data as
SELECT
	*
FROM
	(
		select
			(
				select
					cat_curr_value
				from
					waterfall_2_subcategory_data
			) as cat_curr_value,
			(
				select
					cat_prev_value
				from
					waterfall_2_subcategory_data
			) as cat_prev_value,
			--(select subcat_change from waterfall_2_other_data) as other_brands
			sub_category,
			subcat_curr_value,
			subcat_prev_value
		from
			waterfall_2_subcat_data
	) sc
where
	sub_category in (
		select
			distinct sub_category
		from
			sum_subcat_vol
	);

---------------------DQ score for each subcat------
create temp table dq_sub_cat as
select
	sub_category,
	COALESCE(
		(
			(SUM(curr_avg_dq * curr_avg_org_impr)) / NULLIF(SUM(curr_avg_org_impr), 0) :: float
		) :: numeric(15, 2),
		0
	) as curr_dq_score,
	COALESCE(
		(
			(SUM(prev_avg_dq * prev_avg_org_impr)) / NULLIF(SUM(prev_avg_org_impr), 0) :: float
		) :: numeric(15, 2),
		0
	) as prev_dq,
	COALESCE(
		(
			(
				SUM(curr_avg_visibility_score * curr_avg_org_impr)
			) / NULLIF(SUM(curr_avg_org_impr), 0) :: float
		) :: numeric(15, 2),
		0
	) as curr_visibility_score,
	COALESCE(
		(
			(
				SUM(prev_avg_visibility_score * prev_avg_org_impr)
			) / NULLIF(SUM(prev_avg_org_impr), 0) :: float
		) :: numeric(15, 2),
		0
	) as prev_visibility_score,
	COALESCE(
		(
			(
				SUM(curr_avg_availability_score * curr_avg_org_impr)
			) / NULLIF(SUM(curr_avg_org_impr), 0) :: float
		) :: numeric(15, 2),
		0
	) as curr_availability,
	COALESCE(
		(
			(
				SUM(prev_avg_availability_score * prev_avg_org_impr)
			) / NULLIF(SUM(prev_avg_org_impr), 0) :: float
		) :: numeric(15, 2),
		0
	) as prev_availability,
	COALESCE(
		(
			(
				SUM(curr_avg_buyability_score * curr_avg_org_impr)
			) / NULLIF(SUM(curr_avg_org_impr), 0) :: float
		) :: numeric(15, 2),
		0
	) as curr_buyability,
	COALESCE(
		(
			(
				SUM(prev_avg_buyability_score * prev_avg_org_impr)
			) / NULLIF(SUM(prev_avg_org_impr), 0) :: float
		) :: numeric(15, 2),
		0
	) as prev_buyability,
	COALESCE(
		(
			(SUM(curr_avg_pricing_score * curr_avg_org_impr)) / NULLIF(SUM(curr_avg_org_impr), 0) :: float
		) :: numeric(15, 2),
		0
	) as curr_pricing,
	COALESCE(
		(
			(SUM(prev_avg_pricing_score * prev_avg_org_impr)) / NULLIF(SUM(prev_avg_org_impr), 0) :: float
		) :: numeric(15, 2),
		0
	) as prev_pricing
FROM
	cte_dq_data_
WHERE
	sub_category IN (
		SELECT
			DISTINCT sub_category
		FROM
			cat_vol_chan
	)
GROUP BY
	1
ORDER BY
	1;

-------Top 5 product last component---
----Top 5 BSR - Product /Sub_Catgory Wise----
/*		with top_5 as (	
 select 	sku_title,brand,sku,sub_category
 ,coalesce (sum(unit_per_day)filter(where crawl_date >= '2023-05-01'),0) curr_upd,
 dense_rank() over (partition by sub_category order by coalesce (sum(unit_per_day)filter(where crawl_date >= '2023-05-01'),0) desc) curr_bsr
 from cte_dq_data where bsr_2 is not null group by 1,2,3,4
 order by curr_upd desc
 
 )select * from top_5 where brand = 'zingavita' limit 5-- limit 15
 ; --select * from top_5_prod*/
create temp table top_5_prod as with top_5 as (
	select
		sku_title,
		brand,
		sku,
		COALESCE(
			sum(unit_per_day) filter(
				where
					crawl_date >= enddate_arg
			),
			0
		) curr_upd,
		row_number() over (
			order by
				COALESCE(
					sum(unit_per_day) filter(
						where
							crawl_date >= enddate_arg
					),
					0
				) desc
		) curr_bsr,
		row_number() over (
			order by
				COALESCE(
					sum(unit_per_day) filter(
						where
							crawl_date BETWEEN pre_end_date
							AND enddate_arg - 1
					),
					0
				) desc
		) prev_bsr
	from
		cte_dq_data
	where
		bsr_2 is not null
	group by
		1,
		2,
		3
	order by
		curr_upd desc
)
select
	sku_title,
	brand,
	sku,
	curr_bsr,
	prev_bsr,
	concat(p5.url_format, o.sku, '/') as sku_url
from
	top_5 as o
	left join e4_entity_imported_foreign_tables.channel_country_domain_mapping p5 on (
		p5.channel_id = channel_id_arg
		and lower(p5.country_code) = (
			select
				lower(country)
			from
				e4_entity_imported_foreign_tables.client_master
			where
				id = client_id_arg
		)
	)
where
	brand = brand_arg
limit
	5;

create temp table top_product as with comp_prod as (
	select
		t5.sku,
		prod_target,
		type
	from
		top_5_prod t5
		inner join comp_sku_map cd on (t5.sku = cd.asin)
),
tgt_prod as (
	select
		cp.sku,
		sa.sku as comp_sku,
		type,
		COALESCE(
			sum(organic_impressions) filter(
				where
					crawl_date >= enddate_arg
			),
			0
		) as curr_org_impr,
		COALESCE(
			sum(organic_impressions) filter(
				where
					crawl_date BETWEEN pre_end_date
					AND enddate_arg - 1
			),
			0
		) as prev_org_impr,
		COALESCE(
			sum(paid_impressions) filter(
				where
					crawl_date >= enddate_arg
			),
			0
		) as curr_paid_impr,
		COALESCE(
			sum(paid_impressions) filter(
				where
					crawl_date BETWEEN pre_end_date
					AND enddate_arg - 1
			),
			0
		) as prev_paid_impr
	from
		cte_dq_data sa
		inner join comp_prod cp on (sa.sku = cp.prod_target)
	group by
		1,
		2,
		3
),
max_imp as (
	select
		sku,
		max(curr_org_impr) max_org_impr,
		max(prev_org_impr) max_prev_org_impr,
		max(curr_org_impr) max_curr_paid_impr,
		max(prev_org_impr) max_prev_paid_impr
	from
		tgt_prod
	group by
		1
) --select * from max_imp
,
org_imp as (
	select
		sku,
		curr_org_impr,
		prev_org_impr,
		curr_paid_impr,
		prev_paid_impr
	from
		tgt_prod
	where
		type = 'S'
),
visibility as (
	select
		org.sku,
		COALESCE(
			(
				(org.curr_org_impr) / NULLIF(mp.max_org_impr, 0) :: float * 100
			) :: numeric(15, 2),
			0
		) as curr_org_impr,
		COALESCE(
			(
				(org.prev_org_impr) / NULLIF(mp.max_prev_org_impr, 0) :: float * 100
			) :: numeric(15, 2),
			0
		) as prev_org_impr,
		COALESCE(
			(
				(org.curr_paid_impr) / NULLIF(mp.max_curr_paid_impr, 0) :: float * 100
			) :: numeric(15, 2),
			0
		) as curr_paid_impr,
		COALESCE(
			(
				(org.prev_paid_impr) / NULLIF(mp.max_prev_paid_impr, 0) :: float * 100
			) :: numeric(15, 2),
			0
		) as prev_paid_impr
	FROM
		org_imp org
		INNER JOIN max_imp mp ON (org.sku = mp.sku)
),
seller_types as (
	select
		sku,
		crawl_date,
(
			case
				when TRIM(LOWER(sa.buybox)) in (
					select
						TRIM(
							LOWER(
								row_to_json(json_each_text(seller_details)) ->> 'key'
							)
						)
					from
						e4_entity_imported_foreign_tables.preferred_sellers
					where
						client_id = client_id_arg
				) then 'a'
				else 'u'
			end
		) as bb_type
	from
		cte_dq_data sa
	where
		buybox is not null
),
buybox as (
	select
		t5.sku,
		COALESCE(
			count(bb_type) filter(
				where
					crawl_date >= enddate_arg
					and bb_type = 'a'
			),
			0
		) as bb_curr_authorized,
		COALESCE(
			count(bb_type) filter(
				where
					crawl_date BETWEEN pre_end_date
					AND enddate_arg - 1
					and bb_type = 'a'
			),
			0
		) as bb_prev_authorized,
		COALESCE(
			count(bb_type) filter(
				where
					crawl_date >= enddate_arg
			),
			0
		) as bb_curr_tot,
		COALESCE(
			count(bb_type) filter(
				where
					crawl_date BETWEEN pre_end_date
					AND enddate_arg - 1
			),
			0
		) as bb_prev_tot
	from
		top_5_prod t5
		left join seller_types st on (t5.sku = st.sku)
	group by
		1
) ----top_5_prod (bsr,rating,price/diff)
select
	t5.sku_title,
	t5.sku,
	curr_bsr,
	prev_bsr,
	t5.sku_url,
	coalesce(avg(rating), 0) :: numeric(15, 1) rating,
	coalesce(
		avg(price) filter(
			where
				crawl_date >= enddate_arg
		),
		0
	) :: numeric(15, 2) as curr_lates_price,
	NULLIF(
		avg(price) filter(
			where
				crawl_date BETWEEN pre_end_date
				AND enddate_arg - 1
		),
		0
	) :: numeric(15, 2) as prev_lates_price,
	COALESCE(
		(
			(
				count(*) filter(
					where
						crawl_date >= enddate_arg
						and availability in ('unavailable')
						and availability is not null
				)
			) / NULLIF(
				count(*) filter(
					where
						crawl_date >= enddate_arg
						and availability is not null
				),
				0
			) :: float * 100
		) :: numeric(15, 2),
		0
	) as curr_oos,
	COALESCE(
		(
			(
				count(*) filter(
					where
						crawl_date BETWEEN pre_end_date
						AND enddate_arg - 1
						and availability in ('unavailable')
						and availability is not null
				)
			) / NULLIF(
				count(*) filter(
					where
						crawl_date BETWEEN pre_end_date
						AND enddate_arg - 1
						and availability is not null
				),
				0
			) :: float * 100
		) :: numeric(15, 2),
		0
	) as prev_oos,
	max(rating_count) filter(
		where
			crawl_date = startdate_arg
	) as latest_review,
	max(rating_count) filter(
		where
			crawl_date = enddate_arg -1
	) as prev_review,
	curr_org_impr,
	prev_org_impr,
	curr_paid_impr,
	prev_paid_impr,
	bb_curr_authorized,
	bb_prev_authorized,
	bb_curr_tot,
	bb_prev_tot
from
	top_5_prod t5
	left join cte_dq_data sa on (t5.sku = sa.sku)
	left join visibility vs on (t5.sku = vs.sku)
	left join buybox bb on (t5.sku = bb.sku)
group by
	1,
	2,
	3,
	4,
	5,
	curr_org_impr,
	prev_org_impr,
	curr_paid_impr,
	prev_paid_impr,
	bb_curr_authorized,
	bb_prev_authorized,
	bb_curr_tot,
	bb_prev_tot;

return query
select
	json_build_object (
		'client_id',
		client_id_arg,
		'report_generated_at',
		now(),
		'data_start_date',
		startdate_arg,
		'data_end_date',
		enddate_arg,
		'section_1',
(
			json_build_object (
				'sec1_ms_discreet_values',
(
					select
						array_agg((row_to_json(a)))
					from
(
							select
								curr_ms_value,
								prev_ms_value,
								curr_ms_vol,
								prev_ms_vol
							from
								cte_get_ms_sov
						) a
				),
				'sec1_sov_discreet_values',
(
					select
						array_agg((row_to_json(a)))
					from
(
							select
								curr_paid_sov,
								curr_org_sov,
								prev_paid_sov,
								prev_org_sov
							from
								cte_get_ms_sov
						) a
				),
				'sec1_ms_top_brand_ranks',
(
					select
						array_agg((row_to_json(a)))
					from
(
							select
								*
							from
								ms_top_brand_ranks
						) a
				),
				'sec1_ms_date_wise_brands',
(
					select
						array_agg((row_to_json(a)))
					from
(
							select
								*
							from
								ms_date_percent
						) a
				),
				'sec1_sov_top_brand_ranks',
(
					select
						array_agg((row_to_json(a)))
					from
(
							select
								*
							from
								sov_top_brand_ranks
						) a
				),
				'sec1_sov_date_wise_brands',
(
					select
						array_agg((row_to_json(a)))
					from
(
							select
								*
							from
								sov_date_percent
						) a
				),
				'category_waterfall_data',
(
					select
						array_agg((row_to_json(a)))
					from
(
							select
								*
							from
								waterfall_1_data
						) a
				),
				'section3_cte_dq_data',
(
					select
						array_agg((row_to_json(a)))
					from
(
							select
								*
							from
								dq_brand_cal
						) a
				)
			)
		),
		'section_2',
(
			json_build_object (
				'subcat_waterfall_data',
(
					select
						array_agg((row_to_json(a)))
					from
(
							select
								*
							from
								waterfall_2_data
						) a
				),
				'section2_subcat_firstbox',
(
					select
						array_agg((row_to_json(a)))
					from
(
							select
								cvc.sub_category as name,
								curr_ms_share as curr_market_share_by_volume,
								prev_ms_share as prev_market_share_by_volume
							from
								cat_vol_chan as cvc
						) a
				),
				'section2_subcat_firstbox_datechart',
(
					select
						array_agg((row_to_json(a)))
					from
(
							select
								sub_category,
								crawl_date,
								day_upd
							from
								sum_upd_subcat
						) a
				),
				'sec2_brand_wise_rank',
(
					select
						array_agg((row_to_json(a)))
					from
(
							select
								sub_category,
								brand,
								sub_cat_brand_percent as brand_percent
							from
								cte_get_top5_brands
						) a
				),
				'dq_sub_cat',
(
					select
						array_agg((row_to_json(a)))
					from
(
							select
								*
							from
								dq_sub_cat
						) a
				)
			)
		),
		'section_3',
(
			json_build_object (
				'top_products',
(
					select
						array_agg((row_to_json(a)))
					from
(
							select
								*
							from
								top_product
						) a
				)
			)
		),
		'channel_details',
(
			select
				array_agg((row_to_json(a)))
			from
(
					select
						id,
						name
					from
						e4_entity_imported_foreign_tables.channel_master cm
					where
						id = (
							select
								channel_id_arg
						)
				) a
		)
	);

end;

$ BODY $;

ALTER FUNCTION public.sp_get_ba_t1(integer, date, date, text, text, integer) OWNER TO postgres;
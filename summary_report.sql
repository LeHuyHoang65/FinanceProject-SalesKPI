CREATE TABLE dim_metrics_report (
    funding_id SERIAL PRIMARY KEY,
    funding_code VARCHAR(255) NOT NULL,
    funding_name VARCHAR(255) NOT NULL,
    funding_parent_id INT,
    funding_level INT,
    sortorder int,
    rec_created_dt timestamp default now(),
    rec_updated_dt timestamp default now()
);

select * from dim_metrics_report dmr 

drop table if exists dim_city 
CREATE TABLE dim_city (
    pos_cde VARCHAR(1024) PRIMARY KEY,
    pos_city VARCHAR(1024),          
    area_cde varchar(32)
);

INSERT INTO dim_city (pos_cde, pos_city, area_cde)
SELECT distinct
	fkm.pos_cde,
    fkm.pos_city,
    case 
    	when fkm.pos_city in ('Hà Giang','Tuyên Quang','Phú Thọ','Thái Nguyên','Bắc Kạn','Cao Bằng','Lạng Sơn','Bắc Giang','Quảng Ninh') then 'B'
    	when fkm.pos_city in ('Lào Cai','Yên Bái','Điện Biên','Sơn La','Hòa Bình','Lai Châu') then 'C'
    	when fkm.pos_city in ('Hà Nội','Hải Phòng','Vĩnh Phúc','Bắc Ninh','Hưng Yên','Hải Dương','Thái Bình','Nam Định','Ninh Bình','Hà Nam') then 'D'
    	when fkm.pos_city in ('Thanh Hóa','Nghệ An','Hà Tĩnh','Quảng Bình','Quảng Trị','Huế') then 'E'
    	when fkm.pos_city in ('Đà Nẵng','Quảng Nam','Quảng Ngãi','Bình Định','Phú Yên','Khánh Hòa','Ninh Thuận','Bình Thuận','Kon Tum','Gia Lai','Đắk Lắk','Đắk Nông','Lâm Đồng') then 'F'
    	when fkm.pos_city in ('Cần Thơ','Long An','Đồng Tháp','Tiền Giang','An Giang','Bến Tre','Vĩnh Long','Trà Vinh','Hậu Giang','Kiên Giang','Sóc Trăng','Bạc Liêu','Cà Mau') then 'G'
    	when fkm.pos_city in ('Hồ Chí Minh','Bà Rịa - Vũng Tàu','Bình Dương','Bình Phước','Đồng Nai','Tây Ninh') then 'H'
    	else 'A'
    end as area_cde
FROM fact_kpi_month fkm
WHERE fkm.pos_city IS NOT NULL
AND fkm.pos_cde IS NOT null
ON CONFLICT (pos_cde) DO NOTHING;

select * from dim_city 
select count(*) from dim_city  
select count(distinct pos_cde) from fact_kpi_month fkm 

drop table if exists dim_area  
CREATE TABLE dim_area (
    area_cde VARCHAR(1) PRIMARY KEY,   
    area_name VARCHAR(50)             
);

INSERT INTO dim_area (area_cde, area_name)
SELECT area_cde, area_name
FROM (
    SELECT DISTINCT
        CASE area_name
            WHEN 'Hội Sở' THEN 'A'
            WHEN 'Đông Bắc Bộ' THEN 'B'
            WHEN 'Tây Bắc Bộ' THEN 'C'
            WHEN 'Đồng Bằng Sông Hồng' THEN 'D'
            WHEN 'Bắc Trung Bộ' THEN 'E'
            WHEN 'Nam Trung Bộ' THEN 'F'
            WHEN 'Tây Nam Bộ' THEN 'G'
            WHEN 'Đông Nam Bộ' THEN 'H'
            ELSE 'A'
        END AS area_cde,
        area_name
    FROM kpi_asm_data
    WHERE area_name IS NOT NULL
    UNION
    SELECT 'A', 'Hội Sở'
    ORDER BY area_cde
) AS sorted_areas
ON CONFLICT (area_cde) DO NOTHING;

select * from dim_area da 

drop table if exists fact_txn_month 
CREATE TABLE public.fact_txn_month (
        transaction_date date NULL,
        account_code int8 NOT NULL,
        account_description varchar(1024) NULL,
        analysis_code varchar(100) NULL,
        amount int8 NULL,
        d_c varchar(1) NULL,
        funding_id int4 NULL DEFAULT '-1'::integer
);


CREATE TABLE public.fact_kpi_month (
        kpi_month int8 NOT NULL,
        pos_cde VARCHAR(50),
	    pos_city VARCHAR(50),
	    application_id int8 not null,
	    outstanding_principal int8 null,
	    write_off_month int8 null,
	    write_off_balance_principal int8 null,
	    psdn int4 null,
	    max_bucket int4 null,
	    funding_id int4 NULL DEFAULT '-1'::integer
);

select * from fact_txn_month ftm 
select count(*) from fact_txn_month ftm2 

select * from fact_kpi_month fkm 
select count(*) from fact_kpi_month fkm2 


-- Tinh Lai Trong Han T2
with head as (
--tinh tong lai HEAD
select 
	sum(amount) as total_head
from fact_txn_month ftm 
where account_code in (702000030002, 702000030001,702000030102)
	and extract(year from transaction_date) = 2023 
	and extract(month from transaction_date) <= 2 
	and analysis_code like 'HEAD%' )

--rule1: tinh lai truc tiep tu khu vuc
, rule1_step1 as (
select 
	* ,
	substring(analysis_code,9,1) as ma_vung
from fact_txn_month ftm  
where account_code in (702000030002, 702000030001,702000030102)
	and extract(year from transaction_date) = 2023 
	and extract(month from transaction_date) in (1,2) 
	and analysis_code like 'DVML%' ) 
,rule1_step2 as (	
select 
	ma_vung ,
	sum(amount) as total_rule1 
from rule1_step1 
group by 1 ) 

--rule2: tinh ty trong de phan bo lai HEAD
, rule2_step1 as (
select 
	* 
from fact_kpi_month f
inner join dim_city dc  
on f.pos_cde = dc.pos_cde 
	where kpi_month in (202301,202302)
	and coalesce(max_bucket,1) = 1 )
, rule2_step2 as (
select 
	kpi_month ,
	area_cde ,
	sum(outstanding_principal) as oppermonth
from rule2_step1 
group by 1,2 
order by 2,1 )

,rule2_step3 as (
select 
	area_cde , 
	avg(oppermonth) as ave_khuvuc
from rule2_step2 
group by 1 )
, rule2_step4 as (
select 
	area_cde , 
	(ave_khuvuc * (select total_head from head) / (select sum(ave_khuvuc) from rule2_step3 )) as phanbo
from rule2_step3 ) 

--kq = rule1 + rule2
select 
	distinct 
	da.area_name,
	round((phanbo + total_rule1),2) as Lai_Trong_Han
from rule2_step4 as r2
inner join rule1_step2 as r1
on r2.area_cde = r1.ma_vung
inner join dim_area da 
on r1.ma_vung = da.area_cde  
order by 2 desc 




-- Tạo bảng tạm bằng function 
create or replace
function create_temp_table(year_pram int, month_pram int)
returns void 
as 
$$
begin
	--tạo bảng tạm lãi trong hạn
	create temp table lai_trong_han as
	--tinh lai trong han
	with head as(
	select 
		sum(amount) as total_head
	from fact_txn_month ftm  
	where account_code in (702000030002, 702000030001,702000030102)
		and extract(year from transaction_date) = year_pram
		and extract(month from transaction_date) <= month_pram
		and analysis_code like 'HEAD%')
	
	,rule1_step1 as (
	select 
		* ,
		substring(analysis_code,9,1) as ma_vung
	from fact_txn_month ftm  
	where account_code in (702000030002, 702000030001,702000030102)
		and extract(year from transaction_date) = year_pram 
		and extract(month from transaction_date) <= month_pram
		and analysis_code like 'DVML%' 
		and substring(analysis_code,9,1) in ('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H'))
		
	,rule1_step2 as (	
	select 
		ma_vung ,
		sum(amount) as total_rule1 
	from rule1_step1 
	group by 1 ) 		
			
	,rule2_step1 as (
	select 
		* 
	from fact_kpi_month f
	inner join dim_city dc 
	on f.pos_cde  = dc.pos_cde  
	where kpi_month between year_pram*100+1 and year_pram*100+month_pram
		and coalesce(max_bucket,1) = 1 )
		
	,rule2_step2 as (
	select 
		kpi_month ,
		area_cde ,
		sum(outstanding_principal) as oppermonth
	from rule2_step1 
	group by 1,2 
	order by 2,1 )
	
	,rule2_step3 as (
	select 
		area_cde , 
		avg(oppermonth) as ave_khuvuc
	from rule2_step2 
	group by 1 )
	
	, rule2_step4 as (
	select 
		area_cde , 
		(ave_khuvuc * (select total_head from head) / (select sum(ave_khuvuc) from rule2_step3 )) as phanbo
	from rule2_step3 ) 
		
	select 
		distinct 
		r1.ma_vung,
		(phanbo + total_rule1) as Lai_Trong_Han
	from rule2_step4 as r2
	inner join rule1_step2 as r1
	on r2.area_cde = r1.ma_vung;

	-------------------------------------
	--Tính lãi quá hạn 
	create temp table lai_qua_han as 
	with head as (
	select 
		sum(amount) as total_head
	from fact_txn_month ftm
	where extract(year from transaction_date) = year_pram 
		and extract(month from transaction_date ) <= month_pram
		and analysis_code like 'HEAD%' 
		and account_code in (702000030012, 702000030112) ) 
	 
	,rule1_step1 as ( 
	select 
		* , substring(analysis_code,9,1) as ma_vung 
	from fact_txn_month ftm  
	where extract(year from transaction_date) = year_pram 
		and extract(month from transaction_date ) <= month_pram
		and analysis_code like 'DVML%' 
		and account_code in (702000030012, 702000030112)
		and substring(analysis_code,9,1) in ('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H'))
		
	, rule1_step2 as (
	select 
		ma_vung ,
		sum(amount) as total_rule1
	from rule1_step1 
	group by 1  ) 
	
	, rule2_step1 as (
	select 
		kpi_month , 
		dc.area_cde ,
		sum(outstanding_principal) as total_by_month
	from fact_kpi_month f
	inner join dim_city dc  
	on f.pos_cde  = dc.pos_cde  
	where kpi_month between year_pram*100+1 and year_pram*100+month_pram	 
		and max_bucket = 2
	group by 1,2 
	order by 2,1 ) 
	
	, rule2_step2 as (
	select 
		area_cde ,
		avg(total_by_month) as ave 
	from rule2_step1 
	group by 1 )
		
	, rule2_step3 as (
	select 
		area_cde ,
		ave / (select sum(ave) from rule2_step2 ) as ty_trong
	from rule2_step2 ) 
		
	select 
			distinct 
			r1.ma_vung  ,
			((ty_trong * (select total_head from head )) + total_rule1) as lai_qua_han
	from rule2_step3 r2
	inner join rule1_step2 r1 
	on r2.area_cde = r1.ma_vung ;	
	
	-----------------------------------
	--Tính phí bảo hiểm
	create temp table phi_bao_hiem as 
	with head as (
	select
		sum(amount) as total_head
	from fact_txn_month ftm 
	where extract(year from transaction_date) =year_pram 
		and extract(month from transaction_date) <=month_pram 
		and analysis_code like 'HEAD%'
		and account_code = '716000000001' )
	
		
	,rule1_step1 as (
	select 
		* , substring(analysis_code,9,1) as ma_vung 
	from fact_txn_month ftm 
	where extract(year from transaction_date) =year_pram 
		and extract(month from transaction_date) <=month_pram 
		and analysis_code like 'DVML%'
		and account_code = '716000000001' 
		and substring(analysis_code,9,1) in ('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H'))
		
	, rule1_step2 as (
	select 
		ma_vung ,
		sum(amount) as total_rule1 
	from rule1_step1 
	group by 1 ) 
	
	, rule2_step1 as (
	select 
		kpi_month  ,
		area_cde , 
		count(psdn) as so_luong_psdn
	from fact_kpi_month f
	inner join dim_city dc 
	on f.pos_cde  = dc.pos_cde  
	where kpi_month between year_pram*100+1 and year_pram*100+month_pram 
		 and psdn = 1
	group by 1,2 
	order by 2,1 )
	
	, rule2_step2 as (
	select 
		area_cde , 
		sum(so_luong_psdn) as total_psdn_khuvuc
	from rule2_step1 
	group by 1 )

	, rule2_step3 as (
	select 
		area_cde , 
		total_psdn_khuvuc / (select sum(total_psdn_khuvuc) from rule2_step2 ) as ty_trong
	from rule2_step2 )
	
	select 
		distinct 
		r1.ma_vung ,
		((ty_trong * (select total_head from head) ) + total_rule1 )  as phi_bao_hiem
	from rule2_step3 as r2 
	inner join rule1_step2 as r1 
	on r2.area_cde = r1.ma_vung ;

	----------------------------
	--Tính phí tăng hạn mức
	create temp table phi_tang_han_muc as 
	with head as (
	select 
		sum(amount) as total_head
	from fact_txn_month ftm
	where account_code = '719000030002'
		and extract(year from transaction_date) = year_pram
		and extract(month from transaction_date) <= month_pram
		and analysis_code like 'HEAD%' )
		
	,rule1_step1 as (
	select 
		* ,
		substring(analysis_code,9,1) as ma_vung
	from fact_txn_month ftm  
	where account_code = '719000030002'
		and extract(year from transaction_date) = year_pram 
		and extract(month from transaction_date) <= month_pram  
		and analysis_code like 'DVML%' 
		and substring(analysis_code,9,1) in ('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H'))
		
	,rule1_step2 as (	
	select 
		ma_vung ,
		sum(amount) as total_rule1 
	from rule1_step1 
	group by 1 ) 
	
	,rule2_step1 as (
	select 
		* 
	from fact_kpi_month f
	inner join dim_city dc  
	on f.pos_cde  = dc.pos_cde  
		and kpi_month between year_pram*100+1 and year_pram*100+month_pram
		and coalesce(max_bucket,1) = 1 )
		
	, rule2_step2 as (
	select 
		kpi_month ,
		area_cde ,
		sum(outstanding_principal) as oppermonth
	from rule2_step1 
	group by 1,2 
	order by 2,1 )
	
	,rule2_step3 as (
	select 
		area_cde , 
		avg(oppermonth) as ave_khuvuc
	from rule2_step2 
	group by 1 )

	, rule2_step4 as (
	select 
		area_cde , 
		(ave_khuvuc * (select total_head from head) / (select sum(ave_khuvuc) from rule2_step3 )) as phanbo
	from rule2_step3 )

	select 
		distinct 
		r1.ma_vung  ,
		(phanbo + total_rule1) as Phi_tang_han_muc
	from rule2_step4 as r2
	inner join rule1_step2 as r1
	on r2.area_cde = r1.ma_vung ;

	-----------------------------
	--Tính phí thanh toán chậm, thu từ ngoại bảng
	create temp table phi_thanh_toan_cham as 
	with head as (
	select 
		sum(amount) as total_head
	from fact_txn_month ftm 
	where account_code in ( 719000030003,719000030103,790000030003,790000030103,790000030004,790000030104)
		and extract(year from transaction_date) = year_pram 
		and extract(month from transaction_date) <= month_pram 
		and analysis_code like 'HEAD%' )

	,rule1_step1 as (
	select 
		* ,
		substring(analysis_code,9,1) as ma_vung
	from fact_txn_month ftm
	where account_code in (719000030003,719000030103,790000030003,790000030103,790000030004,790000030104)
		and extract(year from transaction_date) = year_pram 
		and extract(month from transaction_date) <= month_pram
		and analysis_code like 'DVML%' 
		and substring(analysis_code,9,1) in ('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H'))
		
	,rule1_step2 as (	
	select 
		ma_vung ,
		sum(amount) as total_rule1 
	from rule1_step1 
	group by 1 )
	
	,rule2_step1 as (
	select 
		* 
	from fact_kpi_month f
	inner join dim_city dc  
	on f.pos_cde  = dc.pos_cde  
	where kpi_month between year_pram*100+1 and year_pram*100+month_pram 
		and (max_bucket between 2 and 5))
		
	, rule2_step2 as (
	select 
		kpi_month ,
		area_cde ,
		sum(outstanding_principal) as oppermonth
	from rule2_step1 
	group by 1,2 
	order by 2,1 )
	
	,rule2_step3 as (
	select 
		area_cde , 
		avg(oppermonth) as ave_khuvuc
	from rule2_step2 
	group by 1 )
	
	, rule2_step4 as (
	select 
		area_cde , 
		(ave_khuvuc * (select total_head from head) / (select sum(ave_khuvuc) from rule2_step3 )) as phanbo
	from rule2_step3 )
	
	select 
		distinct 
		r1.ma_vung ,
		(phanbo + total_rule1) as Phi_thanh_toan_cham
	from rule2_step4 as r2
	inner join rule1_step2 as r1
	on r2.area_cde = r1.ma_vung ;

	-------------------------------------
	--Tính chi phí 
	-- DT Kinh Doanh 
	create temp table dt_kinhdoanh as 
	with head as (
	select 
		sum(amount) as total_head
	from fact_txn_month ftm
	where extract(year from transaction_date) = year_pram 
		and extract(month from transaction_date) <= month_pram 
		and analysis_code like 'HEAD%'
		and account_code in (702000010001,702000010002,704000000001,705000000001,709000000001,714000000002,
			714000000003,714037000001,714000000004,714014000001,715000000001,715037000001,719000000001,709000000101,719000000101) )

	, rule1_step1 as (	
	select 
		* , 
		substring(analysis_code,9,1) as ma_vung 
	from fact_txn_month ftm
	where extract(year from transaction_date) = year_pram 
		and extract(month from transaction_date) <= month_pram 
		and analysis_code like 'DVML%'
		and account_code in (702000010001,702000010002,704000000001,705000000001,709000000001,714000000002,714000000003,
			714037000001,714000000004,714014000001,715000000001,715037000001,719000000001,709000000101,719000000101) 
			and substring(analysis_code,9,1) in ('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H'))
			
	, rule1_step2 as (
	select 
		ma_vung , 
		sum(amount) as total_rule1 
	from rule1_step1 
	group by 1 )
	
	,rule2_step1 as (
	select 
		kpi_month , 
		dc.area_cde ,
		sum(outstanding_principal) as op_per_month
	from fact_kpi_month f
	inner join dim_city dc  
	on f.pos_cde  = dc.pos_cde  
	where kpi_month between year_pram*100+1 and year_pram*100+month_pram 
	group by 1,2 
	order by 2,1 )
			
	, rule2_step2 as (
	select 
		area_cde , 
		avg(op_per_month) as ave 
	from rule2_step1 
	group by 1 )	
	
	, rule2_step3 as (
	select 
		area_cde , 
		ave / (select sum(ave) from rule2_step2 ) as ty_trong
	from rule2_step2 )
	
	select 
		distinct 
		r1.ma_vung  , 
	((ty_trong * (select total_head from head)) + total_rule1 )  as DT_KinhDoanh
	from rule2_step3 as r2 
	inner join rule1_step2 as r1 
	on r2.area_cde = r1.ma_vung ;

	----------------------------------------------------
	-- Chi phí hoa hồng 
	create temp table cp_hoahong as 
	with head as (
	select 
		sum(amount) as total_head
	from fact_txn_month ftm
	where extract(year from transaction_date) = year_pram 
		and extract(month from transaction_date) <= month_pram 
		and analysis_code like 'HEAD%'
		and account_code in (816000000001,816000000002,816000000003) ) 

	, rule1_step1 as (	
	select 
		* , 
		substring(analysis_code,9,1) as ma_vung 
	from fact_txn_month ftm
	where extract(year from transaction_date) = year_pram 
		and extract(month from transaction_date)<= month_pram 
		and analysis_code like 'DVML%'
		and account_code in (816000000001,816000000002,816000000003) 
		and substring(analysis_code,9,1) in ('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H'))
		
	, rule1_step2 as (
	select 
		ma_vung , 
		sum(amount) as total_rule1 
	from rule1_step1 
	group by 1 ) 
	
	,rule2_step1 as (
	select 
		kpi_month , 
		dc.area_cde ,
		sum(outstanding_principal) as op_per_month
	from fact_kpi_month f
	inner join dim_city dc 
	on f.pos_cde  = dc.pos_cde  
	where kpi_month between year_pram*100+1 and year_pram*100+month_pram
	group by 1,2 
	order by 2,1 ) 
	
	, rule2_step2 as (
	select 
		area_cde , 
		avg(op_per_month) as ave 
	from rule2_step1 
	group by 1 ) 

	, rule2_step3 as (
	select 
		area_cde , 
		ave / (select sum(ave) from rule2_step2 ) as ty_trong
	from rule2_step2 ) 

	select 
		distinct 
		r1.ma_vung  , 
		((ty_trong * (select total_head from head)) + total_rule1 )  as CP_HoaHong
	from rule2_step3 as r2 
	inner join rule1_step2 as r1 
	on r2.area_cde = r1.ma_vung ;

	----------------------------------------
	--Chi phí thuần kinh doanh khác 
	create temp table cp_thuan_kd_khac as 
	with head as (
	select 
		sum(amount) as total_head
	from fact_txn_month ftm
	where extract(year from transaction_date) = year_pram 
		and extract(month from transaction_date)<= month_pram 
		and analysis_code like 'HEAD%'
		and account_code in (809000000002,809000000001,811000000001,811000000102,811000000002,811014000001,811037000001,
		811039000001,811041000001,815000000001,819000000002,819000000003,819000000001,790000000003,790000050101,
		790000000101,790037000001,849000000001,899000000003,899000000002,811000000101,819000060001) ) 

	, rule1_step1 as (	
	select 
		* , 
		substring(analysis_code,9,1) as ma_vung 
	from fact_txn_month ftm
	where extract(year from transaction_date) = year_pram 
		and extract(month from transaction_date) <= month_pram 
		and analysis_code like 'DVML%'
		and account_code in (809000000002,809000000001,811000000001,811000000102,811000000002,811014000001,811037000001,
		811039000001,811041000001,815000000001,819000000002,819000000003,819000000001,790000000003,790000050101,
		790000000101,790037000001,849000000001,899000000003,899000000002,811000000101,819000060001) 
		and substring(analysis_code,9,1) in ('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H'))

	, rule1_step2 as (
	select 
		ma_vung , 
		sum(amount) as total_rule1 
	from rule1_step1 
	group by 1 ) 
	
	,rule2_step1 as (
	select 
		kpi_month , 
		dc.area_cde ,
		sum(outstanding_principal) as op_per_month
	from fact_kpi_month f
	inner join dim_city dc  
	on f.pos_cde  = dc.pos_cde  
	where kpi_month between year_pram*100+1 and year_pram*100+month_pram 
	group by 1,2 
	order by 2,1 ) 
	
	, rule2_step2 as (
	select 
		area_cde , 
		avg(op_per_month) as ave 
	from rule2_step1 
	group by 1 ) 

	, rule2_step3 as (
	select 
		area_cde , 
		ave / (select sum(ave) from rule2_step2 ) as ty_trong
	from rule2_step2 ) 
	
	select 
		distinct 
		r1.ma_vung  , 
		((ty_trong * (select total_head from head)) + total_rule1 ) as CP_ThuanKinhDoanhKhac
	from rule2_step3 as r2 
	inner join rule1_step2 as r1 
	on r2.area_cde = r1.ma_vung ;
	
	----------------------------------------
	--Chi phí hoạt động
	--Chi phí nhân viên
	create temp table cp_nhanvien as 
	with head as (
	select 
		sum(amount) as total_head
	from fact_txn_month ftm
	where extract(year from transaction_date) = year_pram 
		and extract(month from transaction_date) <= month_pram
		and analysis_code like 'HEAD%'
		and floor(account_code / 10000000000)::integer = 85 ) 

	, rule1_step1 as (
	select 
		*  ,
		substring(analysis_code,9,1) as ma_vung 
	from fact_txn_month ftm
	where extract(year from transaction_date) = year_pram 
		and extract(month from transaction_date) <= month_pram
		and analysis_code like 'DVML%'
		and floor(account_code / 10000000000)::integer = 85 
		and substring(analysis_code,9,1) in ('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H'))
		
	, rule1_step2 as (
	select 
		ma_vung , 
		sum(amount) as total_rule1 
	from rule1_step1 
	group by 1 )
	
	, rule2_step1 as (
	select
		da.area_cde ,
		kad.area_name , 
		count(sale_name) as so_luong_ASM
	from kpi_asm_data kad 
	inner join dim_area da  
	on kad.area_name = da.area_name 
	and (
		SELECT BOOL_AND(
                CASE 
                    WHEN m.month <= month_pram THEN 
                        kad.ltn_jan IS NOT NULL 
                        AND kad.approve_jan IS NOT NULL 
                        AND kad.app_in_jan IS NOT NULL 
                        AND kad.approval_rate_jan IS NOT NULL 
                    ELSE TRUE 
                END
            )
            FROM generate_series(1, month_pram) AS m(month)
        )
	group by 1,2
	order by da.area_cde asc )
	
	, rule2_step2 as (
	select 
		area_cde ,
		so_luong_ASM / (select sum(so_luong_ASM) from rule2_step1 ) as ty_trong
	from rule2_step1 )
	
	select 
		r1.ma_vung , 
		(ty_trong *(select total_head from head)) + total_rule1 as CP_nhanvien
	from rule2_step2 as r2 
	inner join rule1_step2 as r1 
	on r2.area_cde = r1.ma_vung ;

	-------------------------------------
	-- Chi phí quản lý
	create temp table cp_quanly as 
	with head as (
	select 
		sum(amount) as total_head
	from fact_txn_month ftm
	where extract(year from transaction_date) = year_pram 
		and extract(month from transaction_date) <= month_pram
		and analysis_code like 'HEAD%'
		and floor(account_code / 10000000000)::integer = 86 ) 
		
	, rule1_step1 as (
	select 
		*  ,
		substring(analysis_code,9,1) as ma_vung 
	from fact_txn_month ftm
	where extract(year from transaction_date) = year_pram 
		and extract(month from transaction_date) <= month_pram
		and analysis_code like 'DVML%'
		and floor(account_code / 10000000000)::integer = 86 
		and substring(analysis_code,9,1) in ('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H')) 
		
	, rule1_step2 as (
	select 
		ma_vung , 
		sum(amount) as total_rule1 
	from rule1_step1 
	group by 1 ) 
	
	, rule2_step1 as (
	select
		da.area_cde ,
		kad.area_name , 
		count(sale_name) as so_luong_ASM
	from kpi_asm_data kad  
	inner join dim_area da 
	on kad.area_name = da.area_name 
	and (
		SELECT BOOL_AND(
                CASE 
                    WHEN m.month <= month_pram THEN 
                        kad.ltn_jan IS NOT NULL 
                        AND kad.approve_jan IS NOT NULL 
                        AND kad.app_in_jan IS NOT NULL 
                        AND kad.approval_rate_jan IS NOT NULL 
                    ELSE TRUE 
                END
            )
            FROM generate_series(1, month_pram) AS m(month)
        )
	group by 1,2
	order by da.area_cde asc ) 
	
	, rule2_step2 as (
	select 
		area_cde ,
		so_luong_ASM / (select sum(so_luong_ASM) from rule2_step1 ) as ty_trong
	from rule2_step1 )
	
	select 
		r1.ma_vung , 
		(ty_trong *(select total_head from head)) + total_rule1 as CP_QuanLy
	from rule2_step2 as r2 
	inner join rule1_step2 as r1 
	on r2.area_cde = r1.ma_vung ;

	----------------------
	-- Chi phí tài sản
	create temp table cp_taisan as 
	with head as (
	select 
		sum(amount) as total_head
	from fact_txn_month ftm
	where extract(year from transaction_date) = year_pram 
		and extract(month from transaction_date) <= month_pram
		and analysis_code like 'HEAD%'
		and floor(account_code / 10000000000)::integer = 87 ) 
		
	, rule1_step1 as (
	select 
		*  ,
		substring(analysis_code,9,1) as ma_vung 
	from fact_txn_month ftm
	where extract(year from transaction_date) = year_pram 
		and extract(month from transaction_date) <= month_pram
		and analysis_code like 'DVML%'
		and floor(account_code / 10000000000)::integer = 87 
		and substring(analysis_code,9,1) in ('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H')) 
		
	, rule1_step2 as (
	select 
		ma_vung , 
		sum(amount) as total_rule1 
	from rule1_step1 
	group by 1 ) 
	
	, rule2_step1 as (
	select
		da.area_cde ,
		kad.area_name , 
		count(sale_name) as so_luong_ASM
	from kpi_asm_data kad  
	inner join dim_area da 
	on kad.area_name = da.area_name 
	and (
		SELECT BOOL_AND(
                CASE 
                    WHEN m.month <= month_pram THEN 
                        kad.ltn_jan IS NOT NULL 
                        AND kad.approve_jan IS NOT NULL 
                        AND kad.app_in_jan IS NOT NULL 
                        AND kad.approval_rate_jan IS NOT NULL 
                    ELSE TRUE 
                END
            )
            FROM generate_series(1, month_pram) AS m(month)
        )
	group by 1,2
	order by da.area_cde asc ) 
	
	, rule2_step2 as (
	select 
		area_cde ,
		so_luong_ASM / (select sum(so_luong_ASM) from rule2_step1 ) as ty_trong
	from rule2_step1 )
	
	select 
		r1.ma_vung , 
		(ty_trong *(select total_head from head)) + total_rule1 as cp_taisan
	from rule2_step2 as r2 
	inner join rule1_step2 as r1 
	on r2.area_cde = r1.ma_vung ;

	-----------------------------------
	-- Chi phí dự phòng
	create temp table cp_duphong  as 
	with head as (
	select 
		sum(amount) as total_head
	from fact_txn_month ftm
	where extract(year from transaction_date) = year_pram 
		and extract(month from transaction_date) <= month_pram
		and analysis_code like 'HEAD%'
		and account_code in (790000050001, 882200050001, 790000030001, 882200030001, 790000000001, 790000020101, 
			882200000001, 882200050101, 882200020101, 882200060001,790000050101 ,882200030101))
			
	, rule1_step1 as (
	select 
		*  ,
		substring(analysis_code,9,1) as ma_vung 
	from fact_txn_month ftm
	where extract(year from transaction_date) = year_pram
		and extract(month from transaction_date) <= month_pram
		and analysis_code like 'DVML%'
		and account_code in (790000050001, 882200050001, 790000030001, 882200030001, 790000000001, 790000020101,
			882200000001, 882200050101, 882200020101, 882200060001,790000050101 ,882200030101)
		and substring(analysis_code,9,1) in ('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H'))
	, rule1_step2 as (
	select 
		ma_vung , 
		sum(amount) as total_rule1 
	from rule1_step1 
	group by 1 ) 
	
	, rule2_step1 as (
	select
		da.area_cde ,
		kad.area_name , 
		count(sale_name) as so_luong_ASM
	from kpi_asm_data kad  
	inner join dim_area da 
	on kad.area_name = da.area_name 
	and (
		SELECT BOOL_AND(
                CASE 
                    WHEN m.month <= month_pram THEN 
                        kad.ltn_jan IS NOT NULL 
                        AND kad.approve_jan IS NOT NULL 
                        AND kad.app_in_jan IS NOT NULL 
                        AND kad.approval_rate_jan IS NOT NULL 
                    ELSE TRUE 
                END
            )
            FROM generate_series(1, month_pram) AS m(month)
        )
	group by 1,2
	order by da.area_cde asc ) 
	
	, rule2_step2 as (
	select 
		area_cde ,
		so_luong_ASM / (select sum(so_luong_ASM) from rule2_step1 ) as ty_trong
	from rule2_step1 )
	
	select 
		r1.ma_vung , 
		(ty_trong *(select total_head from head)) + total_rule1 as CP_DuPhong
	from rule2_step2 as r2 
	inner join rule1_step2 as r1 
	on r2.area_cde = r1.ma_vung ;
end ;
$$ language plpgsql ;

-- tạo report được tổng hợp từ bảng tạm bằng procedure 
create or replace procedure generate_report(year_pram int, month_pram int ) 
language plpgsql 
as $$ 
begin 
	perform create_temp_table(year_pram, month_pram) ; 
	
	drop table if exists report ; 
	create table report(
		tenkhuvuc varchar , 
		lai_trong_han int8 , 
		lai_qua_han int8 ,
		phi_bao_hiem int8 , 
		phi_tang_han_muc int8 ,
		phi_thanh_toan_cham int8 , 
		doanhthu_kinhdoanh int8 ,
		cp_hoahong int8 , 
		cp_thuankdkhac int8 ,
		cp_nhanvien int8 ,
		cp_quanly int8 , 
		cp_taisan int8 ,
		cp_duphong int8	
	) ;

	insert into report
	select 
		da.area_name  , 
		lai_trong_han ,
		lai_qua_han ,
		phi_bao_hiem , 
		phi_tang_han_muc ,
		phi_thanh_toan_cham ,
		dt_kinhdoanh ,
		cp_hoahong ,
		CP_ThuanKinhDoanhKhac , 
		cp_nhanvien , 
		cp_quanly ,
		cp_taisan , 
		cp_duphong 
	from lai_trong_han lt
	inner join lai_qua_han lq
	on lt.ma_vung  = lq.ma_vung 
	inner join phi_bao_hiem pb 
	on lt.ma_vung = pb.ma_vung 
	inner join phi_tang_han_muc pt 
	on lt.ma_vung = pt.ma_vung 
	inner join phi_thanh_toan_cham pc 
	on lt.ma_vung = pc.ma_vung 
	inner join dt_kinhdoanh dk 
	on lt.ma_vung = dk.ma_vung 
	inner join cp_hoahong ch
	on lt.ma_vung = ch.ma_vung 
	inner join cp_thuan_kd_khac ct 
	on lt.ma_vung = ct.ma_vung 
	inner join cp_nhanvien cn 
	on lt.ma_vung = cn.ma_vung 
	inner join cp_quanly 
	on lt.ma_vung = cp_quanly.ma_vung 
	inner join cp_taisan
	on lt.ma_vung = cp_taisan.ma_vung 
	inner join cp_duphong 
	on lt.ma_vung = cp_duphong.ma_vung 
	inner join dim_area da 
	on lt.ma_vung = da.area_cde;

	drop table if exists lai_trong_han ;
	drop table if exists lai_qua_han ;
	drop table if exists phi_bao_hiem ; 
	drop table if exists phi_tang_han_muc ; 
	drop table if exists phi_thanh_toan_cham ; 
	drop table if exists dt_kinhdoanh; 
	drop table if exists cp_thuan_kd_khac; 
	drop table if exists cp_nhanvien ; 
	drop table if exists cp_quanly ; 
	drop table if exists cp_taisan ; 
	drop table if exists cp_duphong ; 
	drop table if exists cp_hoahong ;
end ;
$$ ;

-- Khi gọi procedure ở đây cần truyền tham số năm và tháng cho chúng : muốn có dữ liệu thời điểm nào thi điền thời điểm đó 
-- Do 2 chỉ số CP vốn CCTG và CP vốn TT2 phụ thuộc vào 1 vài chỉ số đã được tạo ra ở report trên , do đó để 
-- tính toán 2 chỉ số này , chúng ta cần tiếp tục tạo ra 1 procedure và sử dụng bảng tạm trong fucntion  để có thể trích xuất dữ liệu từ report trên 
-- -> tạo ra 2 chỉ số -> ra được report final 

-- Tạo bảng vật lý summary_report
DROP TABLE IF EXISTS summary_report;
CREATE TABLE summary_report (
    year INT,
    month INT,
    tenkhuvuc VARCHAR,
    lai_trong_han INT8,
    lai_qua_han INT8,
    phi_bao_hiem INT8,
    phi_tang_han_muc INT8,
    phi_thanh_toan_cham INT8,
    doanhthu_kinhdoanh INT8,
    cp_hoahong INT8,
    cp_thuankdkhac INT8,
    cp_nhanvien INT8,
    cp_quanly INT8,
    cp_taisan INT8,
    cp_duphong INT8,
    cp_von_CCTG NUMERIC,
    cp_von_tt2 NUMERIC,
    PRIMARY KEY (year, month, tenkhuvuc)
);

 -- tạo fucntion để dựng 2 bảng tạm cp_cctg và cp_tt2 từ report đã tạo ra ở trên 
create or replace function cp(year_pram int, month_pram int) 
returns void 
as 
$$ 
begin 
	DROP TABLE IF EXISTS cp_cctg;
    DROP TABLE IF EXISTS cp_tt2;
	
	-- Tạo bảng tạm tính cp_cctg 
	create temp table cp_cctg as 
	with head as (
	select 
		SUM(amount)::NUMERIC AS total_head 
	from fact_txn_month ftm
	where extract(year from transaction_date) = year_pram 
		and extract(month from transaction_date) <= month_pram
		and analysis_code like 'HEAD%'
		and account_code  = '803000000001') 
		
	, step1 as (
	select 
		tenkhuvuc , 
		(lai_trong_han + lai_qua_han + phi_bao_hiem + phi_tang_han_muc + phi_thanh_toan_cham)::NUMERIC AS thu_nhap_tu_hd_the, 
		SUM(doanhthu_kinhdoanh) OVER()::NUMERIC AS total_dt_kinhdoanh
	from report )
	
	, step2 as (
	select 
		tenkhuvuc , 
		thu_nhap_tu_hd_the  , 
		SUM(thu_nhap_tu_hd_the) OVER()::NUMERIC AS total_thunhap_hdthe, 
		total_dt_kinhdoanh 
	from step1 ) 
	
	select 
		tenkhuvuc , 
		CASE 
			WHEN total_thunhap_hdthe + total_dt_kinhdoanh = 0 THEN 0::NUMERIC
			ELSE ((SELECT total_head FROM head) * thu_nhap_tu_hd_the) / (total_thunhap_hdthe + total_dt_kinhdoanh)
		END AS cp_von_CCTG
	from step2 ;

	--------------------------------
	-- Bảng tạm tính cp_tt2 
	create temp table cp_tt2 as 
	with head as (
	select 
		SUM(amount)::NUMERIC AS total_head 
	from fact_txn_month ftm
	where extract(year from transaction_date) = year_pram 
		and extract(month from transaction_date) <= month_pram
		and analysis_code like 'HEAD%'
		and account_code  in (801000000001,802000000001))
	, step1 as (
	select 
		tenkhuvuc , 
		(lai_trong_han + lai_qua_han + phi_bao_hiem + phi_tang_han_muc + phi_thanh_toan_cham)::NUMERIC AS thu_nhap_tu_hd_the, 
		SUM(doanhthu_kinhdoanh) OVER()::NUMERIC AS total_dt_kinhdoanh
	from report ) 
	, step2 as (
	select 
		tenkhuvuc , 
		thu_nhap_tu_hd_the  , 
		SUM(thu_nhap_tu_hd_the) OVER()::NUMERIC AS total_thunhap_hdthe, 
		total_dt_kinhdoanh 
	from step1 ) 
	select 
		tenkhuvuc , 
		CASE 
			WHEN total_thunhap_hdthe + total_dt_kinhdoanh = 0 THEN 0::NUMERIC
			ELSE ((SELECT total_head FROM head) * thu_nhap_tu_hd_the) / (total_thunhap_hdthe + total_dt_kinhdoanh)
		END AS cp_von_tt2
	from step2 ;

	-- Insert vào bảng vật lý summary_report
    INSERT INTO summary_report (
        year, month, tenkhuvuc, lai_trong_han, lai_qua_han, phi_bao_hiem,
        phi_tang_han_muc, phi_thanh_toan_cham, doanhthu_kinhdoanh, cp_hoahong,
        cp_thuankdkhac, cp_nhanvien, cp_quanly, cp_taisan, cp_duphong,
        cp_von_CCTG, cp_von_tt2
    )
    SELECT 
        year_pram as year,
        month_pram as month,
        r.tenkhuvuc,
        r.lai_trong_han,
        r.lai_qua_han,
        r.phi_bao_hiem,
        r.phi_tang_han_muc,
        r.phi_thanh_toan_cham,
        r.doanhthu_kinhdoanh,
        r.cp_hoahong,
        r.cp_thuankdkhac,
        r.cp_nhanvien,
        r.cp_quanly,
        r.cp_taisan,
        r.cp_duphong,
        c.cp_von_CCTG,
        t.cp_von_tt2
    FROM report r 
    INNER JOIN cp_cctg c ON r.tenkhuvuc = c.tenkhuvuc 
    INNER JOIN cp_tt2 t ON r.tenkhuvuc = t.tenkhuvuc
    ON CONFLICT (year, month, tenkhuvuc) DO NOTHING;
	
end ; 
$$ language plpgsql ; 

CREATE OR REPLACE PROCEDURE gen_final_report(year_pram INT, month_pram INT) 
LANGUAGE plpgsql 
AS $$ 
BEGIN 
    -- Gọi function cp với year_pram và month_pram
    PERFORM cp(year_pram, month_pram); 

    -- Tạo bảng final_report với tên cột là Tiêu chí, Đông Bắc Bộ, ..., Total
    DROP TABLE IF EXISTS final_report; 
    CREATE TABLE final_report (
    	year INT,
        month INT,
        "Tiêu chí" TEXT, 
        "Đông Bắc Bộ" TEXT, 
        "Tây Bắc Bộ" TEXT, 
        "Đồng Bằng Sông Hồng" TEXT, 
        "Bắc Trung Bộ" TEXT, 
        "Nam Trung Bộ" TEXT, 
        "Tây Nam Bộ" TEXT, 
        "Đông Nam Bộ" TEXT, 
        "Total" TEXT
    );

    -- Tính toán các chỉ số giống như trước, loại bỏ Hội Sở
    WITH cte1 AS (
        SELECT 
        	s1.year,
            s1.month,
            s1.tenkhuvuc, 
            lai_trong_han + lai_qua_han + phi_bao_hiem + phi_tang_han_muc + phi_thanh_toan_cham AS thu_nhap_tu_hd_the, 
            lai_trong_han, 
            lai_qua_han,
            phi_bao_hiem,
            phi_tang_han_muc,
            phi_thanh_toan_cham,
            cp_von_tt2 + cp_von_cctg AS chi_phi_thuan_KDV, 
            cp_von_tt2,
            cp_von_cctg,
            doanhthu_kinhdoanh + cp_hoahong + cp_thuankdkhac AS chi_phi_thuan_hd_khac, 
            doanhthu_kinhdoanh, 
            cp_hoahong, 
            cp_thuankdkhac,
            cp_nhanvien + cp_quanly + cp_taisan AS tong_chi_phi_hoat_dong, 
            cp_nhanvien, 
            cp_quanly, 
            cp_taisan, 
            cp_duphong,
            s2.sl_nhansu 
        FROM summary_report s1 
        INNER JOIN (
            SELECT 
                p.area_name, 
                COUNT(p.sale_name) AS sl_nhansu
            FROM kpi_asm_data p 
            GROUP BY 1
        ) s2 
        ON s1.tenkhuvuc = s2.area_name 
        WHERE s1.tenkhuvuc != 'Hội Sở'  -- Loại bỏ Hội Sở
        	AND s1.year = year_pram
            AND s1.month = month_pram
    ),
    cte2 AS (
        SELECT 
            *,
            thu_nhap_tu_hd_the + chi_phi_thuan_KDV + chi_phi_thuan_hd_khac AS tong_thu_nhap_hd
        FROM cte1 
    ),
    cte3 AS (
        SELECT 
            *, 
            tong_thu_nhap_hd + tong_chi_phi_hoat_dong + cp_duphong AS loi_nhuan_truoc_thue,
            ROUND((tong_chi_phi_hoat_dong * -1.0 / NULLIF(tong_thu_nhap_hd, 0)) * 100, 2) AS CIR,
            ROUND(((tong_thu_nhap_hd + tong_chi_phi_hoat_dong + cp_duphong) / NULLIF((thu_nhap_tu_hd_the + doanhthu_kinhdoanh), 0)) * 100, 2) AS Margin,
            ROUND(((tong_thu_nhap_hd + tong_chi_phi_hoat_dong + cp_duphong) * -1.0 / NULLIF(chi_phi_thuan_KDV, 0)) * 100, 2) AS hieusuat_von,
            ROUND((tong_thu_nhap_hd + tong_chi_phi_hoat_dong + cp_duphong) / NULLIF(sl_nhansu, 0), 2) AS hieusuat_bq 
        FROM cte2 
    ),
    -- Melt dữ liệu: chuyển các cột chỉ số thành hàng
    melted AS (
        SELECT year, month,'A.Lợi nhuận trước thuế' AS "Tiêu chí", tenkhuvuc, loi_nhuan_truoc_thue::TEXT AS value FROM cte3
        UNION ALL SELECT year, month,'1.Thu nhập từ hoạt động thẻ', tenkhuvuc, thu_nhap_tu_hd_the::TEXT FROM cte3
        UNION ALL SELECT year, month,'Lãi trong hạn', tenkhuvuc, lai_trong_han::TEXT FROM cte3
        UNION ALL SELECT year, month,'Lãi quá hạn', tenkhuvuc, lai_qua_han::TEXT FROM cte3
        UNION ALL SELECT year, month,'Phí Bảo hiểm', tenkhuvuc, phi_bao_hiem::TEXT FROM cte3
        UNION ALL SELECT year, month,'Phí tăng hạn mức', tenkhuvuc, phi_tang_han_muc::TEXT FROM cte3
        UNION ALL SELECT year, month,'Phí thanh toán chậm, thu từ ngoại bảng, khác…', tenkhuvuc, phi_thanh_toan_cham::TEXT FROM cte3
        UNION ALL SELECT year, month,'Chi phí thuần KDV', tenkhuvuc, chi_phi_thuan_KDV::TEXT FROM cte3
        UNION ALL SELECT year, month,'DT Nguồn vốn', tenkhuvuc, '0' FROM cte3
        UNION ALL SELECT year, month,'CP vốn TT 2', tenkhuvuc, cp_von_tt2::TEXT FROM cte3
        UNION ALL SELECT year, month,'CP vốn TT 1', tenkhuvuc, '0' FROM cte3
        UNION ALL SELECT year, month,'CP vốn CCTG', tenkhuvuc, cp_von_cctg::TEXT FROM cte3
        UNION ALL SELECT year, month,'Chi phí thuần hoạt động khác', tenkhuvuc, chi_phi_thuan_hd_khac::TEXT FROM cte3
        UNION ALL SELECT year, month,'DT Fintech', tenkhuvuc, '0' FROM cte3
        UNION ALL SELECT year, month,'DT tiểu thương, cá nhân', tenkhuvuc, '0' FROM cte3
        UNION ALL SELECT year, month,'DT Kinh doanh', tenkhuvuc, doanhthu_kinhdoanh::TEXT FROM cte3
        UNION ALL SELECT year, month,'CP hoa hồng', tenkhuvuc, cp_hoahong::TEXT FROM cte3
        UNION ALL SELECT year, month,'CP thuần KD khác', tenkhuvuc, cp_thuankdkhac::TEXT FROM cte3
        UNION ALL SELECT year, month,'CP hợp tác kd tàu (net)', tenkhuvuc, '0' FROM cte3
        UNION ALL SELECT year, month,'Tổng thu nhập hoạt động', tenkhuvuc, tong_thu_nhap_hd::TEXT FROM cte3
        UNION ALL SELECT year, month,'Tổng chi phí hoạt động', tenkhuvuc, tong_chi_phi_hoat_dong::TEXT FROM cte3
        UNION ALL SELECT year, month,'CP thuế, phí', tenkhuvuc, '0' FROM cte3
        UNION ALL SELECT year, month,'CP nhân viên', tenkhuvuc, cp_nhanvien::TEXT FROM cte3
        UNION ALL SELECT year, month,'CP quản lý', tenkhuvuc, cp_quanly::TEXT FROM cte3
        UNION ALL SELECT year, month,'CP tài sản', tenkhuvuc, cp_taisan::TEXT FROM cte3
        UNION ALL SELECT year, month,'Chi phí dự phòng', tenkhuvuc, cp_duphong::TEXT FROM cte3
        UNION ALL SELECT year, month,'2.Số lượng nhân sự ( Sale Manager )', tenkhuvuc, sl_nhansu::TEXT FROM cte3
        UNION ALL SELECT year, month,'3.Chỉ số tài chính', tenkhuvuc, '0' FROM cte3
        UNION ALL SELECT year, month,'CIR (%)', tenkhuvuc, CIR::TEXT FROM cte3
        UNION ALL SELECT year, month,'Margin (%)', tenkhuvuc, Margin::TEXT FROM cte3
        UNION ALL SELECT year, month,'Hiệu suất trên/vốn (%)', tenkhuvuc, hieusuat_von::TEXT FROM cte3
        UNION ALL SELECT year, month,'Hiệu suất BQ/ Nhân sự', tenkhuvuc, hieusuat_bq::TEXT FROM cte3
    ),
    -- Pivot: chuyển khu vực thành cột, tính Total
    pivoted AS (
        SELECT 
        	year, 
        	month,
            "Tiêu chí",
            MAX(CASE WHEN tenkhuvuc = 'Đông Bắc Bộ' THEN value END) AS "Đông Bắc Bộ",
            MAX(CASE WHEN tenkhuvuc = 'Tây Bắc Bộ' THEN value END) AS "Tây Bắc Bộ",
            MAX(CASE WHEN tenkhuvuc = 'Đồng Bằng Sông Hồng' THEN value END) AS "Đồng Bằng Sông Hồng",
            MAX(CASE WHEN tenkhuvuc = 'Bắc Trung Bộ' THEN value END) AS "Bắc Trung Bộ",
            MAX(CASE WHEN tenkhuvuc = 'Nam Trung Bộ' THEN value END) AS "Nam Trung Bộ",
            MAX(CASE WHEN tenkhuvuc = 'Tây Nam Bộ' THEN value END) AS "Tây Nam Bộ",
            MAX(CASE WHEN tenkhuvuc = 'Đông Nam Bộ' THEN value END) AS "Đông Nam Bộ",
            -- Tính Total: ép kiểu TEXT về NUMERIC để tính tổng, rồi cast lại TEXT (bỏ qua header)
            COALESCE(
                SUM(CASE 
                    WHEN "Tiêu chí" NOT LIKE '%Chỉ số tài chính%' 
                         AND "Tiêu chí" NOT IN ('C.Chỉ số tài chính') 
                    THEN value::NUMERIC 
                    ELSE 0 
                END)::TEXT, 
                '0'
            ) AS "Total"
        FROM melted
        GROUP BY year, month, "Tiêu chí"
    )
    -- Insert vào final_report với thứ tự giống Excel
    INSERT INTO final_report (year, month, "Tiêu chí", "Đông Bắc Bộ", "Tây Bắc Bộ", "Đồng Bằng Sông Hồng", "Bắc Trung Bộ", "Nam Trung Bộ", "Tây Nam Bộ", "Đông Nam Bộ", "Total")
    SELECT
        year,
        month,
        "Tiêu chí",
        "Đông Bắc Bộ",
        "Tây Bắc Bộ",
        "Đồng Bằng Sông Hồng",
        "Bắc Trung Bộ",
        "Nam Trung Bộ",
        "Tây Nam Bộ",
        "Đông Nam Bộ",
        "Total"
    FROM pivoted
    ORDER BY month asc,
        CASE 
            WHEN "Tiêu chí" = '1.Lợi nhuận trước thuế' THEN 1
            WHEN "Tiêu chí" = 'Thu nhập từ hoạt động thẻ' THEN 2
            WHEN "Tiêu chí" = 'Lãi trong hạn' THEN 3
            WHEN "Tiêu chí" = 'Lãi quá hạn' THEN 4
            WHEN "Tiêu chí" = 'Phí Bảo hiểm' THEN 5
            WHEN "Tiêu chí" = 'Phí tăng hạn mức' THEN 6
            WHEN "Tiêu chí" = 'Phí thanh toán chậm, thu từ ngoại bảng, khác…' THEN 7
            WHEN "Tiêu chí" = 'Chi phí thuần KDV' THEN 8
            WHEN "Tiêu chí" = 'DT Nguồn vốn' THEN 9
            WHEN "Tiêu chí" = 'CP vốn TT 2' THEN 10
            WHEN "Tiêu chí" = 'CP vốn TT 1' THEN 11
            WHEN "Tiêu chí" = 'CP vốn CCTG' THEN 12
            WHEN "Tiêu chí" = 'Chi phí thuần hoạt động khác' THEN 13
            WHEN "Tiêu chí" = 'DT Fintech' THEN 14
            WHEN "Tiêu chí" = 'DT tiểu thương, cá nhân' THEN 15
            WHEN "Tiêu chí" = 'DT Kinh doanh' THEN 16
            WHEN "Tiêu chí" = 'CP hoa hồng' THEN 17
            WHEN "Tiêu chí" = 'CP thuần KD khác' THEN 18
            WHEN "Tiêu chí" = 'CP hợp tác kd tàu (net)' THEN 19
            WHEN "Tiêu chí" = 'Tổng thu nhập hoạt động' THEN 20
            WHEN "Tiêu chí" = 'Tổng chi phí hoạt động' THEN 21
            WHEN "Tiêu chí" = 'CP thuế, phí' THEN 22
            WHEN "Tiêu chí" = 'CP nhân viên' THEN 23
            WHEN "Tiêu chí" = 'CP quản lý' THEN 24
            WHEN "Tiêu chí" = 'CP tài sản' THEN 25
            WHEN "Tiêu chí" = 'Chi phí dự phòng' THEN 26
            WHEN "Tiêu chí" = '2.Số lượng nhân sự ( Sale Manager )' THEN 27
            WHEN "Tiêu chí" = '3.Chỉ số tài chính' THEN 28
            WHEN "Tiêu chí" = 'CIR (%)' THEN 29
            WHEN "Tiêu chí" = 'Margin (%)' THEN 30
            WHEN "Tiêu chí" = 'Hiệu suất trên/vốn (%)' THEN 31
            WHEN "Tiêu chí" = 'Hiệu suất BQ/ Nhân sự' THEN 32
        END;

    -- Cleanup temp tables
    DROP TABLE IF EXISTS cp_cctg;
    DROP TABLE IF EXISTS cp_tt2;
END;
$$;

-- Gọi lệnh : phải gọi cả 4 theo đúng thứ tự 
-- chỉ muốn tìm dữ liệu thời điểm nào thì truyền thời điểm đó vào , vd generate_report(2023, 1) 
-- Lưu ý : gen_final_report() cx cần trùng tham số với generate_report do để tạo ra final report cuối cùng 
-- bảng final_report sẽ là kết quả cuối cùng lấy ra 

call generate_report(2023, 2) ; 
call gen_final_report(2023, 2) ; 


select * from report 
select * from final_report 



-- Tạo procedure để tính toán các chỉ số tông hợp và xuất ra thành 1 table báo cáo chuẩn 
CREATE OR REPLACE PROCEDURE gen_final_report_to_asm(
    year_pram int,
    month_pram int
)
LANGUAGE plpgsql
AS $$
DECLARE
    -- Bổ sung các biến nếu cần
BEGIN
    -- ---------------------
    -- THÔNG TIN NGƯỜI TẠO
    -- ---------------------
    -- Tên người tạo: Lê Huy Hoàng
    -- Ngày tạo: 2025-Sep-05
    -- Mục đích: Tạo báo cáo cuối cùng bằng cách tính toán các chỉ số tổng hợp từ báo cáo tạm thời, bao gồm lợi nhuận trước thuế, CIR, Margin, v.v.

    -- ---------------------
    -- THÔNG TIN NGƯỜI CẬP NHẬT
    -- ---------------------
    -- Tên người cập nhật: 
    -- Ngày cập nhật: 
    -- Mục đích cập nhật: 

    -- ---------------------
    -- SUMMARY LUỒNG XỬ LÝ
    -- ---------------------
    -- Bước 1: Gọi hàm cp để tạo các bảng tạm thời cho chi phí vốn CCTG và TT2.
    -- Bước 2: Xóa bảng final_report nếu tồn tại.
    -- Bước 3: Tạo bảng final_report mới với cấu trúc chỉ định.
    -- Bước 4: Insert dữ liệu tính toán từ summary_report vào final_report, sử dụng các CTE để tính toán các chỉ số tổng hợp.
    -- Bước 5: Xóa các bảng tạm thời sau khi sử dụng.
    -- Bước 6: Xử lý ngoại lệ và ghi log (nếu cần).

    -- ---------------------
    -- CHI TIẾT CÁC BƯỚC
    -- ---------------------
	
	-- Bước 1: Gọi procedure generate_report để tạo bảng report tạm thời.
    CALL generate_report(year_pram, month_pram);
    
    -- Bước 2: Gọi hàm cp để tạo các bảng tạm thời cho chi phí vốn CCTG và TT2.
    PERFORM cp(year_pram, month_pram); 
	
    -- Bước 3: Xóa bảng final_report_to_asm nếu tồn tại.
    DROP TABLE IF EXISTS final_report_to_asm; 

    -- Bước 4: Tạo bảng final_report_to_asm mới với cấu trúc chỉ định, thêm year và month.
	create table final_report_to_asm (
		year INT,
        month INT,
		tenkhuvuc varchar , 
		"1.Lợi nhuận trước thuế" int8  ,
		 "Thu nhập từ hoạt động thẻ"  int8 ,
		 "Lãi trong hạn" int8 ,
		 "Lãi quá hạn" int8 ,
		 "Phí Bảo hiểm" int8 ,
		 "Phí tăng hạn mức" int8 ,
		"Phí thanh toán chậm, thu từ ngoại bảng, khác…" int8 ,
		"Chi phí thuần KDV"  int8 ,
		"CP vốn TT 2" int8 ,
		 "CP vốn CCTG" int8 ,
		 "Chi phí thuần hoạt động khác"  int8 ,
		"DT Kinh doanh" int8 ,
		 "CP hoa hồng"  int8 ,
		 "CP thuần KD khác" int8 ,
		"Tổng thu nhập hoạt động" int8 ,
		"Tổng chi phí hoạt động" int8 ,
		"CP nhân viên" int8 ,
		"CP quản lý" int8 ,
		"CP tài sản" int8 ,
		"Chi phí dự phòng" int8 ,
		"2. Số lượng nhân sự ( Sale Manager )" int8 ,
		"CIR (%)" numeric ,
		"Margin (%)" numeric ,
		"Hiệu suất trên/vốn (%)" numeric ,
		"Hiệu suất BQ/ Nhân sự"numeric 
	) ;

    -- Bước 5: Insert dữ liệu tính toán từ summary_report vào final_report, sử dụng các CTE để tính toán các chỉ số tổng hợp.
	INSERT INTO final_report_to_asm (
        year, month, tenkhuvuc,
        "1.Lợi nhuận trước thuế",
        "Thu nhập từ hoạt động thẻ",
        "Lãi trong hạn",
        "Lãi quá hạn",
        "Phí Bảo hiểm",
        "Phí tăng hạn mức",
        "Phí thanh toán chậm, thu từ ngoại bảng, khác…",
        "Chi phí thuần KDV",
        "CP vốn TT 2",
        "CP vốn CCTG",
        "Chi phí thuần hoạt động khác",
        "DT Kinh doanh",
        "CP hoa hồng",
        "CP thuần KD khác",
        "Tổng thu nhập hoạt động",
        "Tổng chi phí hoạt động",
        "CP nhân viên",
        "CP quản lý",
        "CP tài sản",
        "Chi phí dự phòng",
        "2. Số lượng nhân sự ( Sale Manager )",
        "CIR (%)",
        "Margin (%)",
        "Hiệu suất trên/vốn (%)",
        "Hiệu suất BQ/ Nhân sự" 
	)
	with cte1 as (
		select 
			s1.year,
	        s1.month,
			s1.tenkhuvuc , 
			lai_trong_han + lai_qua_han + phi_bao_hiem + phi_tang_han_muc + phi_thanh_toan_cham as thu_nhap_tu_hd_the , 
			lai_trong_han , 
			lai_qua_han ,
			phi_bao_hiem ,
			phi_tang_han_muc ,
			phi_thanh_toan_cham ,
			cp_von_tt2 + cp_von_CCTG as chi_phi_thuan_KDV , 
			cp_von_tt2 ,
			cp_von_cctg ,
			doanhthu_kinhdoanh + cp_hoahong + cp_thuankdkhac as chi_phi_thuan_hd_khac , 
			doanhthu_kinhdoanh , 
			cp_hoahong , 
			cp_thuankdkhac ,
			cp_nhanvien + cp_quanly + cp_taisan as tong_chi_phi_hoat_dong , 
			cp_nhanvien , 
			cp_quanly , 
			cp_taisan , 
			cp_duphong ,
			s2.sl_nhansu 
		from summary_report s1 
		inner join -- thêm metric số lượng nhân sự 
			(select 	
				p.area_name , 
				count(p.sale_name) as sl_nhansu
			from kpi_asm_data p 
			group by 1 ) s2 
		on s1.tenkhuvuc = s2.area_name 
		WHERE s1.tenkhuvuc != 'Hội Sở'  -- Loại bỏ Hội Sở
            AND s1.year = year_pram
            AND s1.month = month_pram  -- Filter theo năm-tháng
	), cte2 as (
	select 
		* ,
		thu_nhap_tu_hd_the + chi_phi_thuan_KDV + chi_phi_thuan_hd_khac as tong_thu_nhap_hd
	from cte1 ) 
	
	, cte3 as (
	select 
		* , 
		tong_thu_nhap_hd+ tong_chi_phi_hoat_dong+cp_duphong as loi_nhuan_truoc_thue ,
		ROUND((tong_chi_phi_hoat_dong * -1.0 / NULLIF(tong_thu_nhap_hd, 0)) * 100, 2) AS CIR,
        ROUND(((tong_thu_nhap_hd + tong_chi_phi_hoat_dong + cp_duphong) / NULLIF((thu_nhap_tu_hd_the + doanhthu_kinhdoanh), 0)) * 100, 2) AS Margin,
        ROUND(((tong_thu_nhap_hd + tong_chi_phi_hoat_dong + cp_duphong) * -1.0 / NULLIF(chi_phi_thuan_KDV, 0)) * 100, 2) AS hieusuat_von,
        ROUND((tong_thu_nhap_hd + tong_chi_phi_hoat_dong + cp_duphong) / NULLIF(sl_nhansu, 0), 2) AS hieusuat_bq 
	from cte2 ) 
	
	select 
		year,
		month,
		tenkhuvuc ,
		loi_nhuan_truoc_thue , 
		thu_nhap_tu_hd_the ,
		lai_trong_han ,
		lai_qua_han ,
		phi_bao_hiem , 
		phi_tang_han_muc ,
		phi_thanh_toan_cham ,
		chi_phi_thuan_KDV  ,
		cp_von_tt2 ,
		cp_von_cctg ,
		chi_phi_thuan_hd_khac ,
		doanhthu_kinhdoanh ,
		cp_hoahong ,
		cp_thuankdkhac ,
		tong_thu_nhap_hd ,
		tong_chi_phi_hoat_dong ,
		cp_nhanvien ,
		cp_quanly ,
		cp_taisan , 
		cp_duphong ,
		sl_nhansu , 
		cir , 
		margin , 
		hieusuat_von , 
		hieusuat_bq 
	from cte3 
	order by month asc,
		case 
			when tenkhuvuc = 'Đông Bắc Bộ' then 1 
			when tenkhuvuc = 'Tây Bắc Bộ'  then 2
			when tenkhuvuc = 'Đồng Bằng Sông Hồng' then 3 
			when tenkhuvuc = 'Bắc Trung Bộ' then 4
			when tenkhuvuc = 'Nam Trung Bộ' then 5 
			when tenkhuvuc = 'Tây Nam Bộ' then 6 
			WHEN tenkhuvuc = 'Đông Nam Bộ' THEN 7
            ELSE 8  -- Để tránh lỗi nếu có khu vực khác 
		end ; 
	
    -- Bước 6: Xóa các bảng tạm thời sau khi sử dụng.
	drop table if exists cp_cctg ;
	drop table if exists cp_tt2 ;

    -- Bước 7: Xử lý ngoại lệ và ghi log (nếu cần).
    EXCEPTION
        WHEN others THEN
            -- Xử lý ngoại lệ ở đây
            -- Có thể ghi log hoặc xử lý các tình huống đặc biệt
            RAISE; -- Tùy chọn để re-raise ngoại lệ
END;
$$ ;


-- Gọi lệnh : phải gọi cả 4 theo đúng thứ tự 
-- chỉ muốn tìm dữ liệu tháng nào thì truyền tháng đó vào , vd generate_report(2023, 1) 
-- Lưu ý : gen_final_report() cx cần trùng tham số với generate_report do để tạo ra final report cuối cùng 
-- bảng final_report sẽ là kết quả cuối cùng lấy ra 

call generate_report(2023, 2) ; 
call gen_final_report_to_asm (2023, 2) ; 


select * from report 
select * from final_report_to_asm frta 


--procedure để so sánh đối chiếu số liệu các tháng trong khu vực
CREATE OR REPLACE PROCEDURE gen_final_report_to_asm_multi_month(
    year_pram INT,
    month_start INT,
    month_end INT
)
LANGUAGE plpgsql
AS $$
DECLARE
    m INT;
BEGIN
    -- Xóa bảng cũ nếu có
    DROP TABLE IF EXISTS final_report_to_asm_multi;

    -- Tạo bảng tổng hợp lưu nhiều tháng
    CREATE TABLE final_report_to_asm_multi (
        year INT,
        month INT,
        tenkhuvuc VARCHAR,
        "1.Lợi nhuận trước thuế" INT8,
        "Thu nhập từ hoạt động thẻ" INT8,
        "Lãi trong hạn" INT8,
        "Lãi quá hạn" INT8,
        "Phí Bảo hiểm" INT8,
        "Phí tăng hạn mức" INT8,
        "Phí thanh toán chậm, thu từ ngoại bảng, khác…" INT8,
        "Chi phí thuần KDV" INT8,
        "CP vốn TT 2" INT8,
        "CP vốn CCTG" INT8,
        "Chi phí thuần hoạt động khác" INT8,
        "DT Kinh doanh" INT8,
        "CP hoa hồng" INT8,
        "CP thuần KD khác" INT8,
        "Tổng thu nhập hoạt động" INT8,
        "Tổng chi phí hoạt động" INT8,
        "CP nhân viên" INT8,
        "CP quản lý" INT8,
        "CP tài sản" INT8,
        "Chi phí dự phòng" INT8,
        "2. Số lượng nhân sự ( Sale Manager )" INT8,
        "CIR (%)" NUMERIC,
        "Margin (%)" NUMERIC,
        "Hiệu suất trên/vốn (%)" NUMERIC,
        "Hiệu suất BQ/ Nhân sự" NUMERIC
    );

    -- Lặp qua từng tháng để tổng hợp
    FOR m IN month_start..month_end LOOP
        RAISE NOTICE 'Generating report for month %', m;

        -- Bước 1: tạo dữ liệu tháng
        CALL generate_report(year_pram, m);
        PERFORM cp(year_pram, m);

        -- Bước 2: tính toán chi tiết cho tháng đó (như gen_final_report_to_asm)
        INSERT INTO final_report_to_asm_multi (
            year, month, tenkhuvuc,
            "1.Lợi nhuận trước thuế",
            "Thu nhập từ hoạt động thẻ",
            "Lãi trong hạn",
            "Lãi quá hạn",
            "Phí Bảo hiểm",
            "Phí tăng hạn mức",
            "Phí thanh toán chậm, thu từ ngoại bảng, khác…",
            "Chi phí thuần KDV",
            "CP vốn TT 2",
            "CP vốn CCTG",
            "Chi phí thuần hoạt động khác",
            "DT Kinh doanh",
            "CP hoa hồng",
            "CP thuần KD khác",
            "Tổng thu nhập hoạt động",
            "Tổng chi phí hoạt động",
            "CP nhân viên",
            "CP quản lý",
            "CP tài sản",
            "Chi phí dự phòng",
            "2. Số lượng nhân sự ( Sale Manager )",
            "CIR (%)",
            "Margin (%)",
            "Hiệu suất trên/vốn (%)",
            "Hiệu suất BQ/ Nhân sự"
        )
        WITH cte1 AS (
            SELECT 
                s1.year,
                s1.month,
                s1.tenkhuvuc,
                lai_trong_han + lai_qua_han + phi_bao_hiem + phi_tang_han_muc + phi_thanh_toan_cham AS thu_nhap_tu_hd_the,
                lai_trong_han,
                lai_qua_han,
                phi_bao_hiem,
                phi_tang_han_muc,
                phi_thanh_toan_cham,
                cp_von_tt2 + cp_von_cctg AS chi_phi_thuan_KDV,
                cp_von_tt2,
                cp_von_cctg,
                doanhthu_kinhdoanh + cp_hoahong + cp_thuankdkhac AS chi_phi_thuan_hd_khac,
                doanhthu_kinhdoanh,
                cp_hoahong,
                cp_thuankdkhac,
                cp_nhanvien + cp_quanly + cp_taisan AS tong_chi_phi_hoat_dong,
                cp_nhanvien,
                cp_quanly,
                cp_taisan,
                cp_duphong,
                s2.sl_nhansu
            FROM summary_report s1
            INNER JOIN (
                SELECT 
                    p.area_name,
                    COUNT(p.sale_name) AS sl_nhansu
                FROM kpi_asm_data p
                GROUP BY 1
            ) s2 ON s1.tenkhuvuc = s2.area_name
            WHERE s1.tenkhuvuc != 'Hội Sở'
              AND s1.year = year_pram
              AND s1.month = m
        ),
        cte2 AS (
            SELECT *,
                   thu_nhap_tu_hd_the + chi_phi_thuan_KDV + chi_phi_thuan_hd_khac AS tong_thu_nhap_hd
            FROM cte1
        ),
        cte3 AS (
            SELECT *,
                   tong_thu_nhap_hd + tong_chi_phi_hoat_dong + cp_duphong AS loi_nhuan_truoc_thue,
                   ROUND((tong_chi_phi_hoat_dong * -1.0 / NULLIF(tong_thu_nhap_hd, 0)) * 100, 2) AS CIR,
                   ROUND(((tong_thu_nhap_hd + tong_chi_phi_hoat_dong + cp_duphong) / NULLIF((thu_nhap_tu_hd_the + doanhthu_kinhdoanh), 0)) * 100, 2) AS Margin,
                   ROUND(((tong_thu_nhap_hd + tong_chi_phi_hoat_dong + cp_duphong) * -1.0 / NULLIF(chi_phi_thuan_KDV, 0)) * 100, 2) AS hieusuat_von,
                   ROUND((tong_thu_nhap_hd + tong_chi_phi_hoat_dong + cp_duphong) / NULLIF(sl_nhansu, 0), 2) AS hieusuat_bq
            FROM cte2
        )
        SELECT
            year,
            month,
            tenkhuvuc,
            loi_nhuan_truoc_thue,
            thu_nhap_tu_hd_the,
            lai_trong_han,
            lai_qua_han,
            phi_bao_hiem,
            phi_tang_han_muc,
            phi_thanh_toan_cham,
            chi_phi_thuan_KDV,
            cp_von_tt2,
            cp_von_cctg,
            chi_phi_thuan_hd_khac,
            doanhthu_kinhdoanh,
            cp_hoahong,
            cp_thuankdkhac,
            tong_thu_nhap_hd,
            tong_chi_phi_hoat_dong,
            cp_nhanvien,
            cp_quanly,
            cp_taisan,
            cp_duphong,
            sl_nhansu,
            CIR,
            Margin,
            hieusuat_von,
            hieusuat_bq
        FROM cte3
        ORDER BY tenkhuvuc, month;
    END LOOP;

    -- Cuối cùng: sắp xếp dữ liệu toàn bộ bảng
    CREATE TABLE temp_sorted AS
    SELECT * FROM final_report_to_asm_multi
    ORDER BY tenkhuvuc, month;

    DROP TABLE final_report_to_asm_multi;
    ALTER TABLE temp_sorted RENAME TO final_report_to_asm_multi;

    RAISE NOTICE '✅ Báo cáo nhiều tháng đã được tạo thành công!';
END;
$$;

CALL gen_final_report_to_asm_multi_month(2023, 1, 6);
SELECT * FROM final_report_to_asm_multi;




CREATE OR REPLACE PROCEDURE gen_final_report_multi(
    year_pram INT,
    month_start INT,
    month_end INT
)
LANGUAGE plpgsql
AS $$
DECLARE
    m INT;
BEGIN
    DROP TABLE IF EXISTS final_report_multi;
    CREATE TABLE final_report_multi (
        year INT,
        month INT,
        "Tiêu chí" TEXT, 
        "Đông Bắc Bộ" TEXT, 
        "Tây Bắc Bộ" TEXT, 
        "Đồng Bằng Sông Hồng" TEXT, 
        "Bắc Trung Bộ" TEXT, 
        "Nam Trung Bộ" TEXT, 
        "Tây Nam Bộ" TEXT, 
        "Đông Nam Bộ" TEXT, 
        "Total" TEXT
    );

    FOR m IN month_start..month_end LOOP
        RAISE NOTICE 'Đang tạo báo cáo cho tháng %', m;

        CALL generate_report(year_pram, m);
        PERFORM cp(year_pram, m);

        WITH cte1 AS (
            SELECT 
                s1.year,
                s1.month,
                s1.tenkhuvuc, 
                lai_trong_han + lai_qua_han + phi_bao_hiem + phi_tang_han_muc + phi_thanh_toan_cham AS thu_nhap_tu_hd_the, 
                lai_trong_han, 
                lai_qua_han,
                phi_bao_hiem,
                phi_tang_han_muc,
                phi_thanh_toan_cham,
                cp_von_tt2 + cp_von_cctg AS chi_phi_thuan_KDV, 
                cp_von_tt2,
                cp_von_cctg,
                doanhthu_kinhdoanh + cp_hoahong + cp_thuankdkhac AS chi_phi_thuan_hd_khac, 
                doanhthu_kinhdoanh, 
                cp_hoahong, 
                cp_thuankdkhac,
                cp_nhanvien + cp_quanly + cp_taisan AS tong_chi_phi_hoat_dong, 
                cp_nhanvien, 
                cp_quanly, 
                cp_taisan, 
                cp_duphong,
                s2.sl_nhansu 
            FROM summary_report s1 
            INNER JOIN (
                SELECT area_name, COUNT(sale_name) AS sl_nhansu
                FROM kpi_asm_data 
                GROUP BY 1
            ) s2 ON s1.tenkhuvuc = s2.area_name 
            WHERE s1.tenkhuvuc != 'Hội Sở'
              AND s1.year = year_pram
              AND s1.month = m
        ),
        cte2 AS (
            SELECT *,
                thu_nhap_tu_hd_the + chi_phi_thuan_KDV + chi_phi_thuan_hd_khac AS tong_thu_nhap_hd
            FROM cte1
        ),
        cte3 AS (
            SELECT *,
                tong_thu_nhap_hd + tong_chi_phi_hoat_dong + cp_duphong AS loi_nhuan_truoc_thue,
                ROUND((tong_chi_phi_hoat_dong * -1.0 / NULLIF(tong_thu_nhap_hd, 0)) * 100, 2) AS CIR,
                ROUND(((tong_thu_nhap_hd + tong_chi_phi_hoat_dong + cp_duphong) / NULLIF((thu_nhap_tu_hd_the + doanhthu_kinhdoanh), 0)) * 100, 2) AS Margin,
                ROUND(((tong_thu_nhap_hd + tong_chi_phi_hoat_dong + cp_duphong) * -1.0 / NULLIF(chi_phi_thuan_KDV, 0)) * 100, 2) AS hieusuat_von,
                ROUND((tong_thu_nhap_hd + tong_chi_phi_hoat_dong + cp_duphong) / NULLIF(sl_nhansu, 0), 2) AS hieusuat_bq
            FROM cte2
        ),
        melted AS (
            SELECT year, month,'1.Lợi nhuận trước thuế' AS "Tiêu chí", tenkhuvuc, loi_nhuan_truoc_thue::TEXT AS value FROM cte3
            UNION ALL SELECT year, month,'Thu nhập từ hoạt động thẻ', tenkhuvuc, thu_nhap_tu_hd_the::TEXT FROM cte3
            UNION ALL SELECT year, month,'Lãi trong hạn', tenkhuvuc, lai_trong_han::TEXT FROM cte3
            UNION ALL SELECT year, month,'Lãi quá hạn', tenkhuvuc, lai_qua_han::TEXT FROM cte3
            UNION ALL SELECT year, month,'Phí Bảo hiểm', tenkhuvuc, phi_bao_hiem::TEXT FROM cte3
            UNION ALL SELECT year, month,'Phí tăng hạn mức', tenkhuvuc, phi_tang_han_muc::TEXT FROM cte3
            UNION ALL SELECT year, month,'Phí thanh toán chậm, thu từ ngoại bảng, khác…', tenkhuvuc, phi_thanh_toan_cham::TEXT FROM cte3
            UNION ALL SELECT year, month,'Chi phí thuần KDV', tenkhuvuc, chi_phi_thuan_KDV::TEXT FROM cte3
            UNION ALL SELECT year, month,'CP vốn TT 2', tenkhuvuc, cp_von_tt2::TEXT FROM cte3
            UNION ALL SELECT year, month,'CP vốn CCTG', tenkhuvuc, cp_von_cctg::TEXT FROM cte3
            UNION ALL SELECT year, month,'Chi phí thuần hoạt động khác', tenkhuvuc, chi_phi_thuan_hd_khac::TEXT FROM cte3
            UNION ALL SELECT year, month,'DT Kinh doanh', tenkhuvuc, doanhthu_kinhdoanh::TEXT FROM cte3
            UNION ALL SELECT year, month,'CP hoa hồng', tenkhuvuc, cp_hoahong::TEXT FROM cte3
            UNION ALL SELECT year, month,'CP thuần KD khác', tenkhuvuc, cp_thuankdkhac::TEXT FROM cte3
            UNION ALL SELECT year, month,'Tổng thu nhập hoạt động', tenkhuvuc, tong_thu_nhap_hd::TEXT FROM cte3
            UNION ALL SELECT year, month,'Tổng chi phí hoạt động', tenkhuvuc, tong_chi_phi_hoat_dong::TEXT FROM cte3
            UNION ALL SELECT year, month,'CP nhân viên', tenkhuvuc, cp_nhanvien::TEXT FROM cte3
            UNION ALL SELECT year, month,'CP quản lý', tenkhuvuc, cp_quanly::TEXT FROM cte3
            UNION ALL SELECT year, month,'CP tài sản', tenkhuvuc, cp_taisan::TEXT FROM cte3
            UNION ALL SELECT year, month,'Chi phí dự phòng', tenkhuvuc, cp_duphong::TEXT FROM cte3
            UNION ALL SELECT year, month,'2.Số lượng nhân sự ( Sale Manager )', tenkhuvuc, sl_nhansu::TEXT FROM cte3
            UNION ALL SELECT year, month,'CIR (%)', tenkhuvuc, CIR::TEXT FROM cte3
            UNION ALL SELECT year, month,'Margin (%)', tenkhuvuc, Margin::TEXT FROM cte3
            UNION ALL SELECT year, month,'Hiệu suất trên/vốn (%)', tenkhuvuc, hieusuat_von::TEXT FROM cte3
            UNION ALL SELECT year, month,'Hiệu suất BQ/ Nhân sự', tenkhuvuc, hieusuat_bq::TEXT FROM cte3
        ),
        pivoted AS (
            SELECT 
                year, month, "Tiêu chí",
                MAX(CASE WHEN tenkhuvuc = 'Đông Bắc Bộ' THEN value END) AS "Đông Bắc Bộ",
                MAX(CASE WHEN tenkhuvuc = 'Tây Bắc Bộ' THEN value END) AS "Tây Bắc Bộ",
                MAX(CASE WHEN tenkhuvuc = 'Đồng Bằng Sông Hồng' THEN value END) AS "Đồng Bằng Sông Hồng",
                MAX(CASE WHEN tenkhuvuc = 'Bắc Trung Bộ' THEN value END) AS "Bắc Trung Bộ",
                MAX(CASE WHEN tenkhuvuc = 'Nam Trung Bộ' THEN value END) AS "Nam Trung Bộ",
                MAX(CASE WHEN tenkhuvuc = 'Tây Nam Bộ' THEN value END) AS "Tây Nam Bộ",
                MAX(CASE WHEN tenkhuvuc = 'Đông Nam Bộ' THEN value END) AS "Đông Nam Bộ",
                COALESCE(
                    SUM(CASE 
                        WHEN "Tiêu chí" NOT LIKE '%Chỉ số tài chính%' THEN value::NUMERIC 
                        ELSE 0 
                    END)::TEXT, '0'
                ) AS "Total"
            FROM melted
            GROUP BY year, month, "Tiêu chí"
        )
        INSERT INTO final_report_multi
        SELECT * FROM pivoted
        ORDER BY 
            month ASC,
            CASE 
                WHEN "Tiêu chí" = '1.Lợi nhuận trước thuế' THEN 1
                WHEN "Tiêu chí" = 'Thu nhập từ hoạt động thẻ' THEN 2
                WHEN "Tiêu chí" = 'Lãi trong hạn' THEN 3
                WHEN "Tiêu chí" = 'Lãi quá hạn' THEN 4
                WHEN "Tiêu chí" = 'Phí Bảo hiểm' THEN 5
                WHEN "Tiêu chí" = 'Phí tăng hạn mức' THEN 6
                WHEN "Tiêu chí" = 'Phí thanh toán chậm, thu từ ngoại bảng, khác…' THEN 7
                WHEN "Tiêu chí" = 'Chi phí thuần KDV' THEN 8
                WHEN "Tiêu chí" = 'CP vốn TT 2' THEN 9
                WHEN "Tiêu chí" = 'CP vốn CCTG' THEN 10
                WHEN "Tiêu chí" = 'Chi phí thuần hoạt động khác' THEN 11
                WHEN "Tiêu chí" = 'DT Kinh doanh' THEN 12
                WHEN "Tiêu chí" = 'CP hoa hồng' THEN 13
                WHEN "Tiêu chí" = 'CP thuần KD khác' THEN 14
                WHEN "Tiêu chí" = 'Tổng thu nhập hoạt động' THEN 15
                WHEN "Tiêu chí" = 'Tổng chi phí hoạt động' THEN 16
                WHEN "Tiêu chí" = 'CP nhân viên' THEN 17
                WHEN "Tiêu chí" = 'CP quản lý' THEN 18
                WHEN "Tiêu chí" = 'CP tài sản' THEN 19
                WHEN "Tiêu chí" = 'Chi phí dự phòng' THEN 20
                WHEN "Tiêu chí" = '2.Số lượng nhân sự ( Sale Manager )' THEN 21
                WHEN "Tiêu chí" = 'CIR (%)' THEN 22
                WHEN "Tiêu chí" = 'Margin (%)' THEN 23
                WHEN "Tiêu chí" = 'Hiệu suất trên/vốn (%)' THEN 24
                WHEN "Tiêu chí" = 'Hiệu suất BQ/ Nhân sự' THEN 25
            END;

        DROP TABLE IF EXISTS cp_cctg;
        DROP TABLE IF EXISTS cp_tt2;
    END LOOP;

    RAISE NOTICE '✅ Báo cáo nhiều tháng đã được tạo thành công (đúng thứ tự Tiêu chí)!';
END;
$$;


CALL gen_final_report_multi(2023, 1, 6);

select * from final_report_multi 


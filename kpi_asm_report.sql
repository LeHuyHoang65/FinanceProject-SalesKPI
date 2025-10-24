drop function if exists npl()

create or replace function npl()
returns void as 
$$ 
begin 
	-- Phục vụ tính npl_trc_wo_lũy kế 
	-- Bước 1 : tạo bảng tạm dnck_avg_345_trc_wot2 chứa 2 column là mã_vùng và dnck_avg_345_trcwo
	create temp table dnck_avg_345_trc_wot2 as 
	with cte1 as (
	select 
		kpi_month , outstanding_principal , write_off_month , write_off_balance_principal , max_bucket , area_cde
	from fact_kpi_month f
	inner join dim_city dc
	on f.pos_city = dc.pos_city 
		and kpi_month in (202301,202302)
		and max_bucket >= 3 ) 
	, cte2 as (
	select 
		kpi_month , 
		area_cde, 
		sum(outstanding_principal) as du_no_sau_wo 
	from cte1 
	group by 1,2 
	order by 2,1 )
	
	, cte3 as (
	select 
		kpi_month , 
		area_cde ,
		sum(write_off_balance_principal) no_xau_demrangoai
	from cte1 
	where kpi_month = write_off_month 
	group by 1, 2
	order by 2,1 ) 
	, cte4 as (
	select 
		cte2.kpi_month , 
		cte2.area_cde ,
		du_no_sau_wo , 
		coalesce (no_xau_demrangoai,0) as no_xau_demrangoai  
	from cte2 
	left join cte3 
	on cte2.kpi_month =cte3.kpi_month  and cte2.area_cde = cte3.area_cde 
	order by 2,1  ) 
	
	,cte5 as (
	select 
		* , 
		sum(no_xau_demrangoai) over(partition by area_cde order by kpi_month) as no_xau_congdon
	from cte4 ) 
	, cte6 as (
	select 
		kpi_month , 
		area_cde , 
		(du_no_sau_wo + no_xau_congdon) as du_no_truoc_wo 
	from cte5  ) 
	
	select 
		area_cde , 
		avg(du_no_truoc_wo) as avg_dnck_345_truocWO_T2
	from cte6
	group by 1  ; 

	-- Bước 2 : tạo bảng tạm 2 để tính dnck_avg_trcwo_t2 chứa 2 column mã vùng và dnck_avg_trcwot2
	create temp table dnck_avg_trcwot2 as 
	with cte1 as (
	select 
		kpi_month , outstanding_principal , write_off_month , write_off_balance_principal , coalesce (max_bucket,1) , area_cde 
	from fact_kpi_month f
	inner join dim_city dc 
	on f.pos_city = dc.pos_city 
		and kpi_month in (202301,202302)
		) 
	, cte2 as (
	select 
		kpi_month , 
		area_cde  , 
		sum(outstanding_principal) as du_no_sau_wo 
	from cte1 
	group by 1,2 
	order by 2,1 )
	
	, cte3 as (
	select 
		kpi_month , 
		area_cde ,
		sum(write_off_balance_principal) no_xau_demrangoai
	from cte1 
	where kpi_month = write_off_month 
	group by 1, 2
	order by 2,1 ) 
	, cte4 as (
	select 
		cte2.kpi_month , 
		cte2.area_cde ,
		du_no_sau_wo , 
		coalesce (no_xau_demrangoai,0) as no_xau_demrangoai  
	from cte2 
	left join cte3 
	on cte2.kpi_month =cte3.kpi_month  and cte2.area_cde = cte3.area_cde  
	order by 2,1  ) 
	
	,cte5 as (
	select 
		* , 
		sum(no_xau_demrangoai) over(partition by area_cde order by kpi_month) as no_xau_congdon
	from cte4 ) 
	, cte6 as (
	select 
		kpi_month , 
		area_cde , 
		(du_no_sau_wo + no_xau_congdon) as du_no_truoc_wo 
	from cte5  ) 
	
	select 
		area_cde , 
		avg(du_no_truoc_wo) as avg_dnck_truoc_WO_T2
	from cte6
	group by 1 ;

	create temp table npl_trc_wot2 as 
	select 
		d1.area_cde , 
		(d1.avg_dnck_345_truocWO_T2*100.0/d2.avg_dnck_truoc_WO_T2) as npl_trcwo_t2 
	from dnck_avg_345_trc_wot2 d1
	inner join dnck_avg_trcwot2 d2
	on d1.area_cde = d2.area_cde ;

	create temp table before_report as 
	with summary as (
	select 
		202302 as month_key ,
		area_name , 
		email , 
		(ltn_jan+ltn_feb)/2 as ltn_avg ,
		(psdn_jan+psdn_feb)/2*1.0 as psdn_avg , 
		approval_rate_jan*1.0 + approval_rate_feb as approve_rate_avg  
	from kpi_asm_data kad 
	 ) 
	
	select 
		202302 as month_key ,
		da.area_cde as ma_vung ,
		s.area_name ,
		email ,
		ltn_avg , 
		rank() over(order by ltn_avg desc) as rank_ltn_avg , 
		psdn_avg ,
		rank() over(order by psdn_avg desc) as rank_psdn_avg , 
		approve_rate_avg , 
		rank() over (order by approve_rate_avg desc) as rank_approve_rate_avg 
	from summary s
	inner join dim_area da  
	on s.area_name = da.area_name 
		and ltn_avg is not null 
		and psdn_avg is not null 
		and approve_rate_avg is not null 
	order by rank_ltn_avg asc ;
end ;
$$ language plpgsql ; 

drop procedure if exists baoCaoXepHangASM_T2()

create or replace procedure baoCaoXepHangASM_T2()
language plpgsql  
as $$ 
begin 
	perform npl() ; 
	
	drop table if exists report_ASM_T2 ; 
	create table report_ASM_T2(
		month_key int , 
		area_code varchar , 
		area_name varchar , 
		email varchar , 
		ltn_avg numeric,  
		rank_ltn_avg int , 
		psdn_avg numeric ,
		rank_psdn_avg int , 
		approve_rate_avg numeric , 
		rank_approval_rate_avg int , 
		npl_trc_wo_luyke numeric ,
		rank_npl_trc_wo_luyke int 
	) ;

	insert into report_ASM_T2
	select 
		month_key ,
		b.ma_vung ,
		area_name ,
		email ,
		ltn_avg , 
		rank_ltn_avg , 
		psdn_avg ,
		rank_psdn_avg , 
		approve_rate_avg , 
		rank_approve_rate_avg ,
		n.npl_trcwo_t2 ,
		rank() over(order by npl_trcwo_t2 asc ) as rank_npl 
	from before_report  as b
	inner join npl_trc_wot2 as n 
	on b.ma_vung = n.area_cde ; 

	drop table if exists dnck_avg_345_trc_wot2;
	drop table if exists dnck_avg_trcwot2;
	drop table if exists npl_trc_wot2 ;
	drop table if exists before_report ;
end ; 
$$ ; 

call baoCaoXepHangASM_T2() ; 

create or replace procedure gen_report_asm_t2()
language plpgsql 
as $$ 
begin 
	
	
	drop table if exists final_report_asm_t2 ;
	create table final_report_asm_t2 (
	month_key int,
	area_code  varchar ,
	area_name  varchar ,
	email varchar , 
	tong_diem  int8 , 
	rank_final int8, 
	ltn_avg int8,
	rank_ltn_avg int8,
	psdn_avg numeric,
	rank_psdn_avg  int8,
	approve_rate_avg numeric, 
	rank_approval_rate_avg int8 ,
	npl_trc_wo_luyke numeric,
	rank_npl_trc_wo_luyke int8 ,
	diem_quy_mo int8,
	rank_ptkd int8, 
	cir numeric, 
	rank_cir int8, 
	margin numeric, 
	rank_margin int8 ,
	hs_von numeric,
	rank_hs_von int8, 
	hsbq_nhan_su int8,
	rank_hsbq_nhan_su int8,
	diem_fin int8,
	rank_fin int8
	) ;

	insert into final_report_asm_t2 
	with bang_tong_hop1 as (
	select 
		rat.* , 
		(rank_ltn_avg + rank_psdn_avg + rank_approval_rate_avg + rank_npl_trc_wo_luyke) as diem_quy_mo , 
		fr."CIR (%)"  as cir ,
		fr."Margin (%)" as margin, 
		fr."Hiệu suất trên/vốn (%)" as hs_von,
		fr."Hiệu suất BQ/ Nhân sự" as hsbq_nhan_su
	from report_asm_t2 rat  
	inner join final_report_to_asm fr 
	on rat.area_name = fr.tenkhuvuc ) 
	 , bang_tong_hop2 as (
	select 
		* , 
		rank() over(order by diem_quy_mo asc) as rank_ptkd , 
		rank() over(order by cir asc) as rank_cir , 
		rank() over(order by margin asc ) as rank_margin , 
		rank() over(order by hs_von) as rank_hs_von , 
		rank() over(order by hsbq_nhan_su) as rank_hsbq_nhan_su
	from bang_tong_hop1 ) 
	, bang_tong_hop3 as (
	select 
		* , 
		rank() over(order by diem_fin asc) as rank_fin
	from (
		select 
			* , 
			(rank_cir + rank_margin + rank_hs_von + rank_hsbq_nhan_su) as diem_fin
		from bang_tong_hop2 ) ) 
	
	select 
		month_key ,
		area_code ,
		area_name ,
		email, 
		tong_diem  , 
		rank() over(order by tong_diem asc) as rank_final , 
		ltn_avg ,
		rank_ltn_avg ,
		psdn_avg ,
		rank_psdn_avg  ,
		approve_rate_avg , 
		rank_approval_rate_avg  ,
		npl_trc_wo_luyke ,
		rank_npl_trc_wo_luyke ,
		diem_quy_mo ,
		rank_ptkd , 
		cir , 
		rank_cir , 
		margin , 
		rank_margin ,
		hs_von ,
		rank_hs_von , 
		hsbq_nhan_su ,
		rank_hsbq_nhan_su ,
		diem_fin ,
		rank_fin 
	from (
	select 
		* , 
		(diem_quy_mo +diem_fin) as tong_diem
	from bang_tong_hop3 ) ;

end ; 
$$ ; 
	
call gen_report_asm_t2() ; 

select * from report_asm_t2 rat 
select * from final_report_asm_t2 frat 
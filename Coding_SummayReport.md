# Detailed Coding
## Table of Contents
1.[Metrics](#1metrics)  
2.[Calculate Report](#2Calculate-REPORT)  
3.[Calculate Summary Report](#3Calculate-Summary-Report)  

<p align="center">
  <img width="940" height="465" alt="FLOW" src="https://github.com/user-attachments/assets/570dde93-86e0-4569-8961-7afa44947eab" />
</p>  

As illustrated in the diagram above, this is the process I will follow to produce the analysis results. Here, I will create a Summary Report using two datasets: `fact_kpi_month_dataraw` and `fact_txn_month_dataraw`. By generating the Summary Report, we can load the data into Power BI to produce comprehensive dashboards on revenue and expenses.  

To calculate the metrics in the Summary Report, specific business knowledge is required. Below are two files that will support you in understanding the necessary business knowledge: `huongdan_data` and `param_dim_data`.  

In this file, I will focus on calculating the `Summary Report`. The `KPI ASM report` file will follow a similar process, and I will include the code for it in the repository for reference.  
<p align="center">
<img width="1249" height="682" alt="summaryt2" src="https://github.com/user-attachments/assets/d387b5f7-ca3f-46d1-a8f9-293479d50759" />  
</p>

# 1.Metrics
The Summary Report includes numerous metrics. Some of these are aggregate metrics derived from other metrics. Therefore, I will categorize them into two levels:  
## Level 1 Metrics:
- lai_trong_han: Interest within the term
- lai_qua_han: Overdue interest
- phi_bao_hiem: Insurance fee
- phi_tang_han_muc: Limit increase fee
- phi_thanh_toan_cham: Late payment fee
- dt_kinhdoanh: Business revenue
- cp_hoahong: Commission costs
- cp_thuan_kinhdoanhkhac: Other net business costs
- cp_nhanvien: Employee costs
- cp_quanly: Management costs
- cp_taisan: Asset costs
- cp_duphong: Provision costs
- so_luong_nhansu : number saleman  
## Level 2 Metrics:
- cp_von_tt2: Cost of capital TT2
- cp_von_cctg: Cost of capital CCTG
## Aggregate Metrics:
- thu_nhap_hoat_dong_the: Operating income, calculated as:  
  `lai_trong_han + lai_qua_han + phi_bao_hiem + phi_tang_han_muc + phi_thanh_toan_cham`
- Cp_thuan_KDV: Net business costs, calculated as:  
  `cp_von_tt2 + cp_von_cctg`
- cp_thuan_hoat_dong_khac: Net other operating costs, calculated as:  
  `dt_kinhdoanh + cp_hoahong + cp_thuan_kinhdoanhkhac`
- tong_thu_nhap_hoat_dong: Total operating income, calculated as:  
  `thu_nhap_hoat_dong_the + Cp_thuan_KDV + cp_thuan_hoat_dong_khac`
- tong_chi_phi_hoat_dong: Total operating expenses, calculated as:  
  `cp_quanly + cp_nhanvien + cp_taisan`
- loi_nhuan_truoc_thue: Profit before tax, calculated as:  
  `tong_thu_nhap_hoat_dong - tong_chi_phi_hoat_dong - cp_duphong`
- cir: Cost-to-income ratio, calculated as:  
  `tong_chi_phi_hoat_dong / tong_thu_nhap_hoat_dong`
- tong_doanh_thu: Total revenue, calculated as:  
  `dt_kinhdoanh + thu_nhap_hoat_dong_the`
- margin: Margin, calculated as:  
  `loi_nhuan_truoc_thue / tong_doanh_thu`
- hs_von: Capital efficiency, calculated as:  
  `loi_nhuan_truoc_thue / Cp_thuan_KDV`
- hieu_suat_BQ: Average efficiency, calculated as:  
  `loi_nhuan_truoc_thue / so_luong_nhansu`

# 2.Calculate REPORT
Below is a diagram illustrating the calculation process in step 1. 
<p align="center">
	<img width="1672" height="493" alt="Screenshot 2025-10-24 184142" src="https://github.com/user-attachments/assets/b7f3a65e-d36f-4cac-84cb-a9df40d50939" />
</p>  

After importing the two data files, `fact_kpi_month_dataraw` and `fact_txn_month_dataraw`, along with the two guide files `huongdan_data`, we will sequentially calculate the metrics at level 1. Since this project requires the calculation of numerous metrics, to facilitate easy modification and maintenance of the code, I will use functions and procedures.  

After calculating the metrics at level 1, I will create a function to store all these metrics in temporary tables in PostgreSQL. Finally, I will use a procedure to consolidate these temporary tables into a single table `report` that contains all the level 1 metrics.  

Let's canculate Metrics level 1 😎. Here is a diagram illustrating the calculation process for the level 1 metrics.  
<p align="center">
<img width="783" height="442" alt="metrics_lv1" src="https://github.com/user-attachments/assets/8d1fcafd-e292-4497-ac4c-d69cacdf1cd9" />
</p>  

I will calculate the first metric: lai_trong_han in 02/2023.
~~~sql
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
~~~

and result  
| Khu Vực | lai_trong_han |
|---------|---------|
| Tay Nam Bộ | 252.252.873.483,34 |
| Đông Nam Bộ | 104.784.917.684,64 |
| Đông Bằng Sông Hồng | 96.090.675.423,85 |
| Đông Bắc Bộ | 71.210.401.715,68 |
| Nam Trung Bộ | 54.408.780.960,00 |
| Tây Bắc Bộ | 40.355.250.939,22 |
| Bắc Trung Bộ | 19.822.618.842,27 |

Similarly, for the other level 1 metrics, I will provide complete calculation scripts for each metric in the repository for your reference.  

Next, I will use a function to create a temporary table containing the metrics ☺️😚. I will only provide an example of inserting one metric, as including all the level 1 metrics would be too lengthy. You can find the complete script file in the repository.  

I will pass two parameter month_param and year_param to both the function and procedure, allowing for calculations to be performed for the desired time period.  

~~~sql
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
end ;
$$ language plpgsql ;
~~~

Now, after creating the temporary tables for all the metrics, I will use a procedure to generate `Report`, which contains all the level 1 metrics, from these temporary tables.  

~~~sql
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
~~~

When calling the procedure and passing a specific time period, such as (2023, 2), the code inside will be executed, and simultaneously, the previously created temporary tables will be deleted to free up memory.  

~~~sql
call generate_report(2023, 2) ;
select *
from report ;
~~~

| Khu Vực       | lai_trong_han    | lai_qua_han   | phi_bao_hiem | phi_tang_han_muc | phi_thanh_toan_cham | doanhthu_kinhdoanh | cp_hoahong    | cp_nhanvien   | cp_quanly    | cp_taisan    | cp_duphong     |
|---------------|------------------|---------------|---------------|-------------------|---------------------|-------------------|---------------|---------------|--------------|--------------|----------------|
| Đông Bắc      | 71,210.401.716   | 64,191.654    | 2,248.781.647 | 3,625.425.756     | 634.722.991         | 54.515.640        | 54.112.847.483 | 9.172.655.789 | 356.162.358  | 894.738.097  | 40.615.828.951  |
| Tây Bắc Bộ    | 40,355.250.939   | 55,514.989    | 574.178.105   | 2,113.532.018     | 399.710.741         | 46.315.116        | 3.240.656.297  | 6.115.861.629 | 240.733.533  | 598.318.832  | 23.318.975.760  |
| Đông Bằng     | 96,090.675.424   | 87,499.120    | 944.207.739   | 5,004.389.958     | 741.649.960         | 9.1043.735        | 6.169.920.131  | 13.799.752.480| 544.890.364  | 1.249.420.516| 43.413.672.058  |
| Bắc Trung Bộ  | 19,822.618.842   | 765.223       | 60,962.181    | 1,040.022.295     | 156.193.505         | 25.259.007        | 1.630.922.532  | 3.477.629.404 | 136.693.619  | 319.438.705  | 6.498.522.434   |
| Nam Trung Bộ  | 54,408.780.960   | 53,184.939    | 1,325.958.386 | 2,789.252.822     | 270.150.876         | 44.164.241        | 3.188.264.670  | 5.715.311.356 | 222.914.654  | 554.295.559  | 22.308.118.789  |
| Tây Nam Bộ    | 252.252.873.483  | 63,407.212    | 3,895.335.021 | 12,950.572.262    | 2,886.946.171       | 237.313.124       | 16.485.262.716 | 26.733.389.392| 1.026.441.571| 2.753.969.173| 109.179.627.167 |
| Đông Nam Bộ   | 104,784.917.685  | 233,448.619   | 2,485.006.743 | 5,490.859.663     | 1,481.077.682       | 116.750.438       | 7.878.503.667  | 15.110.980.529| 589.810.980  | 1.448.148.541| 58.642.193.852  |

# 3.Calculate Summary Report
Below is a diagram illustrating the calculation process in step 2.  
<p align="center">
	<img width="926" height="422" alt="Screenshot 2025-10-24 185519" src="https://github.com/user-attachments/assets/12e1c4b2-0824-4d22-9159-b4ecfc9f2b67" />
</p>  

After constructing `Report`, I will utilize a function to create temporary tables for calculating level 2 metrics based on the level 1 metrics data from `Report`. Subsequently, I will employ a procedure to compute aggregate metrics and generate the summary report.  

Now, I will create a function to calculate the two metrics, cp_von_tt2 and cp_von_cctg, from `Report`.  

~~~sql
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
~~~

Finally, I will use a procedure to calculate aggregate metrics and build the summary report from the temporary tables created earlier.  

~~~sql
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
~~~

and result after call procedure  

~~~sql
call gen_final_report_to_asm (2023, 2) ; 
select * from final_report_to_asm
~~~


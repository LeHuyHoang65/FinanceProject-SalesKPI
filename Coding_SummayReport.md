# Detailed Coding
## Table of Contents
1.[Metrics](#1metrics)  
2.[Calculate Report1](#2Calculate-REPORT1)  
3.Calculate Summary Report  

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

# 2.Calculate REPORT1
Below is a diagram illustrating the calculation process in step 1. 
<p align="center">
<img width="934" height="277" alt="report1" src="https://github.com/user-attachments/assets/7d3330d6-9ec8-454e-ac01-4cec23bad52e" />
</p>  

After importing the two data files, `fact_kpi_month_dataraw` and `fact_txn_month_dataraw`, along with the two guide files `huongdan_data`, we will sequentially calculate the metrics at level 1. Since this project requires the calculation of numerous metrics, to facilitate easy modification and maintenance of the code, I will use functions and procedures.  

After calculating the metrics at level 1, I will create a function to store all these metrics in temporary tables in PostgreSQL. Finally, I will use a procedure to consolidate these temporary tables into a single table `report1` that contains all the level 1 metrics.  

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

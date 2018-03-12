
-- adding in lb/yr as it may be useful later
-- this version will also create the watershed calculations

drop view if exists analysissusqu.final_lateralshed_04;
create view analysissusqu.final_lateralshed_04 

AS

select nhd.gnis_name
,nhd.nord
,nhd.nordstop
,lu_nhd.tot_cells / 4046.86 	as	total_acres
,lu_nhd.crp_sum	  / 4046.86	as	crp_acre
,lu_nhd.p_crp			
,lu_nhd.fore_sum  / 4046.86	as	fore_acre
,lu_nhd.p_fore			
,lu_nhd.inr_sum	  / 4046.86	as	inr_acre
,lu_nhd.p_inr			
,lu_nhd.ir_sum	  / 4046.86	as	ir_acre
,lu_nhd.p_ir			
,lu_nhd.mo_sum	  / 4046.86	as	mo_acre
,lu_nhd.p_mo			
,lu_nhd.pas_sum	  / 4046.86	as	pas_acre
,lu_nhd.p_pas			
,lu_nhd.tci_sum	  / 4046.86	as	tci_acre
,lu_nhd.p_tci			
,lu_nhd.tct_sum	  / 4046.86	as	tct_acre
,lu_nhd.p_tct			
,lu_nhd.tg_sum	  / 4046.86	as	tg_acre
,lu_nhd.p_tg			
,lu_nhd.wat_sum	  / 4046.86	as	wat_acre
,lu_nhd.p_wat			
,lu_nhd.wlf_sum	  / 4046.86	as	wlf_acre
,lu_nhd.p_wlf			
,lu_nhd.wlo_sum	  / 4046.86	as	wlo_acre
,lu_nhd.p_wlo			
,lu_nhd.wlt_sum2  / 4046.86	as	wlt_acre
,lu_nhd.p_wlt	
,1 - (((lu_nhd.wat_sum + lu_nhd.wlf_sum + lu_nhd.wlo_sum + lu_nhd.wlt_sum2) / lu_nhd.tot_cells) * 0.12) as tn_extict_coef
,1 - (((lu_nhd.wat_sum + lu_nhd.wlf_sum + lu_nhd.wlo_sum + lu_nhd.wlt_sum2) / lu_nhd.tot_cells) * 0.29) as tp_extict_coef
,1 - (((lu_nhd.wat_sum + lu_nhd.wlf_sum + lu_nhd.wlo_sum + lu_nhd.wlt_sum2) / lu_nhd.tot_cells) * 0.84) as tss_extict_coef
,t2.*
,nhd.geom
		
from
(
select	t1.comid
,	nhd.maflowv						as flow_cfs	

	/*
,	sum(case when t1.tn_crp_lbacre is null then 0 else t1.tn_crp_lbacre end)		 								as tn_crp_lbacre
,	sum(case when t1.tp_crp_lbacre is null then 0 else t1.tp_crp_lbacre end)										as tp_crp_lbacre
,	sum(case when t1.tss_crp_lbacre is null then 0 else t1.tss_crp_lbacre end)		 								as tss_crp_lbacre
,	sum(case when t1.tn_crp_lbyr is null then 0 else t1.tn_crp_lbyr end)		 									as tn_crp_lbyr
,	sum(case when t1.tp_crp_lbyr is null then 0 else t1.tp_crp_lbyr end)											as tp_crp_lbyr
,	sum(case when t1.tss_crp_lbyr is null then 0 else t1.tss_crp_lbyr end)		 									as tss_crp_lbyr
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tn_crp_mgl is null then 0 else t1.tn_crp_mgl end) end			 		as tn_crp_mgl
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tp_crp_mgl is null then 0 else t1.tp_crp_mgl end) end			 		as tp_crp_mgl
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tss_crp_mgl is null then 0 else t1.tss_crp_mgl end) end					as tss_crp_mgl

,	sum(case when t1.tn_fore_lbacre is null then 0 else t1.tn_fore_lbacre end)		 								as tn_fore_lbacre
,	sum(case when t1.tp_fore_lbacre is null then 0 else t1.tp_fore_lbacre end)		 								as tp_fore_lbacre
,	sum(case when t1.tss_fore_lbacre is null then 0 else t1.tss_fore_lbacre end)		 								as tss_fore_lbacre
,	sum(case when t1.tn_fore_lbyr is null then 0 else t1.tn_fore_lbyr end)		 									as tn_fore_lbyr
,	sum(case when t1.tp_fore_lbyr is null then 0 else t1.tp_fore_lbyr end)		 									as tp_fore_lbyr
,	sum(case when t1.tss_fore_lbyr is null then 0 else t1.tss_fore_lbyr end)		 								as tss_fore_lbyr
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tn_fore_mgl is null then 0 else t1.tn_fore_mgl end) end					as tn_fore_mgl
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tp_fore_mgl is null then 0 else t1.tp_fore_mgl end) end					as tp_fore_mgl
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tss_fore_mgl is null then 0 else t1.tss_fore_mgl end) end					as tss_fore_mgl

,	sum(case when t1.tn_inr_lbacre is null then 0 else t1.tn_inr_lbacre end)		 								as tn_inr_lbacre
,	sum(case when t1.tp_inr_lbacre is null then 0 else t1.tp_inr_lbacre end)		 								as tp_inr_lbacre
,	sum(case when t1.tss_inr_lbacre is null then 0 else t1.tss_inr_lbacre end)		 								as tss_inr_lbacre
,	sum(case when t1.tn_inr_lbyr is null then 0 else t1.tn_inr_lbyr end)		 									as tn_inr_lbyr
,	sum(case when t1.tp_inr_lbyr is null then 0 else t1.tp_inr_lbyr end)		 									as tp_inr_lbyr
,	sum(case when t1.tss_inr_lbyr is null then 0 else t1.tss_inr_lbyr end)		 									as tss_inr_lbyr
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tn_inr_mgl is null then 0 else t1.tn_inr_mgl end) end				 	as tn_inr_mgl
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tp_inr_mgl is null then 0 else t1.tp_inr_mgl end) end				 	as tp_inr_mgl
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tss_inr_mgl is null then 0 else t1.tss_inr_mgl end) end					as tss_inr_mgl

,	sum(case when t1.tn_ir_lbacre is null then 0 else t1.tn_ir_lbacre end)		 									as tn_ir_lbacre
,	sum(case when t1.tp_ir_lbacre is null then 0 else t1.tp_ir_lbacre end)		 									as tp_ir_lbacre
,	sum(case when t1.tss_ir_lbacre is null then 0 else t1.tss_ir_lbacre end)		 								as tss_ir_lbacre
,	sum(case when t1.tn_ir_lbyr is null then 0 else t1.tn_ir_lbyr end)		 									as tn_ir_lbyr
,	sum(case when t1.tp_ir_lbyr is null then 0 else t1.tp_ir_lbyr end)		 									as tp_ir_lbyr
,	sum(case when t1.tss_ir_lbyr is null then 0 else t1.tss_ir_lbyr end)		 									as tss_ir_lbyr
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tn_ir_mgl is null then 0 else t1.tn_ir_mgl end) end		 				as tn_ir_mgl
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tp_ir_mgl is null then 0 else t1.tp_ir_mgl end) end		 				as tp_ir_mgl
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tss_ir_mgl is null then 0 else t1.tss_ir_mgl end) end		 			as tss_ir_mgl

,	sum(case when t1.tn_mo_lbacre is null then 0 else t1.tn_mo_lbacre end)		 									as tn_mo_lbacre
,	sum(case when t1.tp_mo_lbacre is null then 0 else t1.tp_mo_lbacre end)		 									as tp_mo_lbacre
,	sum(case when t1.tss_mo_lbacre is null then 0 else t1.tss_mo_lbacre end)		 								as tss_mo_lbacre
,	sum(case when t1.tn_mo_lbyr is null then 0 else t1.tn_mo_lbyr end)		 									as tn_mo_lbyr
,	sum(case when t1.tp_mo_lbyr is null then 0 else t1.tp_mo_lbyr end)		 									as tp_mo_lbyr
,	sum(case when t1.tss_mo_lbyr is null then 0 else t1.tss_mo_lbyr end)		 									as tss_mo_lbyr
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tn_mo_mgl is null then 0 else t1.tn_mo_mgl end) end		 				as tn_mo_mgl
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tp_mo_mgl is null then 0 else t1.tp_mo_mgl end) end		 				as tp_mo_mgl
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tss_mo_mgl is null then 0 else t1.tss_mo_mgl end) end			 		as tss_mo_mgl

,	sum(case when t1.tn_pas_lbacre is null then 0 else t1.tn_pas_lbacre end)		 								as tn_pas_lbacre
,	sum(case when t1.tp_pas_lbacre is null then 0 else t1.tp_pas_lbacre end)		 								as tp_pas_lbacre
,	sum(case when t1.tss_pas_lbacre is null then 0 else t1.tss_pas_lbacre end)		 								as tss_pas_lbacre
,	sum(case when t1.tn_pas_lbyr is null then 0 else t1.tn_pas_lbyr end)		 									as tn_pas_lbyr
,	sum(case when t1.tp_pas_lbyr is null then 0 else t1.tp_pas_lbyr end)		 									as tp_pas_lbyr
,	sum(case when t1.tss_pas_lbyr is null then 0 else t1.tss_pas_lbyr end)		 									as tss_pas_lbyr
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tn_pas_mgl is null then 0 else t1.tn_pas_mgl end) end			 		as tn_pas_mgl
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tp_pas_mgl is null then 0 else t1.tp_pas_mgl end) end			 		as tp_pas_mgl
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tss_pas_mgl is null then 0 else t1.tss_pas_mgl end) end					as tss_pas_mgl

,	sum(case when t1.tn_tci_lbacre is null then 0 else t1.tn_tci_lbacre end)		 								as tn_tci_lbacre
,	sum(case when t1.tp_tci_lbacre is null then 0 else t1.tp_tci_lbacre end)		 								as tp_tci_lbacre
,	sum(case when t1.tss_tci_lbacre is null then 0 else t1.tss_tci_lbacre end)		 								as tss_tci_lbacre
,	sum(case when t1.tn_tci_lbyr is null then 0 else t1.tn_tci_lbyr end)		 									as tn_tci_lbyr
,	sum(case when t1.tp_tci_lbyr is null then 0 else t1.tp_tci_lbyr end)		 									as tp_tci_lbyr
,	sum(case when t1.tss_tci_lbyr is null then 0 else t1.tss_tci_lbyr end)		 									as tss_tci_lbyr
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tn_tci_mgl is null then 0 else t1.tn_tci_mgl end) end		 			as tn_tci_mgl
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tp_tci_mgl is null then 0 else t1.tp_tci_mgl end) end		 			as tp_tci_mgl
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tss_tci_mgl is null then 0 else t1.tss_tci_mgl end) end					as tss_tci_mgl

,	sum(case when t1.tn_tct_lbacre is null then 0 else t1.tn_tct_lbacre end)		 								as tn_tct_lbacre
,	sum(case when t1.tp_tct_lbacre is null then 0 else t1.tp_tct_lbacre end)		 								as tp_tct_lbacre
,	sum(case when t1.tss_tct_lbacre is null then 0 else t1.tss_tct_lbacre end)		 								as tss_tct_lbacre
,	sum(case when t1.tn_tct_lbyr is null then 0 else t1.tn_tct_lbyr end)		 									as tn_tct_lbyr
,	sum(case when t1.tp_tct_lbyr is null then 0 else t1.tp_tct_lbyr end)		 									as tp_tct_lbyr
,	sum(case when t1.tss_tct_lbyr is null then 0 else t1.tss_tct_lbyr end)		 									as tss_tct_lbyr
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tn_tct_mgl is null then 0 else t1.tn_tct_mgl end) end		 			as tn_tct_mgl
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tp_tct_mgl is null then 0 else t1.tp_tct_mgl end) end		 			as tp_tct_mgl
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tss_tct_mgl is null then 0 else t1.tss_tct_mgl end) end					as tss_tct_mgl

,	sum(case when t1.tn_tg_lbacre is null then 0 else t1.tn_tg_lbacre end)		 									as tn_tg_lbacre
,	sum(case when t1.tp_tg_lbacre is null then 0 else t1.tp_tg_lbacre end)		 									as tp_tg_lbacre
,	sum(case when t1.tss_tg_lbacre is null then 0 else t1.tss_tg_lbacre end)		 								as tss_tg_lbacre
,	sum(case when t1.tn_tg_lbyr is null then 0 else t1.tn_tg_lbyr end)		 									as tn_tg_lbyr
,	sum(case when t1.tp_tg_lbyr is null then 0 else t1.tp_tg_lbyr end)		 									as tp_tg_lbyr
,	sum(case when t1.tss_tg_lbyr is null then 0 else t1.tss_tg_lbyr end)		 									as tss_tg_lbyr
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tn_tg_mgl is null then 0 else t1.tn_tg_mgl end) end		 				as tn_tg_mgl
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tp_tg_mgl is null then 0 else t1.tp_tg_mgl end) end		 				as tp_tg_mgl
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tss_tg_mgl is null then 0 else t1.tss_tg_mgl end) end		 			as tss_tg_mgl

,	sum(case when t1.tn_wat_lbacre is null then 0 else t1.tn_wat_lbacre end)		 								as tn_wat_lbacre
,	sum(case when t1.tp_wat_lbacre is null then 0 else t1.tp_wat_lbacre end)		 								as tp_wat_lbacre
,	sum(case when t1.tss_wat_lbacre is null then 0 else t1.tss_wat_lbacre end)		 								as tss_wat_lbacre
,	sum(case when t1.tn_wat_lbyr is null then 0 else t1.tn_wat_lbyr end)		 									as tn_wat_lbyr
,	sum(case when t1.tp_wat_lbyr is null then 0 else t1.tp_wat_lbyr end)		 									as tp_wat_lbyr
,	sum(case when t1.tss_wat_lbyr is null then 0 else t1.tss_wat_lbyr end)		 									as tss_wat_lbyr
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tn_wat_mgl is null then 0 else t1.tn_wat_mgl end) end		 			as tn_wat_mgl
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tp_wat_mgl is null then 0 else t1.tp_wat_mgl end) end		 			as tp_wat_mgl
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tss_wat_mgl is null then 0 else t1.tss_wat_mgl end) end					as tss_wat_mgl

,	sum(case when t1.tn_wlf_lbacre is null then 0 else t1.tn_wlf_lbacre end)		 								as tn_wlf_lbacre
,	sum(case when t1.tp_wlf_lbacre is null then 0 else t1.tp_wlf_lbacre end)		 								as tp_wlf_lbacre
,	sum(case when t1.tss_wlf_lbacre is null then 0 else t1.tss_wlf_lbacre end)		 								as tss_wlf_lbacre
,	sum(case when t1.tn_wlf_lbyr is null then 0 else t1.tn_wlf_lbyr end)		 									as tn_wlf_lbyr
,	sum(case when t1.tp_wlf_lbyr is null then 0 else t1.tp_wlf_lbyr end)		 									as tp_wlf_lbyr
,	sum(case when t1.tss_wlf_lbyr is null then 0 else t1.tss_wlf_lbyr end)		 									as tss_wlf_lbyr
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tn_wlf_mgl is null then 0 else t1.tn_wlf_mgl end) end		 			as tn_wlf_mgl
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tp_wlf_mgl is null then 0 else t1.tp_wlf_mgl end) end		 			as tp_wlf_mgl
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tss_wlf_mgl is null then 0 else t1.tss_wlf_mgl end) end					as tss_wlf_mgl

,	sum(case when t1.tn_wlo_lbacre is null then 0 else t1.tn_wlo_lbacre end)		 								as tn_wlo_lbacre
,	sum(case when t1.tp_wlo_lbacre is null then 0 else t1.tp_wlo_lbacre end)		 								as tp_wlo_lbacre
,	sum(case when t1.tss_wlo_lbacre is null then 0 else t1.tss_wlo_lbacre end)		 								as tss_wlo_lbacre
,	sum(case when t1.tn_wlo_lbyr is null then 0 else t1.tn_wlo_lbyr end)		 									as tn_wlo_lbyr
,	sum(case when t1.tp_wlo_lbyr is null then 0 else t1.tp_wlo_lbyr end)		 									as tp_wlo_lbyr
,	sum(case when t1.tss_wlo_lbyr is null then 0 else t1.tss_wlo_lbyr end)		 									as tss_wlo_lbyr
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tn_wlo_mgl is null then 0 else t1.tn_wlo_mgl end) end		 			as tn_wlo_mgl
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tp_wlo_mgl is null then 0 else t1.tp_wlo_mgl end) end		 			as tp_wlo_mgl
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tss_wlo_mgl is null then 0 else t1.tss_wlo_mgl end) end					as tss_wlo_mgl

,	sum(case when t1.tn_construction_lbacre is null then 0 else t1.tn_construction_lbacre end)		 						as tn_construction_lbacre
,	sum(case when t1.tp_construction_lbacre is null then 0 else t1.tp_construction_lbacre end)		 						as tp_construction_lbacre
,	sum(case when t1.tss_construction_lbacre is null then 0 else t1.tss_construction_lbacre end)		 						as tss_construction_lbacre
,	sum(case when t1.tn_construction_lbyr is null then 0 else t1.tn_construction_lbyr end)		 							as tn_construction_lbyr
,	sum(case when t1.tp_construction_lbyr is null then 0 else t1.tp_construction_lbyr end)		 							as tp_construction_lbyr
,	sum(case when t1.tss_construction_lbyr is null then 0 else t1.tss_construction_lbyr end)		 						as tss_construction_lbyr
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tn_construction_mgl is null then 0 else t1.tn_construction_mgl end) end			as tn_construction_mgl
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tp_construction_mgl is null then 0 else t1.tp_construction_mgl end) end			as tp_construction_mgl
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tss_construction_mgl is null then 0 else t1.tss_construction_mgl end) end			as tss_construction_mgl

,	sum(case when t1.tn_rpd_lbacre is null then 0 else t1.tn_rpd_lbacre end)		 								as tn_rpd_lbacre
,	sum(case when t1.tp_rpd_lbacre is null then 0 else t1.tp_rpd_lbacre end)		 								as tp_rpd_lbacre
,	sum(case when t1.tss_rpd_lbacre is null then 0 else t1.tss_rpd_lbacre end)		 								as tss_rpd_lbacre
,	sum(case when t1.tn_rpd_lbyr is null then 0 else t1.tn_rpd_lbyr end)		 									as tn_rpd_lbyr
,	sum(case when t1.tp_rpd_lbyr is null then 0 else t1.tp_rpd_lbyr end)		 									as tp_rpd_lbyr
,	sum(case when t1.tss_rpd_lbyr is null then 0 else t1.tss_rpd_lbyr end)		 									as tss_rpd_lbyr
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tn_rpd_mgl is null then 0 else t1.tn_rpd_mgl end) end		 			as tn_rpd_mgl
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tp_rpd_mgl is null then 0 else t1.tp_rpd_mgl end) end		 			as tp_rpd_mgl
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tss_rpd_mgl is null then 0 else t1.tss_rpd_mgl end) end					as tss_rpd_mgl

,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tn_shore_lbkm is null then 0 else t1.tn_shore_lbkm end) end		 			as tn_shore_lbkm
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tp_shore_lbkm is null then 0 else t1.tp_shore_lbkm end) end		 			as tp_shore_lbkm
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tss_shore_lbkm is null then 0 else t1.tss_shore_lbkm end) end		 		as tss_shore_lbkm
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tn_shorelbyr is null then 0 else t1.tn_shorelbyr end) end		 			as tn_shorelbyr
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tp_shorelbyr is null then 0 else t1.tp_shorelbyr end) end		 			as tp_shorelbyr
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tss_shorelbyr is null then 0 else t1.tss_shorelbyr end) end			 		as tss_shorelbyr
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tn_shore_mgl is null then 0 else t1.tn_shore_mgl end) end		 			as tn_shore_mgl
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tp_shore_mgl is null then 0 else t1.tp_shore_mgl end) end		 			as tp_shore_mgl
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tss_shore_mgl is null then 0 else t1.tss_shore_mgl end) end		 			as tss_shore_mgl

,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tn_streambnb_lbkm is null then 0 else t1.tn_streambnb_lbkm end) end		 		as tn_streambnb_lbkm
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tp_streambnb_lbkm is null then 0 else t1.tp_streambnb_lbkm end) end		 		as tp_streambnb_lbkm
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tss_streambnb_lbkm is null then 0 else t1.tss_streambnb_lbkm end) end		 	as tss_streambnb_lbkm
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tn_streambnblbyr is null then 0 else t1.tn_streambnblbyr end) end		 		as tn_streambnblbyr
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tp_streambnblbyr is null then 0 else t1.tp_streambnblbyr end) end		 		as tp_streambnblbyr
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tss_streambnblbyr is null then 0 else t1.tss_streambnblbyr end) end		 		as tss_streambnblbyr
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tn_streambnb_mgl is null then 0 else t1.tn_streambnb_mgl end) end		 		as tn_streambnb_mgl
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tp_streambnb_mgl is null then 0 else t1.tp_streambnb_mgl end) end		 		as tp_streambnb_mgl
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tss_streambnb_mgl is null then 0 else t1.tss_streambnb_mgl end) end		 		as tss_streambnb_mgl

,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tn_streamfp_lbkm is null then 0 else t1.tn_streamfp_lbkm end) end		 		as tn_streamfp_lbkm
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tp_streamfp_lbkm is null then 0 else t1.tp_streamfp_lbkm end) end				as tp_streamfp_lbkm
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tss_streamfp_lbkm is null then 0 else t1.tss_streamfp_lbkm end) end		 		as tss_streamfp_lbkm
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tn_streamfplbyr is null then 0 else t1.tn_streamfplbyr end) end		 		as tn_streamfplbyr
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tp_streamfplbyr is null then 0 else t1.tp_streamfplbyr end) end				as tp_streamfplbyr
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tss_streamfplbyr is null then 0 else t1.tss_streamfplbyr end) end		 		as tss_streamfplbyr
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tn_streamfp_mgl is null then 0 else t1.tn_streamfp_mgl end) end		 		as tn_streamfp_mgl
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tp_streamfp_mgl is null then 0 else t1.tp_streamfp_mgl end) end		 		as tp_streamfp_mgl
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tss_streamfp_mgl is null then 0 else t1.tss_streamfp_mgl end) end		 		as tss_streamfp_mgl

,	sum(case when t1.tn_cso_areaonly_lbacre is null then 0 else t1.tn_cso_areaonly_lbacre end)		 						as tn_cso_areaonly_lbacre
,	sum(case when t1.tp_cso_areaonly_lbacre is null then 0 else t1.tp_cso_areaonly_lbacre end)		 						as tp_cso_areaonly_lbacre
,	sum(case when t1.tss_cso_areaonly_lbacre is null then 0 else t1.tss_cso_areaonly_lbacre end)		 						as tss_cso_areaonly_lbacre
,	sum(case when t1.tn_cso_areaonly_lbyr is null then 0 else t1.tn_cso_areaonly_lbyr end)		 							as tn_cso_areaonly_lbyr
,	sum(case when t1.tp_cso_areaonly_lbyr is null then 0 else t1.tp_cso_areaonly_lbyr end)		 							as tp_cso_areaonly_lbyr
,	sum(case when t1.tss_cso_areaonly_lbyr is null then 0 else t1.tss_cso_areaonly_lbyr end)		 						as tss_cso_areaonly_lbyr
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tn_cso_areaonly_mgl is null then 0 else t1.tn_cso_areaonly_mgl end) end			as tn_cso_areaonly_mgl
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tp_cso_areaonly_mgl is null then 0 else t1.tp_cso_areaonly_mgl end) end			as tp_cso_areaonly_mgl
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tss_cso_areaonly_mgl is null then 0 else t1.tss_cso_areaonly_mgl end) end			as tss_cso_areaonly_mgl

,	sum(case when t1.tn_wwi_lbacre is null then 0 else t1.tn_wwi_lbacre end)		 								as tn_wwi_lbacre
,	sum(case when t1.tp_wwi_lbacre is null then 0 else t1.tp_wwi_lbacre end)		 								as tp_wwi_lbacre
,	sum(case when t1.tss_wwi_lbacre is null then 0 else t1.tss_wwi_lbacre end)		 								as tss_wwi_lbacre
,	sum(case when t1.tn_wwi_lbyr is null then 0 else t1.tn_wwi_lbyr end)		 									as tn_wwi_lbyr
,	sum(case when t1.tp_wwi_lbyr is null then 0 else t1.tp_wwi_lbyr end)		 									as tp_wwi_lbyr
,	sum(case when t1.tss_wwi_lbyr is null then 0 else t1.tss_wwi_lbyr end)		 									as tss_wwi_lbyr
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tn_wwi_mgl is null then 0 else t1.tn_wwi_mgl end) end		 			as tn_wwi_mgl
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tp_wwi_mgl is null then 0 else t1.tp_wwi_mgl end) end		 			as tp_wwi_mgl
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tss_wwi_mgl is null then 0 else t1.tss_wwi_mgl end) end		 			as tss_wwi_mgl

,	sum(case when t1.tn_wwm_lbacre is null then 0 else t1.tn_wwm_lbacre end)		 								as tn_wwm_lbacre
,	sum(case when t1.tp_wwm_lbacre is null then 0 else t1.tp_wwm_lbacre end)		 								as tp_wwm_lbacre
,	sum(case when t1.tss_wwm_lbacre is null then 0 else t1.tss_wwm_lbacre end)		 								as tss_wwm_lbacre
,	sum(case when t1.tn_wwm_lbyr is null then 0 else t1.tn_wwm_lbyr end)		 									as tn_wwm_lbyr
,	sum(case when t1.tp_wwm_lbyr is null then 0 else t1.tp_wwm_lbyr end)		 									as tp_wwm_lbyr
,	sum(case when t1.tss_wwm_lbyr is null then 0 else t1.tss_wwm_lbyr end)		 									as tss_wwm_lbyr
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tn_wwm_mgl is null then 0 else t1.tn_wwm_mgl end) end		 			as tn_wwm_mgl
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tp_wwm_mgl is null then 0 else t1.tp_wwm_mgl end) end		 			as tp_wwm_mgl
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tss_wwm_mgl is null then 0 else t1.tss_wwm_mgl end) end		 			as tss_wwm_mgl

,	sum(case when t1.tn_rib_lbacre is null then 0 else t1.tn_rib_lbacre end)		 								as tn_rib_lbacre
,	sum(case when t1.tp_rib_lbacre is null then 0 else t1.tp_rib_lbacre end)		 								as tp_rib_lbacre
,	sum(case when t1.tn_rib_lbyr is null then 0 else t1.tn_rib_lbyr end)		 									as tn_rib_lbyr
,	sum(case when t1.tp_rib_lbyr is null then 0 else t1.tp_rib_lbyr end)		 									as tp_rib_lbyr
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tn_rib_mgl is null then 0 else t1.tn_rib_mgl end) end		 			as tn_rib_mgl
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tp_rib_mgl is null then 0 else t1.tp_rib_mgl end) end		 			as tp_rib_mgl

,	sum(case when t1.tn_sep_lbacre is null then 0 else t1.tn_sep_lbacre end)		 								as tn_sep_lbacre
,	sum(case when t1.tp_sep_lbacre is null then 0 else t1.tp_sep_lbacre end)		 								as tp_sep_lbacre
,	sum(case when t1.tn_sep_lbyr is null then 0 else t1.tn_sep_lbyr end)		 									as tn_sep_lbyr
,	sum(case when t1.tp_sep_lbyr is null then 0 else t1.tp_sep_lbyr end)		 									as tp_sep_lbyr
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tn_sep_mgl is null then 0 else t1.tn_sep_mgl end) end		 			as tn_sep_mgl
,	case when nhd.maflowv < 0 then -9999 else sum(case when t1.tp_sep_mgl is null then 0 else t1.tp_sep_mgl end) end		 			as tp_sep_mgl
	*/

,sum(	case when t1.tn_crp_lbacre is null then 0 else t1.tn_crp_lbyr end			
+	case when t1.tn_fore_lbyr is null then 0 else t1.tn_fore_lbyr end			
+	case when t1.tn_inr_lbyr is null then 0 else t1.tn_inr_lbyr end			
+	case when t1.tn_ir_lbyr is null then 0 else t1.tn_ir_lbyr end			
+	case when t1.tn_mo_lbyr is null then 0 else t1.tn_mo_lbyr end			
+	case when t1.tn_pas_lbyr is null then 0 else t1.tn_pas_lbyr end			
+	case when t1.tn_tci_lbyr is null then 0 else t1.tn_tci_lbyr end			
+	case when t1.tn_tct_lbyr is null then 0 else t1.tn_tct_lbyr end			
+	case when t1.tn_tg_lbyr is null then 0 else t1.tn_tg_lbyr end			
+	case when t1.tn_wat_lbyr is null then 0 else t1.tn_wat_lbyr end			
+	case when t1.tn_wlf_lbyr is null then 0 else t1.tn_wlf_lbyr end			
+	case when t1.tn_wlo_lbyr is null then 0 else t1.tn_wlo_lbyr end			
+	case when t1.tn_construction_lbyr is null then 0 else t1.tn_construction_lbyr end			
+	case when t1.tn_rpd_lbyr is null then 0 else t1.tn_rpd_lbyr end			
+	case when t1.tn_shore_lbyr is null then 0 else t1.tn_shore_lbyr end			
+	case when t1.tn_streambnb_lbyr is null then 0 else t1.tn_streambnb_lbyr end			
+	case when t1.tn_streamfp_lbyr is null then 0 else t1.tn_streamfp_lbyr end			
+	case when t1.tn_cso_areaonly_lbyr is null then 0 else t1.tn_cso_areaonly_lbyr end			
+	case when t1.tn_wwi_lbyr is null then 0 else t1.tn_wwi_lbyr end			
+	case when t1.tn_wwm_lbyr is null then 0 else t1.tn_wwm_lbyr end			
+	case when t1.tn_rib_lbyr is null then 0 else t1.tn_rib_lbyr end			
+	case when t1.tn_sep_lbyr is null then 0 else t1.tn_sep_lbyr end	)					as	tn_lbyr
				
				
,sum(	case when t1.tn_crp_lbyr is null then 0 else t1.tn_crp_lbyr end			
+	case when t1.tn_pas_lbyr is null then 0 else t1.tn_pas_lbyr end			
+	case when t1.tn_rpd_lbyr is null then 0 else t1.tn_rpd_lbyr end	)					as	tn_ag_lbyr
				
				
,sum(	case when t1.tn_inr_lbyr is null then 0 else t1.tn_inr_lbyr end			
+	case when t1.tn_ir_lbyr is null then 0 else t1.tn_ir_lbyr end			
+	case when t1.tn_construction_lbyr is null then 0 else t1.tn_construction_lbyr end			
+	case when t1.tn_tci_lbyr is null then 0 else t1.tn_tci_lbyr end			
+	case when t1.tn_tct_lbyr is null then 0 else t1.tn_tct_lbyr end			
+	case when t1.tn_tg_lbyr is null then 0 else t1.tn_tg_lbyr end	)					as	tn_urb_lbyr
				
				
,sum(	case when t1.tn_fore_lbyr is null then 0 else t1.tn_fore_lbyr end			
+	case when t1.tn_mo_lbyr is null then 0 else t1.tn_mo_lbyr end			
+	case when t1.tn_wat_lbyr is null then 0 else t1.tn_wat_lbyr end			
+	case when t1.tn_wlf_lbyr is null then 0 else t1.tn_wlf_lbyr end			
+	case when t1.tn_wlo_lbyr is null then 0 else t1.tn_wlo_lbyr end			
+	case when t1.tn_shore_lbyr is null then 0 else t1.tn_shore_lbyr end			
+	case when t1.tn_streambnb_lbyr is null then 0 else t1.tn_streambnb_lbyr end			
+	case when t1.tn_streamfp_lbyr is null then 0 else t1.tn_streamfp_lbyr end	)				as	tn_nat_lbyr
				
				
,sum(	case when t1.tn_cso_areaonly_lbyr is null then 0 else t1.tn_cso_areaonly_lbyr end			
+	case when t1.tn_wwi_lbyr is null then 0 else t1.tn_wwi_lbyr end			
+	case when t1.tn_wwm_lbyr is null then 0 else t1.tn_wwm_lbyr end	)					as	tn_waste_lbyr
				
				
,sum(	case when t1.tn_rib_lbyr is null then 0 else t1.tn_rib_lbyr end			
+	case when t1.tn_sep_lbyr is null then 0 else t1.tn_sep_lbyr end	)					as	tn_septic_lbyr
				
				
,sum(	case when t1.tp_crp_lbyr is null then 0 else t1.tp_crp_lbyr end			
+	case when t1.tp_fore_lbyr is null then 0 else t1.tp_fore_lbyr end			
+	case when t1.tp_inr_lbyr is null then 0 else t1.tp_inr_lbyr end			
+	case when t1.tp_ir_lbyr is null then 0 else t1.tp_ir_lbyr end			
+	case when t1.tp_mo_lbyr is null then 0 else t1.tp_mo_lbyr end			
+	case when t1.tp_pas_lbyr is null then 0 else t1.tp_pas_lbyr end			
+	case when t1.tp_tci_lbyr is null then 0 else t1.tp_tci_lbyr end			
+	case when t1.tp_tct_lbyr is null then 0 else t1.tp_tct_lbyr end			
+	case when t1.tp_tg_lbyr is null then 0 else t1.tp_tg_lbyr end			
+	case when t1.tp_wat_lbyr is null then 0 else t1.tp_wat_lbyr end			
+	case when t1.tp_wlf_lbyr is null then 0 else t1.tp_wlf_lbyr end			
+	case when t1.tp_wlo_lbyr is null then 0 else t1.tp_wlo_lbyr end			
+	case when t1.tp_construction_lbyr is null then 0 else t1.tp_construction_lbyr end			
+	case when t1.tp_rpd_lbyr is null then 0 else t1.tp_rpd_lbyr end			
+	case when t1.tp_shore_lbyr is null then 0 else t1.tp_shore_lbyr end			
+	case when t1.tp_streambnb_lbyr is null then 0 else t1.tp_streambnb_lbyr end			
+	case when t1.tp_streamfp_lbyr is null then 0 else t1.tp_streamfp_lbyr end			
+	case when t1.tp_cso_areaonly_lbyr is null then 0 else t1.tp_cso_areaonly_lbyr end			
+	case when t1.tp_wwi_lbyr is null then 0 else t1.tp_wwi_lbyr end			
+	case when t1.tp_wwm_lbyr is null then 0 else t1.tp_wwm_lbyr end			
+	case when t1.tp_rib_lbyr is null then 0 else t1.tp_rib_lbyr end			
+	case when t1.tp_sep_lbyr is null then 0 else t1.tp_sep_lbyr end	)					as	tp_lbyr
				
				
,sum(	case when t1.tp_crp_lbyr is null then 0 else t1.tp_crp_lbyr end			
+	case when t1.tp_pas_lbyr is null then 0 else t1.tp_pas_lbyr end			
+	case when t1.tp_rpd_lbyr is null then 0 else t1.tp_rpd_lbyr end	)					as	tp_ag_lbyr
				
				
,sum(	case when t1.tp_inr_lbyr is null then 0 else t1.tp_inr_lbyr end			
+	case when t1.tp_ir_lbyr is null then 0 else t1.tp_ir_lbyr end			
+	case when t1.tp_construction_lbyr is null then 0 else t1.tp_construction_lbyr end			
+	case when t1.tp_tci_lbyr is null then 0 else t1.tp_tci_lbyr end			
+	case when t1.tp_tct_lbyr is null then 0 else t1.tp_tct_lbyr end			
+	case when t1.tp_tg_lbyr is null then 0 else t1.tp_tg_lbyr end	)					as	tp_urb_lbyr
				
				
,sum(	case when t1.tp_fore_lbyr is null then 0 else t1.tp_fore_lbyr end			
+	case when t1.tp_mo_lbyr is null then 0 else t1.tp_mo_lbyr end			
+	case when t1.tp_wat_lbyr is null then 0 else t1.tp_wat_lbyr end			
+	case when t1.tp_wlf_lbyr is null then 0 else t1.tp_wlf_lbyr end			
+	case when t1.tp_wlo_lbyr is null then 0 else t1.tp_wlo_lbyr end			
+	case when t1.tp_shore_lbyr is null then 0 else t1.tp_shore_lbyr end			
+	case when t1.tp_streambnb_lbyr is null then 0 else t1.tp_streambnb_lbyr end			
+	case when t1.tp_streamfp_lbyr is null then 0 else t1.tp_streamfp_lbyr end	)				as	tp_nat_lbyr
				
				
,sum(	case when t1.tp_cso_areaonly_lbyr is null then 0 else t1.tp_cso_areaonly_lbyr end			
+	case when t1.tp_wwi_lbyr is null then 0 else t1.tp_wwi_lbyr end			
+	case when t1.tp_wwm_lbyr is null then 0 else t1.tp_wwm_lbyr end	)					as	tp_waste_lbyr
				
				
,sum(	case when t1.tp_rib_lbyr is null then 0 else t1.tp_rib_lbyr end			
+	case when t1.tp_sep_lbyr is null then 0 else t1.tp_sep_lbyr end	)					as	tp_septic_lbyr
				
				
,sum(	case when t1.tss_crp_lbyr is null then 0 else t1.tss_crp_lbyr end			
+	case when t1.tss_fore_lbyr is null then 0 else t1.tss_fore_lbyr end			
+	case when t1.tss_inr_lbyr is null then 0 else t1.tss_inr_lbyr end			
+	case when t1.tss_ir_lbyr is null then 0 else t1.tss_ir_lbyr end			
+	case when t1.tss_mo_lbyr is null then 0 else t1.tss_mo_lbyr end			
+	case when t1.tss_pas_lbyr is null then 0 else t1.tss_pas_lbyr end			
+	case when t1.tss_tci_lbyr is null then 0 else t1.tss_tci_lbyr end			
+	case when t1.tss_tct_lbyr is null then 0 else t1.tss_tct_lbyr end			
+	case when t1.tss_tg_lbyr is null then 0 else t1.tss_tg_lbyr end			
+	case when t1.tss_wat_lbyr is null then 0 else t1.tss_wat_lbyr end			
+	case when t1.tss_wlf_lbyr is null then 0 else t1.tss_wlf_lbyr end			
+	case when t1.tss_wlo_lbyr is null then 0 else t1.tss_wlo_lbyr end			
+	case when t1.tss_construction_lbyr is null then 0 else t1.tss_construction_lbyr end			
+	case when t1.tss_rpd_lbyr is null then 0 else t1.tss_rpd_lbyr end			
+	case when t1.tss_shore_lbyr is null then 0 else t1.tss_shore_lbyr end			
+	case when t1.tss_streambnb_lbyr is null then 0 else t1.tss_streambnb_lbyr end			
+	case when t1.tss_streamfp_lbyr is null then 0 else t1.tss_streamfp_lbyr end			
+	case when t1.tss_cso_areaonly_lbyr is null then 0 else t1.tss_cso_areaonly_lbyr end			
+	case when t1.tss_wwi_lbyr is null then 0 else t1.tss_wwi_lbyr end			
+	case when t1.tss_wwm_lbyr is null then 0 else t1.tss_wwm_lbyr end	)					as	tss_lbyr
				
				
,sum(	case when t1.tss_crp_lbyr is null then 0 else t1.tss_crp_lbyr end			
+	case when t1.tss_pas_lbyr is null then 0 else t1.tss_pas_lbyr end			
+	case when t1.tss_rpd_lbyr is null then 0 else t1.tss_rpd_lbyr end	)					as	tss_ag_lbyr
				
				
,sum(	case when t1.tss_inr_lbyr is null then 0 else t1.tss_inr_lbyr end			
+	case when t1.tss_ir_lbyr is null then 0 else t1.tss_ir_lbyr end			
+	case when t1.tss_construction_lbyr is null then 0 else t1.tss_construction_lbyr end			
+	case when t1.tss_tci_lbyr is null then 0 else t1.tss_tci_lbyr end			
+	case when t1.tss_tct_lbyr is null then 0 else t1.tss_tct_lbyr end			
+	case when t1.tss_tg_lbyr is null then 0 else t1.tss_tg_lbyr end	)					as	tss_urb_lbyr
				
				
,sum(	case when t1.tss_fore_lbyr is null then 0 else t1.tss_fore_lbyr end			
+	case when t1.tss_mo_lbyr is null then 0 else t1.tss_mo_lbyr end			
+	case when t1.tss_wat_lbyr is null then 0 else t1.tss_wat_lbyr end			
+	case when t1.tss_wlf_lbyr is null then 0 else t1.tss_wlf_lbyr end			
+	case when t1.tss_wlo_lbyr is null then 0 else t1.tss_wlo_lbyr end			
+	case when t1.tss_shore_lbyr is null then 0 else t1.tss_shore_lbyr end			
+	case when t1.tss_streambnb_lbyr is null then 0 else t1.tss_streambnb_lbyr end			
+	case when t1.tss_streamfp_lbyr is null then 0 else t1.tss_streamfp_lbyr end	)				as	tss_nat_lbyr
				
				
,sum(	case when t1.tss_cso_areaonly_lbyr is null then 0 else t1.tss_cso_areaonly_lbyr end			
+	case when t1.tss_wwi_lbyr is null then 0 else t1.tss_wwi_lbyr end			
+	case when t1.tss_wwm_lbyr is null then 0 else t1.tss_wwm_lbyr end	)					as	tss_waste_lbyr
				

,sum(	case when t1.tn_crp_lbacre is null then 0 else t1.tn_crp_lbacre end			
+	case when t1.tn_fore_lbacre is null then 0 else t1.tn_fore_lbacre end			
+	case when t1.tn_inr_lbacre is null then 0 else t1.tn_inr_lbacre end			
+	case when t1.tn_ir_lbacre is null then 0 else t1.tn_ir_lbacre end			
+	case when t1.tn_mo_lbacre is null then 0 else t1.tn_mo_lbacre end			
+	case when t1.tn_pas_lbacre is null then 0 else t1.tn_pas_lbacre end			
+	case when t1.tn_tci_lbacre is null then 0 else t1.tn_tci_lbacre end			
+	case when t1.tn_tct_lbacre is null then 0 else t1.tn_tct_lbacre end			
+	case when t1.tn_tg_lbacre is null then 0 else t1.tn_tg_lbacre end			
+	case when t1.tn_wat_lbacre is null then 0 else t1.tn_wat_lbacre end			
+	case when t1.tn_wlf_lbacre is null then 0 else t1.tn_wlf_lbacre end			
+	case when t1.tn_wlo_lbacre is null then 0 else t1.tn_wlo_lbacre end			
+	case when t1.tn_construction_lbacre is null then 0 else t1.tn_construction_lbacre end			
										)					as	tn_lbacre

,sum(	case when t1.tn_crp_lbyr is null then 0 else t1.tn_crp_lbyr end			
+	case when t1.tn_fore_lbyr is null then 0 else t1.tn_fore_lbyr end			
+	case when t1.tn_inr_lbyr is null then 0 else t1.tn_inr_lbyr end			
+	case when t1.tn_ir_lbyr is null then 0 else t1.tn_ir_lbyr end			
+	case when t1.tn_mo_lbyr is null then 0 else t1.tn_mo_lbyr end			
+	case when t1.tn_pas_lbyr is null then 0 else t1.tn_pas_lbyr end			
+	case when t1.tn_tci_lbyr is null then 0 else t1.tn_tci_lbyr end			
+	case when t1.tn_tct_lbyr is null then 0 else t1.tn_tct_lbyr end			
+	case when t1.tn_tg_lbyr is null then 0 else t1.tn_tg_lbyr end			
+	case when t1.tn_wat_lbyr is null then 0 else t1.tn_wat_lbyr end			
+	case when t1.tn_wlf_lbyr is null then 0 else t1.tn_wlf_lbyr end			
+	case when t1.tn_wlo_lbyr is null then 0 else t1.tn_wlo_lbyr end			
+	case when t1.tn_construction_lbyr is null then 0 else t1.tn_construction_lbyr end			
										)					as	tn_lbyr_surfaceload			
				
,sum(	case when t1.tn_crp_lbacre is null then 0 else t1.tn_crp_lbacre end			
+	case when t1.tn_pas_lbacre is null then 0 else t1.tn_pas_lbacre end			
										)					as	tn_ag_lbacre
				
				
,sum(	case when t1.tn_inr_lbacre is null then 0 else t1.tn_inr_lbacre end			
+	case when t1.tn_ir_lbacre is null then 0 else t1.tn_ir_lbacre end			
+	case when t1.tn_construction_lbacre is null then 0 else t1.tn_construction_lbacre end			
+	case when t1.tn_tci_lbacre is null then 0 else t1.tn_tci_lbacre end			
+	case when t1.tn_tct_lbacre is null then 0 else t1.tn_tct_lbacre end			
+	case when t1.tn_tg_lbacre is null then 0 else t1.tn_tg_lbacre end	)					as	tn_urb_lbacre
				
				
,sum(	case when t1.tn_fore_lbacre is null then 0 else t1.tn_fore_lbacre end			
+	case when t1.tn_mo_lbacre is null then 0 else t1.tn_mo_lbacre end			
+	case when t1.tn_wat_lbacre is null then 0 else t1.tn_wat_lbacre end			
+	case when t1.tn_wlf_lbacre is null then 0 else t1.tn_wlf_lbacre end			
+	case when t1.tn_wlo_lbacre is null then 0 else t1.tn_wlo_lbacre end			
											)				as	tn_nat_lbacre
							
,sum(	case when t1.tp_crp_lbacre is null then 0 else t1.tp_crp_lbacre end			
+	case when t1.tp_fore_lbacre is null then 0 else t1.tp_fore_lbacre end			
+	case when t1.tp_inr_lbacre is null then 0 else t1.tp_inr_lbacre end			
+	case when t1.tp_ir_lbacre is null then 0 else t1.tp_ir_lbacre end			
+	case when t1.tp_mo_lbacre is null then 0 else t1.tp_mo_lbacre end			
+	case when t1.tp_pas_lbacre is null then 0 else t1.tp_pas_lbacre end			
+	case when t1.tp_tci_lbacre is null then 0 else t1.tp_tci_lbacre end			
+	case when t1.tp_tct_lbacre is null then 0 else t1.tp_tct_lbacre end			
+	case when t1.tp_tg_lbacre is null then 0 else t1.tp_tg_lbacre end			
+	case when t1.tp_wat_lbacre is null then 0 else t1.tp_wat_lbacre end			
+	case when t1.tp_wlf_lbacre is null then 0 else t1.tp_wlf_lbacre end			
+	case when t1.tp_wlo_lbacre is null then 0 else t1.tp_wlo_lbacre end			
+	case when t1.tp_construction_lbacre is null then 0 else t1.tp_construction_lbacre end			
										)					as	tp_lbacre
				
,sum(	case when t1.tp_crp_lbyr is null then 0 else t1.tp_crp_lbyr end			
+	case when t1.tp_fore_lbyr is null then 0 else t1.tp_fore_lbyr end			
+	case when t1.tp_inr_lbyr is null then 0 else t1.tp_inr_lbyr end			
+	case when t1.tp_ir_lbyr is null then 0 else t1.tp_ir_lbyr end			
+	case when t1.tp_mo_lbyr is null then 0 else t1.tp_mo_lbyr end			
+	case when t1.tp_pas_lbyr is null then 0 else t1.tp_pas_lbyr end			
+	case when t1.tp_tci_lbyr is null then 0 else t1.tp_tci_lbyr end			
+	case when t1.tp_tct_lbyr is null then 0 else t1.tp_tct_lbyr end			
+	case when t1.tp_tg_lbyr is null then 0 else t1.tp_tg_lbyr end			
+	case when t1.tp_wat_lbyr is null then 0 else t1.tp_wat_lbyr end			
+	case when t1.tp_wlf_lbyr is null then 0 else t1.tp_wlf_lbyr end			
+	case when t1.tp_wlo_lbyr is null then 0 else t1.tp_wlo_lbyr end			
+	case when t1.tp_construction_lbyr is null then 0 else t1.tp_construction_lbyr end			
										)					as	tp_lbyr_surfaceload	
		
,sum(	case when t1.tp_crp_lbacre is null then 0 else t1.tp_crp_lbacre end			
+	case when t1.tp_pas_lbacre is null then 0 else t1.tp_pas_lbacre end			
										)					as	tp_ag_lbacre
				
				
,sum(	case when t1.tp_inr_lbacre is null then 0 else t1.tp_inr_lbacre end			
+	case when t1.tp_ir_lbacre is null then 0 else t1.tp_ir_lbacre end			
+	case when t1.tp_construction_lbacre is null then 0 else t1.tp_construction_lbacre end			
+	case when t1.tp_tci_lbacre is null then 0 else t1.tp_tci_lbacre end			
+	case when t1.tp_tct_lbacre is null then 0 else t1.tp_tct_lbacre end			
+	case when t1.tp_tg_lbacre is null then 0 else t1.tp_tg_lbacre end	)					as	tp_urb_lbacre
				
				
,sum(	case when t1.tp_fore_lbacre is null then 0 else t1.tp_fore_lbacre end			
+	case when t1.tp_mo_lbacre is null then 0 else t1.tp_mo_lbacre end			
+	case when t1.tp_wat_lbacre is null then 0 else t1.tp_wat_lbacre end			
+	case when t1.tp_wlf_lbacre is null then 0 else t1.tp_wlf_lbacre end			
+	case when t1.tp_wlo_lbacre is null then 0 else t1.tp_wlo_lbacre end			
											)				as	tp_nat_lbacre
				
,sum(	case when t1.tss_crp_lbacre is null then 0 else t1.tss_crp_lbacre end			
+	case when t1.tss_fore_lbacre is null then 0 else t1.tss_fore_lbacre end			
+	case when t1.tss_inr_lbacre is null then 0 else t1.tss_inr_lbacre end			
+	case when t1.tss_ir_lbacre is null then 0 else t1.tss_ir_lbacre end			
+	case when t1.tss_mo_lbacre is null then 0 else t1.tss_mo_lbacre end			
+	case when t1.tss_pas_lbacre is null then 0 else t1.tss_pas_lbacre end			
+	case when t1.tss_tci_lbacre is null then 0 else t1.tss_tci_lbacre end			
+	case when t1.tss_tct_lbacre is null then 0 else t1.tss_tct_lbacre end			
+	case when t1.tss_tg_lbacre is null then 0 else t1.tss_tg_lbacre end			
+	case when t1.tss_wat_lbacre is null then 0 else t1.tss_wat_lbacre end			
+	case when t1.tss_wlf_lbacre is null then 0 else t1.tss_wlf_lbacre end			
+	case when t1.tss_wlo_lbacre is null then 0 else t1.tss_wlo_lbacre end			
+	case when t1.tss_construction_lbacre is null then 0 else t1.tss_construction_lbacre end			
										)					as	tss_lbacre

,sum(	case when t1.tss_crp_lbyr is null then 0 else t1.tss_crp_lbyr end			
+	case when t1.tss_fore_lbyr is null then 0 else t1.tss_fore_lbyr end			
+	case when t1.tss_inr_lbyr is null then 0 else t1.tss_inr_lbyr end			
+	case when t1.tss_ir_lbyr is null then 0 else t1.tss_ir_lbyr end			
+	case when t1.tss_mo_lbyr is null then 0 else t1.tss_mo_lbyr end			
+	case when t1.tss_pas_lbyr is null then 0 else t1.tss_pas_lbyr end			
+	case when t1.tss_tci_lbyr is null then 0 else t1.tss_tci_lbyr end			
+	case when t1.tss_tct_lbyr is null then 0 else t1.tss_tct_lbyr end			
+	case when t1.tss_tg_lbyr is null then 0 else t1.tss_tg_lbyr end			
+	case when t1.tss_wat_lbyr is null then 0 else t1.tss_wat_lbyr end			
+	case when t1.tss_wlf_lbyr is null then 0 else t1.tss_wlf_lbyr end			
+	case when t1.tss_wlo_lbyr is null then 0 else t1.tss_wlo_lbyr end			
+	case when t1.tss_construction_lbyr is null then 0 else t1.tss_construction_lbyr end			
										)					as	tss_lbyr_surfaceload				
				
,sum(	case when t1.tss_crp_lbacre is null then 0 else t1.tss_crp_lbacre end			
+	case when t1.tss_pas_lbacre is null then 0 else t1.tss_pas_lbacre end			
										)					as	tss_ag_lbacre
				
				
,sum(	case when t1.tss_inr_lbacre is null then 0 else t1.tss_inr_lbacre end			
+	case when t1.tss_ir_lbacre is null then 0 else t1.tss_ir_lbacre end			
+	case when t1.tss_construction_lbacre is null then 0 else t1.tss_construction_lbacre end			
+	case when t1.tss_tci_lbacre is null then 0 else t1.tss_tci_lbacre end			
+	case when t1.tss_tct_lbacre is null then 0 else t1.tss_tct_lbacre end			
+	case when t1.tss_tg_lbacre is null then 0 else t1.tss_tg_lbacre end	)					as	tss_urb_lbacre
				
				
,sum(	case when t1.tss_fore_lbacre is null then 0 else t1.tss_fore_lbacre end			
+	case when t1.tss_mo_lbacre is null then 0 else t1.tss_mo_lbacre end			
+	case when t1.tss_wat_lbacre is null then 0 else t1.tss_wat_lbacre end			
+	case when t1.tss_wlf_lbacre is null then 0 else t1.tss_wlf_lbacre end			
+	case when t1.tss_wlo_lbacre is null then 0 else t1.tss_wlo_lbacre end			
											)				as	tss_nat_lbacre

,case when nhd.maflowv < 0 then -9999 else sum(	case when t1.tn_crp_mgl is null then 0 else t1.tn_crp_mgl end			
+	case when t1.tn_fore_mgl is null then 0 else t1.tn_fore_mgl end			
+	case when t1.tn_inr_mgl is null then 0 else t1.tn_inr_mgl end			
+	case when t1.tn_ir_mgl is null then 0 else t1.tn_ir_mgl end			
+	case when t1.tn_mo_mgl is null then 0 else t1.tn_mo_mgl end			
+	case when t1.tn_pas_mgl is null then 0 else t1.tn_pas_mgl end			
+	case when t1.tn_tci_mgl is null then 0 else t1.tn_tci_mgl end			
+	case when t1.tn_tct_mgl is null then 0 else t1.tn_tct_mgl end			
+	case when t1.tn_tg_mgl is null then 0 else t1.tn_tg_mgl end			
+	case when t1.tn_wat_mgl is null then 0 else t1.tn_wat_mgl end			
+	case when t1.tn_wlf_mgl is null then 0 else t1.tn_wlf_mgl end			
+	case when t1.tn_wlo_mgl is null then 0 else t1.tn_wlo_mgl end			
+	case when t1.tn_construction_mgl is null then 0 else t1.tn_construction_mgl end			
+	case when t1.tn_rpd_mgl is null then 0 else t1.tn_rpd_mgl end			
+	case when t1.tn_shore_mgl is null then 0 else t1.tn_shore_mgl end			
+	case when t1.tn_streambnb_mgl is null then 0 else t1.tn_streambnb_mgl end			
+	case when t1.tn_streamfp_mgl is null then 0 else t1.tn_streamfp_mgl end			
+	case when t1.tn_cso_areaonly_mgl is null then 0 else t1.tn_cso_areaonly_mgl end			
+	case when t1.tn_wwi_mgl is null then 0 else t1.tn_wwi_mgl end			
+	case when t1.tn_wwm_mgl is null then 0 else t1.tn_wwm_mgl end			
+	case when t1.tn_rib_mgl is null then 0 else t1.tn_rib_mgl end			
+	case when t1.tn_sep_mgl is null then 0 else t1.tn_sep_mgl end	) end						as	tn_mgl
				
				
,case when nhd.maflowv < 0 then -9999 else sum(	case when t1.tn_crp_mgl is null then 0 else t1.tn_crp_mgl end			
+	case when t1.tn_pas_mgl is null then 0 else t1.tn_pas_mgl end			
+	case when t1.tn_rpd_mgl is null then 0 else t1.tn_rpd_mgl end	) end						as	tn_ag_mgl
				
				
,case when nhd.maflowv < 0 then -9999 else sum(	case when t1.tn_inr_mgl is null then 0 else t1.tn_inr_mgl end			
+	case when t1.tn_ir_mgl is null then 0 else t1.tn_ir_mgl end			
+	case when t1.tn_construction_mgl is null then 0 else t1.tn_construction_mgl end			
+	case when t1.tn_tci_mgl is null then 0 else t1.tn_tci_mgl end			
+	case when t1.tn_tct_mgl is null then 0 else t1.tn_tct_mgl end			
+	case when t1.tn_tg_mgl is null then 0 else t1.tn_tg_mgl end	) end						as	tn_urb_mgl
				
				
,case when nhd.maflowv < 0 then -9999 else sum(	case when t1.tn_fore_mgl is null then 0 else t1.tn_fore_mgl end			
+	case when t1.tn_mo_mgl is null then 0 else t1.tn_mo_mgl end			
+	case when t1.tn_wat_mgl is null then 0 else t1.tn_wat_mgl end			
+	case when t1.tn_wlf_mgl is null then 0 else t1.tn_wlf_mgl end			
+	case when t1.tn_wlo_mgl is null then 0 else t1.tn_wlo_mgl end			
+	case when t1.tn_shore_mgl is null then 0 else t1.tn_shore_mgl end			
+	case when t1.tn_streambnb_mgl is null then 0 else t1.tn_streambnb_mgl end			
+	case when t1.tn_streamfp_mgl is null then 0 else t1.tn_streamfp_mgl end	) end				as	tn_nat_mgl
				
				
,case when nhd.maflowv < 0 then -9999 else sum(	case when t1.tn_cso_areaonly_mgl is null then 0 else t1.tn_cso_areaonly_mgl end			
+	case when t1.tn_wwi_mgl is null then 0 else t1.tn_wwi_mgl end			
+	case when t1.tn_wwm_mgl is null then 0 else t1.tn_wwm_mgl end	) end						as	tn_waste_mgl
				
				
,case when nhd.maflowv < 0 then -9999 else sum(	case when t1.tn_rib_mgl is null then 0 else t1.tn_rib_mgl end			
+	case when t1.tn_sep_mgl is null then 0 else t1.tn_sep_mgl end	) end						as	tn_septic_mgl
				
				
,case when nhd.maflowv < 0 then -9999 else sum(	case when t1.tp_crp_mgl is null then 0 else t1.tp_crp_mgl end			
+	case when t1.tp_fore_mgl is null then 0 else t1.tp_fore_mgl end			
+	case when t1.tp_inr_mgl is null then 0 else t1.tp_inr_mgl end			
+	case when t1.tp_ir_mgl is null then 0 else t1.tp_ir_mgl end			
+	case when t1.tp_mo_mgl is null then 0 else t1.tp_mo_mgl end			
+	case when t1.tp_pas_mgl is null then 0 else t1.tp_pas_mgl end			
+	case when t1.tp_tci_mgl is null then 0 else t1.tp_tci_mgl end			
+	case when t1.tp_tct_mgl is null then 0 else t1.tp_tct_mgl end			
+	case when t1.tp_tg_mgl is null then 0 else t1.tp_tg_mgl end			
+	case when t1.tp_wat_mgl is null then 0 else t1.tp_wat_mgl end			
+	case when t1.tp_wlf_mgl is null then 0 else t1.tp_wlf_mgl end			
+	case when t1.tp_wlo_mgl is null then 0 else t1.tp_wlo_mgl end			
+	case when t1.tp_construction_mgl is null then 0 else t1.tp_construction_mgl end			
+	case when t1.tp_rpd_mgl is null then 0 else t1.tp_rpd_mgl end			
+	case when t1.tp_shore_mgl is null then 0 else t1.tp_shore_mgl end			
+	case when t1.tp_streambnb_mgl is null then 0 else t1.tp_streambnb_mgl end			
+	case when t1.tp_streamfp_mgl is null then 0 else t1.tp_streamfp_mgl end			
+	case when t1.tp_cso_areaonly_mgl is null then 0 else t1.tp_cso_areaonly_mgl end			
+	case when t1.tp_wwi_mgl is null then 0 else t1.tp_wwi_mgl end			
+	case when t1.tp_wwm_mgl is null then 0 else t1.tp_wwm_mgl end			
+	case when t1.tp_rib_mgl is null then 0 else t1.tp_rib_mgl end			
+	case when t1.tp_sep_mgl is null then 0 else t1.tp_sep_mgl end	) end						as	tp_mgl
				
				
,case when nhd.maflowv < 0 then -9999 else sum(	case when t1.tp_crp_mgl is null then 0 else t1.tp_crp_mgl end			
+	case when t1.tp_pas_mgl is null then 0 else t1.tp_pas_mgl end			
+	case when t1.tp_rpd_mgl is null then 0 else t1.tp_rpd_mgl end	) end						as	tp_ag_mgl
				
				
,case when nhd.maflowv < 0 then -9999 else sum(	case when t1.tp_inr_mgl is null then 0 else t1.tp_inr_mgl end			
+	case when t1.tp_ir_mgl is null then 0 else t1.tp_ir_mgl end			
+	case when t1.tp_construction_mgl is null then 0 else t1.tp_construction_mgl end			
+	case when t1.tp_tci_mgl is null then 0 else t1.tp_tci_mgl end			
+	case when t1.tp_tct_mgl is null then 0 else t1.tp_tct_mgl end			
+	case when t1.tp_tg_mgl is null then 0 else t1.tp_tg_mgl end	) end						as	tp_urb_mgl
				
				
,case when nhd.maflowv < 0 then -9999 else sum(	case when t1.tp_fore_mgl is null then 0 else t1.tp_fore_mgl end			
+	case when t1.tp_mo_mgl is null then 0 else t1.tp_mo_mgl end			
+	case when t1.tp_wat_mgl is null then 0 else t1.tp_wat_mgl end			
+	case when t1.tp_wlf_mgl is null then 0 else t1.tp_wlf_mgl end			
+	case when t1.tp_wlo_mgl is null then 0 else t1.tp_wlo_mgl end			
+	case when t1.tp_shore_mgl is null then 0 else t1.tp_shore_mgl end			
+	case when t1.tp_streambnb_mgl is null then 0 else t1.tp_streambnb_mgl end			
+	case when t1.tp_streamfp_mgl is null then 0 else t1.tp_streamfp_mgl end	) end				as	tp_nat_mgl
				
				
,case when nhd.maflowv < 0 then -9999 else sum(	case when t1.tp_cso_areaonly_mgl is null then 0 else t1.tp_cso_areaonly_mgl end			
+	case when t1.tp_wwi_mgl is null then 0 else t1.tp_wwi_mgl end			
+	case when t1.tp_wwm_mgl is null then 0 else t1.tp_wwm_mgl end	) end						as	tp_waste_mgl
				
				
,case when nhd.maflowv < 0 then -9999 else sum(	case when t1.tp_rib_mgl is null then 0 else t1.tp_rib_mgl end			
+	case when t1.tp_sep_mgl is null then 0 else t1.tp_sep_mgl end	) end						as	tp_septic_mgl
				
				
,case when nhd.maflowv < 0 then -9999 else sum(	case when t1.tss_crp_mgl is null then 0 else t1.tss_crp_mgl end			
+	case when t1.tss_fore_mgl is null then 0 else t1.tss_fore_mgl end			
+	case when t1.tss_inr_mgl is null then 0 else t1.tss_inr_mgl end			
+	case when t1.tss_ir_mgl is null then 0 else t1.tss_ir_mgl end			
+	case when t1.tss_mo_mgl is null then 0 else t1.tss_mo_mgl end			
+	case when t1.tss_pas_mgl is null then 0 else t1.tss_pas_mgl end			
+	case when t1.tss_tci_mgl is null then 0 else t1.tss_tci_mgl end			
+	case when t1.tss_tct_mgl is null then 0 else t1.tss_tct_mgl end			
+	case when t1.tss_tg_mgl is null then 0 else t1.tss_tg_mgl end			
+	case when t1.tss_wat_mgl is null then 0 else t1.tss_wat_mgl end			
+	case when t1.tss_wlf_mgl is null then 0 else t1.tss_wlf_mgl end			
+	case when t1.tss_wlo_mgl is null then 0 else t1.tss_wlo_mgl end			
+	case when t1.tss_construction_mgl is null then 0 else t1.tss_construction_mgl end			
+	case when t1.tss_rpd_mgl is null then 0 else t1.tss_rpd_mgl end			
+	case when t1.tss_shore_mgl is null then 0 else t1.tss_shore_mgl end			
+	case when t1.tss_streambnb_mgl is null then 0 else t1.tss_streambnb_mgl end			
+	case when t1.tss_streamfp_mgl is null then 0 else t1.tss_streamfp_mgl end			
+	case when t1.tss_cso_areaonly_mgl is null then 0 else t1.tss_cso_areaonly_mgl end			
+	case when t1.tss_wwi_mgl is null then 0 else t1.tss_wwi_mgl end			
+	case when t1.tss_wwm_mgl is null then 0 else t1.tss_wwm_mgl end	) end						as	tss_mgl
				
				
,case when nhd.maflowv < 0 then -9999 else sum(	case when t1.tss_crp_mgl is null then 0 else t1.tss_crp_mgl end			
+	case when t1.tss_pas_mgl is null then 0 else t1.tss_pas_mgl end			
+	case when t1.tss_rpd_mgl is null then 0 else t1.tss_rpd_mgl end	) end						as	tss_ag_mgl
				
				
,case when nhd.maflowv < 0 then -9999 else sum(	case when t1.tss_inr_mgl is null then 0 else t1.tss_inr_mgl end			
+	case when t1.tss_ir_mgl is null then 0 else t1.tss_ir_mgl end			
+	case when t1.tss_construction_mgl is null then 0 else t1.tss_construction_mgl end			
+	case when t1.tss_tci_mgl is null then 0 else t1.tss_tci_mgl end			
+	case when t1.tss_tct_mgl is null then 0 else t1.tss_tct_mgl end			
+	case when t1.tss_tg_mgl is null then 0 else t1.tss_tg_mgl end	) end						as	tss_urb_mgl
				
				
,case when nhd.maflowv < 0 then -9999 else sum(	case when t1.tss_fore_mgl is null then 0 else t1.tss_fore_mgl end			
+	case when t1.tss_mo_mgl is null then 0 else t1.tss_mo_mgl end			
+	case when t1.tss_wat_mgl is null then 0 else t1.tss_wat_mgl end			
+	case when t1.tss_wlf_mgl is null then 0 else t1.tss_wlf_mgl end			
+	case when t1.tss_wlo_mgl is null then 0 else t1.tss_wlo_mgl end			
+	case when t1.tss_shore_mgl is null then 0 else t1.tss_shore_mgl end			
+	case when t1.tss_streambnb_mgl is null then 0 else t1.tss_streambnb_mgl end			
+	case when t1.tss_streamfp_mgl is null then 0 else t1.tss_streamfp_mgl end	) end				as	tss_nat_mgl
				
				
,case when nhd.maflowv < 0 then -9999 else sum(	case when t1.tss_cso_areaonly_mgl is null then 0 else t1.tss_cso_areaonly_mgl end			
+	case when t1.tss_wwi_mgl is null then 0 else t1.tss_wwi_mgl end			
+	case when t1.tss_wwm_mgl is null then 0 else t1.tss_wwm_mgl end	) end						as	tss_waste_mgl

,nhd.catchment


from
(
select distinct a.comid, a.huc12,a.nhd_areasqkm, a.intersect_areasqkm, 
       a.h12_tot_sqkm, a.h12crp_sqkm, a.h12fore_sqkm, a.h12inr_sqkm,a. h12ir_sqkm, 
      a.h12mo_sqkm, a.h12pas_sqkm, a.h12tci_sqkm, a.h12tct_sqkm, a.h12tg_sqkm, 
       a.h12wat_sqkm, a.h12wlf_sqkm, a.h12wlo_sqkm, a.h12wlt_sqkm, a.nhdcrp_sqkm, 
       a.nhdfore_sqkm, a.nhdinr_sqkm, a.nhdir_sqkm, a.nhdmo_sqkm, a.nhdpas_sqkm, 
       a.nhdtci_sqkm, a.nhdtct_sqkm, a.nhdtg_sqkm, a.nhdwat_sqkm, a.nhdwlf_sqkm, 
       a.nhdwlo_sqkm, a.nhdwlt_sqkm, a.nord, a.nordstop, a.nhd_lengthkm, a.intersect_lengthkm, 
       a.huc12_lengthkm

-- lulc associated load_sources

-- CRP

, case when h12crp_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdcrp_sqkm / h12crp_sqkm) * tn_crp) / (intersect_areasqkm * 247.105) 
	end  	as tn_crp_lbacre
, case when h12crp_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdcrp_sqkm / h12crp_sqkm) * tp_crp) / (intersect_areasqkm * 247.105) 
	end  	as tp_crp_lbacre
, case when h12crp_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdcrp_sqkm / h12crp_sqkm)  * tss_crp) / (intersect_areasqkm * 247.105) 
	end 	as tss_crp_lbacre

, case when h12crp_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdcrp_sqkm / h12crp_sqkm) * tn_crp)
	end  	as tn_crp_lbyr
, case when h12crp_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdcrp_sqkm / h12crp_sqkm) * tp_crp)
	end  	as tp_crp_lbyr
, case when h12crp_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdcrp_sqkm / h12crp_sqkm)  * tss_crp)
	end 	as tss_crp_lbyr

, case when h12crp_sqkm = 0 then 0 
	else ( tn_crp * (nhdcrp_sqkm / h12crp_sqkm)  * 453592 ) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	end as tn_crp_mgl
, case when h12crp_sqkm = 0 then 0 
	else ( tp_crp * (nhdcrp_sqkm / h12crp_sqkm)  * 453592 ) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	end as tp_crp_mgl
, case when h12crp_sqkm = 0 then 0 
	else ( tss_crp * (nhdcrp_sqkm / h12crp_sqkm) * 453592 ) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	end as tss_crp_mgl

-- FORE

, case when h12fore_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdfore_sqkm / h12fore_sqkm) * tn_fore) / (intersect_areasqkm * 247.105) 
	end  	as tn_fore_lbacre
, case when h12fore_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdfore_sqkm / h12fore_sqkm) * tp_fore) / (intersect_areasqkm * 247.105) 
	end  	as tp_fore_lbacre
, case when h12fore_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdfore_sqkm / h12fore_sqkm)  * tss_fore) / (intersect_areasqkm * 247.105) 
	end 	as tss_fore_lbacre

, case when h12fore_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdfore_sqkm / h12fore_sqkm) * tn_fore)
	end  	as tn_fore_lbyr
, case when h12fore_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdfore_sqkm / h12fore_sqkm) * tp_fore)
	end  	as tp_fore_lbyr
, case when h12fore_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdfore_sqkm / h12fore_sqkm)  * tss_fore)
	end 	as tss_fore_lbyr

, case when h12fore_sqkm = 0 then 0 
	else ( tn_fore * (nhdfore_sqkm / h12fore_sqkm)  * 453592 ) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	end as tn_fore_mgl
, case when h12fore_sqkm = 0 then 0 
	else ( tp_fore * (nhdfore_sqkm / h12fore_sqkm)  * 453592 ) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	end as tp_fore_mgl
, case when h12fore_sqkm = 0 then 0 
	else ( tss_fore * (nhdfore_sqkm / h12fore_sqkm) * 453592 ) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	end as tss_fore_mgl

-- INR

, case when h12inr_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdinr_sqkm / h12inr_sqkm) * tn_inr) / (intersect_areasqkm * 247.105) 
	end  	as tn_inr_lbacre
, case when h12inr_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdinr_sqkm / h12inr_sqkm) * tp_inr) / (intersect_areasqkm * 247.105) 
	end  	as tp_inr_lbacre
, case when h12inr_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdinr_sqkm / h12inr_sqkm)  * tss_inr) / (intersect_areasqkm * 247.105) 
	end 	as tss_inr_lbacre

, case when h12inr_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdinr_sqkm / h12inr_sqkm) * tn_inr)
	end  	as tn_inr_lbyr
, case when h12inr_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdinr_sqkm / h12inr_sqkm) * tp_inr)
	end  	as tp_inr_lbyr
, case when h12inr_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdinr_sqkm / h12inr_sqkm)  * tss_inr)
	end 	as tss_inr_lbyr

, case when h12inr_sqkm = 0 then 0 
	else ( tn_inr * (nhdinr_sqkm / h12inr_sqkm)  * 453592 ) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	end as tn_inr_mgl
, case when h12inr_sqkm = 0 then 0 
	else ( tp_inr * (nhdinr_sqkm / h12inr_sqkm)  * 453592 ) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	end as tp_inr_mgl
, case when h12inr_sqkm = 0 then 0 
	else ( tss_inr * (nhdinr_sqkm / h12inr_sqkm) * 453592 ) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	end as tss_inr_mgl

-- IR

, case when h12ir_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdir_sqkm / h12ir_sqkm) * tn_ir) / (intersect_areasqkm * 247.105) 
	end  	as tn_ir_lbacre
, case when h12ir_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdir_sqkm / h12ir_sqkm) * tp_ir) / (intersect_areasqkm * 247.105) 
	end  	as tp_ir_lbacre
, case when h12ir_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdir_sqkm / h12ir_sqkm)  * tss_ir) / (intersect_areasqkm * 247.105) 
	end 	as tss_ir_lbacre

, case when h12ir_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdir_sqkm / h12ir_sqkm) * tn_ir)
	end  	as tn_ir_lbyr
, case when h12ir_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdir_sqkm / h12ir_sqkm) * tp_ir)
	end  	as tp_ir_lbyr
, case when h12ir_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdir_sqkm / h12ir_sqkm)  * tss_ir)
	end 	as tss_ir_lbyr

, case when h12ir_sqkm = 0 then 0 
	else ( tn_ir * (nhdir_sqkm / h12ir_sqkm)  * 453592 ) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	end as tn_ir_mgl
, case when h12ir_sqkm = 0 then 0 
	else ( tp_ir * (nhdir_sqkm / h12ir_sqkm)  * 453592 ) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	end as tp_ir_mgl
, case when h12ir_sqkm = 0 then 0 
	else ( tss_ir * (nhdir_sqkm / h12ir_sqkm) * 453592 ) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	end as tss_ir_mgl

-- MO

, case when h12mo_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdmo_sqkm / h12mo_sqkm) * tn_mo) / (intersect_areasqkm * 247.105) 
	end  	as tn_mo_lbacre
, case when h12mo_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdmo_sqkm / h12mo_sqkm) * tp_mo) / (intersect_areasqkm * 247.105) 
	end  	as tp_mo_lbacre
, case when h12mo_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdmo_sqkm / h12mo_sqkm)  * tss_mo) / (intersect_areasqkm * 247.105) 
	end 	as tss_mo_lbacre

, case when h12mo_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdmo_sqkm / h12mo_sqkm) * tn_mo)
	end  	as tn_mo_lbyr
, case when h12mo_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdmo_sqkm / h12mo_sqkm) * tp_mo)
	end  	as tp_mo_lbyr
, case when h12mo_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdmo_sqkm / h12mo_sqkm)  * tss_mo)
	end 	as tss_mo_lbyr

, case when h12mo_sqkm = 0 then 0 
	else ( tn_mo * (nhdmo_sqkm / h12mo_sqkm)  * 453592 ) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	end as tn_mo_mgl
, case when h12mo_sqkm = 0 then 0 
	else ( tp_mo * (nhdmo_sqkm / h12mo_sqkm)  * 453592 ) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	end as tp_mo_mgl
, case when h12mo_sqkm = 0 then 0 
	else ( tss_mo * (nhdmo_sqkm / h12mo_sqkm) * 453592 ) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	end as tss_mo_mgl

-- PAS

, case when h12pas_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdpas_sqkm / h12pas_sqkm) * tn_pas) / (intersect_areasqkm * 247.105) 
	end  	as tn_pas_lbacre
, case when h12pas_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdpas_sqkm / h12pas_sqkm) * tp_pas) / (intersect_areasqkm * 247.105) 
	end  	as tp_pas_lbacre
, case when h12pas_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdpas_sqkm / h12pas_sqkm)  * tss_pas) / (intersect_areasqkm * 247.105) 
	end 	as tss_pas_lbacre

, case when h12pas_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdpas_sqkm / h12pas_sqkm) * tn_pas)
	end  	as tn_pas_lbyr
, case when h12pas_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdpas_sqkm / h12pas_sqkm) * tp_pas)
	end  	as tp_pas_lbyr
, case when h12pas_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdpas_sqkm / h12pas_sqkm)  * tss_pas)
	end 	as tss_pas_lbyr

, case when h12pas_sqkm = 0 then 0 
	else ( tn_pas * (nhdpas_sqkm / h12pas_sqkm)  * 453592 ) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	end as tn_pas_mgl
, case when h12pas_sqkm = 0 then 0 
	else ( tp_pas * (nhdpas_sqkm / h12pas_sqkm)  * 453592 ) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	end as tp_pas_mgl
, case when h12pas_sqkm = 0 then 0 
	else ( tss_pas * (nhdpas_sqkm / h12pas_sqkm) * 453592 ) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	end as tss_pas_mgl

-- TCI

, case when h12tci_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdtci_sqkm / h12tci_sqkm) * tn_tci) / (intersect_areasqkm * 247.105) 
	end  	as tn_tci_lbacre
, case when h12tci_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdtci_sqkm / h12tci_sqkm) * tp_tci) / (intersect_areasqkm * 247.105) 
	end  	as tp_tci_lbacre
, case when h12tci_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdtci_sqkm / h12tci_sqkm)  * tss_tci) / (intersect_areasqkm * 247.105) 
	end 	as tss_tci_lbacre

, case when h12tci_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdtci_sqkm / h12tci_sqkm) * tn_tci)
	end  	as tn_tci_lbyr
, case when h12tci_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdtci_sqkm / h12tci_sqkm) * tp_tci)
	end  	as tp_tci_lbyr
, case when h12tci_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdtci_sqkm / h12tci_sqkm)  * tss_tci)
	end 	as tss_tci_lbyr

, case when h12tci_sqkm = 0 then 0 
	else ( tn_tci * (nhdtci_sqkm / h12tci_sqkm)  * 453592 ) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	end as tn_tci_mgl
, case when h12tci_sqkm = 0 then 0 
	else ( tp_tci * (nhdtci_sqkm / h12tci_sqkm)  * 453592 ) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	end as tp_tci_mgl
, case when h12tci_sqkm = 0 then 0 
	else ( tss_tci * (nhdtci_sqkm / h12tci_sqkm) * 453592 ) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	end as tss_tci_mgl

-- TCT

, case when h12tct_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdtct_sqkm / h12tct_sqkm) * tn_tct) / (intersect_areasqkm * 247.105) 
	end  	as tn_tct_lbacre
, case when h12tct_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdtct_sqkm / h12tct_sqkm) * tp_tct) / (intersect_areasqkm * 247.105) 
	end  	as tp_tct_lbacre
, case when h12tct_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdtct_sqkm / h12tct_sqkm)  * tss_tct) / (intersect_areasqkm * 247.105) 
	end 	as tss_tct_lbacre

, case when h12tct_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdtct_sqkm / h12tct_sqkm) * tn_tct)
	end  	as tn_tct_lbyr
, case when h12tct_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdtct_sqkm / h12tct_sqkm) * tp_tct)
	end  	as tp_tct_lbyr
, case when h12tct_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdtct_sqkm / h12tct_sqkm)  * tss_tct)
	end 	as tss_tct_lbyr

, case when h12tct_sqkm = 0 then 0 
	else ( tn_tct * (nhdtct_sqkm / h12tct_sqkm)  * 453592 ) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	end as tn_tct_mgl
, case when h12tct_sqkm = 0 then 0 
	else ( tp_tct * (nhdtct_sqkm / h12tct_sqkm)  * 453592 ) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	end as tp_tct_mgl
, case when h12tct_sqkm = 0 then 0 
	else ( tss_tct * (nhdtct_sqkm / h12tct_sqkm) * 453592 ) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	end as tss_tct_mgl

-- TG

, case when h12tg_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdtg_sqkm / h12tg_sqkm) * tn_tg) / (intersect_areasqkm * 247.105) 
	end  	as tn_tg_lbacre
, case when h12tg_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdtg_sqkm / h12tg_sqkm) * tp_tg) / (intersect_areasqkm * 247.105) 
	end  	as tp_tg_lbacre
, case when h12tg_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdtg_sqkm / h12tg_sqkm)  * tss_tg) / (intersect_areasqkm * 247.105) 
	end 	as tss_tg_lbacre

, case when h12tg_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdtg_sqkm / h12tg_sqkm) * tn_tg)
	end  	as tn_tg_lbyr
, case when h12tg_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdtg_sqkm / h12tg_sqkm) * tp_tg)
	end  	as tp_tg_lbyr
, case when h12tg_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdtg_sqkm / h12tg_sqkm)  * tss_tg)
	end 	as tss_tg_lbyr

, case when h12tg_sqkm = 0 then 0 
	else ( tn_tg * (nhdtg_sqkm / h12tg_sqkm)  * 453592 ) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	end as tn_tg_mgl
, case when h12tg_sqkm = 0 then 0 
	else ( tp_tg * (nhdtg_sqkm / h12tg_sqkm)  * 453592 ) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	end as tp_tg_mgl
, case when h12tg_sqkm = 0 then 0 
	else ( tss_tg * (nhdtg_sqkm / h12tg_sqkm) * 453592 ) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	end as tss_tg_mgl

-- WAT

, case when h12wat_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdwat_sqkm / h12wat_sqkm) * tn_wat) / (intersect_areasqkm * 247.105) 
	end  	as tn_wat_lbacre
, case when h12wat_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdwat_sqkm / h12wat_sqkm) * tp_wat) / (intersect_areasqkm * 247.105) 
	end  	as tp_wat_lbacre
, case when h12wat_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdwat_sqkm / h12wat_sqkm)  * tss_wat) / (intersect_areasqkm * 247.105) 
	end 	as tss_wat_lbacre

, case when h12wat_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdwat_sqkm / h12wat_sqkm) * tn_wat)
	end  	as tn_wat_lbyr
, case when h12wat_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdwat_sqkm / h12wat_sqkm) * tp_wat)
	end  	as tp_wat_lbyr
, case when h12wat_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdwat_sqkm / h12wat_sqkm)  * tss_wat)
	end 	as tss_wat_lbyr

, case when h12wat_sqkm = 0 then 0 
	else ( tn_wat * (nhdwat_sqkm / h12wat_sqkm)  * 453592 ) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	end as tn_wat_mgl
, case when h12wat_sqkm = 0 then 0 
	else ( tp_wat * (nhdwat_sqkm / h12wat_sqkm)  * 453592 ) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	end as tp_wat_mgl
, case when h12wat_sqkm = 0 then 0 
	else ( tss_wat * (nhdwat_sqkm / h12wat_sqkm) * 453592 ) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	end as tss_wat_mgl

-- WLF

, case when h12wlf_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdwlf_sqkm / h12wlf_sqkm) * tn_wlf) / (intersect_areasqkm * 247.105) 
	end  	as tn_wlf_lbacre
, case when h12wlf_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdwlf_sqkm / h12wlf_sqkm) * tp_wlf) / (intersect_areasqkm * 247.105) 
	end  	as tp_wlf_lbacre
, case when h12wlf_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdwlf_sqkm / h12wlf_sqkm)  * tss_wlf) / (intersect_areasqkm * 247.105) 
	end 	as tss_wlf_lbacre

, case when h12wlf_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdwlf_sqkm / h12wlf_sqkm) * tn_wlf)
	end  	as tn_wlf_lbyr
, case when h12wlf_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdwlf_sqkm / h12wlf_sqkm) * tp_wlf)
	end  	as tp_wlf_lbyr
, case when h12wlf_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdwlf_sqkm / h12wlf_sqkm)  * tss_wlf)
	end 	as tss_wlf_lbyr

, case when h12wlf_sqkm = 0 then 0 
	else ( tn_wlf * (nhdwlf_sqkm / h12wlf_sqkm)  * 453592 ) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	end as tn_wlf_mgl
, case when h12wlf_sqkm = 0 then 0 
	else ( tp_wlf * (nhdwlf_sqkm / h12wlf_sqkm)  * 453592 ) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	end as tp_wlf_mgl
, case when h12wlf_sqkm = 0 then 0 
	else ( tss_wlf * (nhdwlf_sqkm / h12wlf_sqkm) * 453592 ) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	end as tss_wlf_mgl

-- WLO

, case when h12wlo_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdwlo_sqkm / h12wlo_sqkm) * tn_wlo) / (intersect_areasqkm * 247.105) 
	end  	as tn_wlo_lbacre
, case when h12wlo_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdwlo_sqkm / h12wlo_sqkm) * tp_wlo) / (intersect_areasqkm * 247.105) 
	end  	as tp_wlo_lbacre
, case when h12wlo_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdwlo_sqkm / h12wlo_sqkm)  * tss_wlo) / (intersect_areasqkm * 247.105) 
	end 	as tss_wlo_lbacre

, case when h12wlo_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdwlo_sqkm / h12wlo_sqkm) * tn_wlo)
	end  	as tn_wlo_lbyr
, case when h12wlo_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdwlo_sqkm / h12wlo_sqkm) * tp_wlo)
	end  	as tp_wlo_lbyr
, case when h12wlo_sqkm = 0 then 0 
	else ((intersect_areasqkm / nhd_areasqkm) * (nhdwlo_sqkm / h12wlo_sqkm)  * tss_wlo)
	end 	as tss_wlo_lbyr

, case when h12wlo_sqkm = 0 then 0 
	else ( tn_wlo * (nhdwlo_sqkm / h12wlo_sqkm)  * 453592 ) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	end as tn_wlo_mgl
, case when h12wlo_sqkm = 0 then 0 
	else ( tp_wlo * (nhdwlo_sqkm / h12wlo_sqkm)  * 453592 ) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	end as tp_wlo_mgl
, case when h12wlo_sqkm = 0 then 0 
	else ( tss_wlo * (nhdwlo_sqkm / h12wlo_sqkm) * 453592 ) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	end as tss_wlo_mgl

-- WLT -- There are no load inputs or reductions noted for WLT

-- remaining load_sources
-- construction  /* CONSIDER LIMITING THIS TO MO/INR/IR LULC instead of a pure by area distribution*/
, tn_con_lbyr  * (intersect_areasqkm / h12_tot_sqkm) / (intersect_areasqkm * 247.105) as tn_construction_lbacre
, tp_con_lbyr  * (intersect_areasqkm / h12_tot_sqkm) / (intersect_areasqkm * 247.105) as tp_construction_lbacre
, tss_con_lbyr * (intersect_areasqkm / h12_tot_sqkm) / (intersect_areasqkm * 247.105) as tss_construction_lbacre

, tn_con_lbyr  * (intersect_areasqkm / h12_tot_sqkm) as tn_construction_lbyr
, tp_con_lbyr  * (intersect_areasqkm / h12_tot_sqkm) as tp_construction_lbyr
, tss_con_lbyr * (intersect_areasqkm / h12_tot_sqkm) as tss_construction_lbyr

, ( tn_con_lbyr  * 453592 )  *  (intersect_areasqkm / h12_tot_sqkm) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) as tn_construction_mgl
, ( tp_con_lbyr  * 453592 )  *  (intersect_areasqkm / h12_tot_sqkm) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) as tp_construction_mgl
, ( tss_con_lbyr * 453592 )  *  (intersect_areasqkm / h12_tot_sqkm) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) as tss_construction_mgl

-- riparian pasture deposition - rpd is in lb/yr direct load into streams
, tn_rpd   * (intersect_areasqkm / h12_tot_sqkm) / (intersect_areasqkm * 247.105) as tn_rpd_lbacre
, tp_rpd   * (intersect_areasqkm / h12_tot_sqkm) / (intersect_areasqkm * 247.105) as tp_rpd_lbacre
, tss_rpd  * (intersect_areasqkm / h12_tot_sqkm) / (intersect_areasqkm * 247.105) as tss_rpd_lbacre

, tn_rpd   * (intersect_areasqkm / h12_tot_sqkm) as tn_rpd_lbyr
, tp_rpd   * (intersect_areasqkm / h12_tot_sqkm) as tp_rpd_lbyr
, tss_rpd  * (intersect_areasqkm / h12_tot_sqkm) as tss_rpd_lbyr

, ( tn_rpd   * 453592 )  *  (intersect_areasqkm / h12_tot_sqkm) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) as tn_rpd_mgl
, ( tp_rpd   * 453592 )  *  (intersect_areasqkm / h12_tot_sqkm) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) as tp_rpd_mgl
, ( tss_rpd  * 453592 )  *  (intersect_areasqkm / h12_tot_sqkm) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) as tss_rpd_mgl

-- shoreline
, tn_shore_lbkm  * (intersect_lengthkm / huc12_lengthkm) as tn_shore_lbkm
, tp_shore_lbkm  * (intersect_lengthkm / huc12_lengthkm) as tp_shore_lbkm
, tss_shore_lbkm * (intersect_lengthkm / huc12_lengthkm) as tss_shore_lbkm

, tn_shore_lbyr  * (intersect_lengthkm / huc12_lengthkm) as tn_shore_lbyr
, tp_shore_lbyr  * (intersect_lengthkm / huc12_lengthkm) as tp_shore_lbyr
, tss_shore_lbyr * (intersect_lengthkm / huc12_lengthkm) as tss_shore_lbyr

, ( tn_shore_lbyr    * 453592 )  *  (intersect_areasqkm / h12_tot_sqkm) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) as tn_shore_mgl
, ( tp_shore_lbyr    * 453592 )  *  (intersect_areasqkm / h12_tot_sqkm) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) as tp_shore_mgl
, ( tss_shore_lbyr   * 453592 )  *  (intersect_areasqkm / h12_tot_sqkm) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) as tss_shore_mgl

-- stream bed and bank
, tn_sbb_lbkm  * (intersect_lengthkm / huc12_lengthkm) as tn_streambnb_lbkm
, tp_sbb_lbkm  * (intersect_lengthkm / huc12_lengthkm) as tp_streambnb_lbkm
, tss_sbb_lbkm * (intersect_lengthkm / huc12_lengthkm) as tss_streambnb_lbkm

, tn_sbb_lbyr  * (intersect_lengthkm / huc12_lengthkm) as tn_streambnb_lbyr
, tp_sbb_lbyr  * (intersect_lengthkm / huc12_lengthkm) as tp_streambnb_lbyr
, tss_sbb_lbyr * (intersect_lengthkm / huc12_lengthkm) as tss_streambnb_lbyr

, ( tn_sbb_lbyr    * 453592 )  * (intersect_lengthkm / huc12_lengthkm) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) as tn_streambnb_mgl
, ( tp_sbb_lbyr    * 453592 )  * (intersect_lengthkm / huc12_lengthkm) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) as tp_streambnb_mgl
, ( tss_sbb_lbyr   * 453592 )  * (intersect_lengthkm / huc12_lengthkm) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) as tss_streambnb_mgl

-- stream flood plain
, tn_sfp_lbkm  * (intersect_lengthkm / huc12_lengthkm) as tn_streamfp_lbkm
, tp_sfp_lbkm  * (intersect_lengthkm / huc12_lengthkm) as tp_streamfp_lbkm
, tss_sfp_lbkm * (intersect_lengthkm / huc12_lengthkm) as tss_streamfp_lbkm

, tn_sfp_lbyr  * (intersect_lengthkm / huc12_lengthkm) as tn_streamfp_lbyr
, tp_sfp_lbyr  * (intersect_lengthkm / huc12_lengthkm) as tp_streamfp_lbyr
, tss_sfp_lbyr * (intersect_lengthkm / huc12_lengthkm) as tss_streamfp_lbyr

, ( tn_sfp_lbyr    * 453592 )  * (intersect_lengthkm / huc12_lengthkm) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) as tn_streamfp_mgl
, ( tp_sfp_lbyr    * 453592 )  * (intersect_lengthkm / huc12_lengthkm) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) as tp_streamfp_mgl
, ( tss_sfp_lbyr   * 453592 )  * (intersect_lengthkm / huc12_lengthkm) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) as tss_streamfp_mgl

-- wastewater load_sources

-- cso - tn_cso_lbyr - tn_cso_areaonly_lbyr (strict by area from huc12 to nhd) vs tn_cso_npdes_lbyr (accuonts for cso location within huc12 and assigns load to those nhd catchments)
, tn_cso_areaonly_lbyr  * (intersect_areasqkm / h12_tot_sqkm) / (intersect_areasqkm * 247.105) as tn_cso_areaonly_lbacre
, tp_cso_areaonly_lbyr  * (intersect_areasqkm / h12_tot_sqkm) / (intersect_areasqkm * 247.105) as tp_cso_areaonly_lbacre
, tss_cso_areaonly_lbyr * (intersect_areasqkm / h12_tot_sqkm) / (intersect_areasqkm * 247.105) as tss_cso_areaonly_lbacre

, tn_cso_areaonly_lbyr  * (intersect_areasqkm / h12_tot_sqkm) as tn_cso_areaonly_lbyr
, tp_cso_areaonly_lbyr  * (intersect_areasqkm / h12_tot_sqkm) as tp_cso_areaonly_lbyr
, tss_cso_areaonly_lbyr * (intersect_areasqkm / h12_tot_sqkm) as tss_cso_areaonly_lbyr

, ( tn_cso_areaonly_lbyr    * 453592 )  * (intersect_lengthkm / huc12_lengthkm) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) as tn_cso_areaonly_mgl
, ( tp_cso_areaonly_lbyr    * 453592 )  * (intersect_lengthkm / huc12_lengthkm) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) as tp_cso_areaonly_mgl
, ( tss_cso_areaonly_lbyr   * 453592 )  * (intersect_lengthkm / huc12_lengthkm) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) as tss_cso_areaonly_mgl

-- wwi - tn_wwi_lbyr
, sum(tn_wwi_lbyr)  * (intersect_areasqkm / h12_tot_sqkm) / (intersect_areasqkm * 247.105) as tn_wwi_lbacre
, sum(tp_wwi_lbyr)  * (intersect_areasqkm / h12_tot_sqkm) / (intersect_areasqkm * 247.105) as tp_wwi_lbacre
, sum(tss_wwi_lbyr) * (intersect_areasqkm / h12_tot_sqkm) / (intersect_areasqkm * 247.105) as tss_wwi_lbacre

, sum(tn_wwi_lbyr)  * (intersect_areasqkm / h12_tot_sqkm) as tn_wwi_lbyr
, sum(tp_wwi_lbyr)  * (intersect_areasqkm / h12_tot_sqkm) as tp_wwi_lbyr
, sum(tss_wwi_lbyr) * (intersect_areasqkm / h12_tot_sqkm) as tss_wwi_lbyr

, sum(tn_wwi_mgl) as tn_wwi_mgl
, sum(tp_wwi_mgl) as tp_wwi_mgl
, sum(tss_wwi_mgl) as tss_wwi_mgl

-- wwm - tn_wwm_lbyr
, sum(tn_wwm_lbyr)   * (intersect_areasqkm / h12_tot_sqkm) / (intersect_areasqkm * 247.105) as tn_wwm_lbacre
, sum(tp_wwm_lbyr)   * (intersect_areasqkm / h12_tot_sqkm) / (intersect_areasqkm * 247.105) as tp_wwm_lbacre
, sum(tss_wwm_lbyr)  * (intersect_areasqkm / h12_tot_sqkm) / (intersect_areasqkm * 247.105) as tss_wwm_lbacre

, sum(tn_wwm_lbyr)   * (intersect_areasqkm / h12_tot_sqkm) as tn_wwm_lbyr
, sum(tp_wwm_lbyr)   * (intersect_areasqkm / h12_tot_sqkm) as tp_wwm_lbyr
, sum(tss_wwm_lbyr)  * (intersect_areasqkm / h12_tot_sqkm) as tss_wwm_lbyr

, sum(tn_wwm_mgl) as tn_wwm_mgl
, sum(tp_wwm_mgl) as tp_wwm_mgl
, sum(tss_wwm_mgl) as tss_wwm_mgl

-- rib - tn_rib_lbyr
, tn_rib_lbyr    * (intersect_areasqkm / h12_tot_sqkm) / (intersect_areasqkm * 247.105) as tn_rib_lbacre
, tp_rib_lbyr    * (intersect_areasqkm / h12_tot_sqkm) / (intersect_areasqkm * 247.105) as tp_rib_lbacre

, tn_rib_lbyr    * (intersect_areasqkm / h12_tot_sqkm) as tn_rib_lbyr
, tp_rib_lbyr    * (intersect_areasqkm / h12_tot_sqkm) as tp_rib_lbyr

, ( tn_rib_lbyr    * 453592 )  * (intersect_lengthkm / huc12_lengthkm) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) as tn_rib_mgl
, ( tp_rib_lbyr    * 453592 )  * (intersect_lengthkm / huc12_lengthkm) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) as tp_rib_mgl

-- sep - tn_sep_lbyr
, tn_sep_lbyr     * (intersect_areasqkm / h12_tot_sqkm) / (intersect_areasqkm * 247.105) as tn_sep_lbacre
, tp_sep_lbyr     * (intersect_areasqkm / h12_tot_sqkm) / (intersect_areasqkm * 247.105) as tp_sep_lbacre

, tn_sep_lbyr     * (intersect_areasqkm / h12_tot_sqkm) as tn_sep_lbyr
, tp_sep_lbyr     * (intersect_areasqkm / h12_tot_sqkm) as tp_sep_lbyr

, ( tn_sep_lbyr    * 453592 )  * (intersect_lengthkm / huc12_lengthkm) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) as tn_sep_mgl
, ( tp_sep_lbyr    * 453592 )  * (intersect_lengthkm / huc12_lengthkm) / (  case when maflowv > 0 then maflowv else null end  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) as tp_sep_mgl


from datasusqu.nhdxhuc12_rerun as a--(select * from datasusqu.nhdxhuc12_rerun where (intersect_areasqkm / nhd_areasqkm) > 0.05) as a

-- lulc joins

left join
	( -- lb/year of nutrient loading from cropland
	 select huc12
	  ,sum(nloadeos) as tn_crp
	  ,sum(ploadeos) as tp_crp
	  ,sum(sloadeos) as tss_crp 
	 from datasusqu.cbwsm_huc12_loading_sources 
	 where lu_code like 'crp'
	 group by huc12
	) as crp
on a.huc12 = crp.huc12
left join
	( -- lb/year of nutrient loading from forest
	 select huc12
	  ,sum(nloadeos) as tn_fore
	  ,sum(ploadeos) as tp_fore
	  ,sum(sloadeos) as tss_fore 
	 from datasusqu.cbwsm_huc12_loading_sources 
	 where lu_code like 'fore'
	 group by huc12
	) as fore
on a.huc12 = fore.huc12
left join
	( -- lb/year of nutrient loading from impervious non-roads
	 select huc12
	  ,sum(nloadeos) as tn_inr
	  ,sum(ploadeos) as tp_inr
	  ,sum(sloadeos) as tss_inr 
	 from datasusqu.cbwsm_huc12_loading_sources 
	 where lu_code like 'inr'
	 group by huc12
	) as inr
on a.huc12 = inr.huc12
left join
	( -- lb/year of nutrient loading from impervious roads
	 select huc12
	  ,sum(nloadeos) as tn_ir
	  ,sum(ploadeos) as tp_ir
	  ,sum(sloadeos) as tss_ir 
	 from datasusqu.cbwsm_huc12_loading_sources 
	 where lu_code like 'ir'
	 group by huc12
	) as ir
on a.huc12 = ir.huc12
left join
	( -- lb/year of nutrient loading from mixed open
	 select huc12
	  ,sum(nloadeos) as tn_mo
	  ,sum(ploadeos) as tp_mo
	  ,sum(sloadeos) as tss_mo 
	 from datasusqu.cbwsm_huc12_loading_sources 
	 where lu_code like 'mo'
	 group by huc12
	) as mo
on a.huc12 = mo.huc12
left join
	( -- lb/year of nutrient loading from pasture
	 select huc12
	  ,sum(nloadeos) as tn_pas
	  ,sum(ploadeos) as tp_pas
	  ,sum(sloadeos) as tss_pas 
	 from datasusqu.cbwsm_huc12_loading_sources 
	 where lu_code like 'pas'
	 group by huc12
	) as pas
on a.huc12 = pas.huc12
left join
	( -- lb/year of nutrient loading from tree canopy over impervious
	 select huc12
	  ,sum(nloadeos) as tn_tci
	  ,sum(ploadeos) as tp_tci
	  ,sum(sloadeos) as tss_tci 
	 from datasusqu.cbwsm_huc12_loading_sources 
	 where lu_code like 'tci'
	 group by huc12
	) as tci
on a.huc12 = tci.huc12
left join
	( -- lb/year of nutrient loading from tree cover over turf grass
	 select huc12
	  ,sum(nloadeos) as tn_tct
	  ,sum(ploadeos) as tp_tct
	  ,sum(sloadeos) as tss_tct 
	 from datasusqu.cbwsm_huc12_loading_sources
	 where lu_code like 'tct'
	 group by huc12
	) as tct
on a.huc12 = tct.huc12
left join
	( -- lb/year of nutrient loading from turf grass
	 select huc12
	  ,sum(nloadeos) as tn_tg
	  ,sum(ploadeos) as tp_tg
	  ,sum(sloadeos) as tss_tg 
	 from datasusqu.cbwsm_huc12_loading_sources 
	 where lu_code like 'tg'
	 group by huc12
	) as tg
on a.huc12 = tg.huc12
left join
	( -- lb/year of nutrient loading from water
	 select huc12
	  ,sum(nloadeos) as tn_wat
	  ,sum(ploadeos) as tp_wat
	  ,sum(sloadeos) as tss_wat 
	 from datasusqu.cbwsm_huc12_loading_sources 
	 where lu_code like 'wat'
	 group by huc12
	) as wat
on a.huc12 = wat.huc12
left join
	( -- lb/year of nutrient loading from floodplain wetlands
	 select huc12
	  ,sum(nloadeos) as tn_wlf
	  ,sum(ploadeos) as tp_wlf
	  ,sum(sloadeos) as tss_wlf 
	 from datasusqu.cbwsm_huc12_loading_sources 
	 where lu_code like 'wlf'
	 group by huc12
	) as wlf
on a.huc12 = wlf.huc12
left join
	( -- lb/year of nutrient loading from open non-tidal wetlands
	 select huc12
	  ,sum(nloadeos) as tn_wlo
	  ,sum(ploadeos) as tp_wlo
	  ,sum(sloadeos) as tss_wlo 
	 from datasusqu.cbwsm_huc12_loading_sources 
	 where lu_code like 'wlo'
	 group by huc12
	) as wlo
on a.huc12 = wlo.huc12

-------------------------------------------------------------------------------------------------------------------------

-- wastewater and septic joins

/*THIS CALC IS NOT DONE, NEED TO FIGURE OUT CSO POINT SOURCE DATASET*/
left join
	( -- lb/year of nutrient loading from combined sewer overflow, using NPDES outfall locations and their MGD -> mg/L nutrient concentrations
	 select huc12,comid,npdes_id
	  ,sum(tn_lbyr) as tn_cso_npdes_lbyr
	  ,sum(tp_lbyr) as tp_cso_npdes_lbyr
	  ,sum(tss_lbyr) as tss_cso_npdes_lbyr
	  ,sum(tn_mgl) as tn_cso_npdes_mgl
	  ,sum(tp_mgl) as tp_cso_npdes_mgl
	  ,sum(tss_mgl) as tss_cso_npdes_mgl
	 from datasusqu.ww_cso 
	 group by huc12,comid ,npdes_id
	 order by huc12
	) as cso_npdes
on a.huc12 = cso_npdes.huc12
left join
	( -- lb/year of nutrient loading from combined sewer overflow, using the CBWSM HUC12 output
	 select huc12
	  ,sum(nloadeos) as tn_cso_areaonly_lbyr
	  ,sum(ploadeos) as tp_cso_areaonly_lbyr
	  ,sum(sloadeos) as tss_cso_areaonly_lbyr
	 from datasusqu.cbwsm_huc12_loading_sources 
	 where lu_code like 'cso'
	 group by huc12
	 order by huc12
	) as cso_areaonly
on a.huc12 = cso_areaonly.huc12
/*END THIS CALC IS NOT DONE*/

left join
	( -- lb/year of nutrient loading from industrial wastewater
	 select huc12,comid
	  ,sum(tn_lbyr) as tn_wwi_lbyr
	  ,sum(tp_lbyr) as tp_wwi_lbyr
	  ,sum(tss_lbyr) as tss_wwi_lbyr
	  ,sum(tn_mgl) as tn_wwi_mgl
	  ,sum(tp_mgl) as tp_wwi_mgl
	  ,sum(tss_mgl) as tss_wwi_mgl
	 from datasusqu.ww_wwtp 
	 where loadsource like '%Industrial%'
	 group by huc12,comid
	 order by comid
	) as wwi
on a.comid = wwi.comid
left join
	( -- lb/year of nutrient loading from municipal wastewater
	 select comid,huc12
	  ,sum(tn_lbyr) as tn_wwm_lbyr
	  ,sum(tp_lbyr) as tp_wwm_lbyr
	  ,sum(tss_lbyr) as tss_wwm_lbyr
	  ,sum(tn_mgl) as tn_wwm_mgl
	  ,sum(tp_mgl) as tp_wwm_mgl
	  ,sum(tss_mgl) as tss_wwm_mgl
	 from datasusqu.ww_wwtp 
	 where loadsource like '%Municipal%'
	 group by comid,huc12
	 order by comid
	) as wwm
on a.comid = wwm.comid
left join
	( -- lb/year of nutrient loading from rapid infiltration basin
	 select huc12
	  ,sum(tn_lbyr) as tn_rib_lbyr
	  ,sum(tp_lbyr) as tp_rib_lbyr
	 from datasusqu.ww_septic_rapidinfilbasin 
	 where loadsource like '%Rapid%' 
	 group by huc12
	) as rib
on a.huc12 = rib.huc12
left join
	( -- lb/year of nutrient loading from septic
	 select huc12
	  ,sum(tn_lbyr) as tn_sep_lbyr
	  ,sum(tp_lbyr) as tp_sep_lbyr
	 from datasusqu.ww_septic_rapidinfilbasin 
	 where loadsource like '%Septic%' 
	 group by huc12
	) as sep
on a.huc12 = sep.huc12

-------------------------------------------------------------------------------------------------------------------------

-- remaining joins: stream bed and bank, stream flood plain, shoreline, riparian pasture deposistion and construction joins
left join
	( -- lb/year of nutrient loading from construction
	 select huc12
	  ,sum(nloadeos) as tn_con_lbyr
	  ,sum(ploadeos) as tp_con_lbyr
	  ,sum(sloadeos) as tss_con_lbyr 
	  ,sum(nloadeos / (amount)) as tn_con_lbacre
	  ,sum(ploadeos / (amount)) as tp_con_lbacre
	  ,sum(sloadeos / (amount)) as tss_con_lbacre 
	 from datasusqu.cbwsm_huc12_loading_sources 
	 where lu_code like 'con' and amount > 0
	 group by huc12
	) as con
on a.huc12 = con.huc12
left join
	( -- lb/year of nutrient loading from riparian pasture deposition (direct, unit-less area, load) lb/year
	 select huc12
	  ,sum(nloadeos) as tn_rpd
	  ,sum(ploadeos) as tp_rpd
	  ,sum(sloadeos) as tss_rpd
	 from datasusqu.cbwsm_huc12_loading_sources 
	 where lu_code like 'rpd'
	 group by huc12
	) as rpd
on a.huc12 = rpd.huc12
left join
	( -- lb/year of nutrient loading from shoreline
	 select huc12
	  ,sum(nloadeos) as tn_shore_lbyr
	  ,sum(ploadeos) as tp_shore_lbyr
	  ,sum(sloadeos) as tss_shore_lbyr
	  ,sum(nloadeos / ((amount+0.0000000000000000000001) * 1.609)) as tn_shore_lbkm
	  ,sum(ploadeos / ((amount+0.0000000000000000000001) * 1.609)) as tp_shore_lbkm
	  ,sum(sloadeos / ((amount+0.0000000000000000000001) * 1.609)) as tss_shore_lbkm
	 from datasusqu.cbwsm_huc12_loading_sources 
	 where lu_code like 'shore'
	 group by huc12
	) as shore
on a.huc12 = shore.huc12
left join
	( -- lb/year of nutrient loading from the stream bed and stream bank
	 select huc12
	  ,sum(nloadeos) as tn_sbb_lbyr
	  ,sum(ploadeos) as tp_sbb_lbyr
	  ,sum(sloadeos) as tss_sbb_lbyr
	  ,sum(nloadeos / ((amount+0.0000000000000000000001) * 1.609)) as tn_sbb_lbkm
	  ,sum(ploadeos / ((amount+0.0000000000000000000001) * 1.609)) as tp_sbb_lbkm
	  ,sum(sloadeos / ((amount+0.0000000000000000000001) * 1.609)) as tss_sbb_lbkm
	 from datasusqu.cbwsm_huc12_loading_sources 
	 where lu_code like 'sbb'
	 group by huc12
	) as sbb
on a.huc12 = sbb.huc12
left join
	( -- lb/year of nutrient loading from the stream flood plain
	 select huc12
	  ,sum(nloadeos) as tn_sfp_lbyr
	  ,sum(ploadeos) as tp_sfp_lbyr
	  ,sum(sloadeos) as tss_sfp_lbyr
	  ,sum(nloadeos / ((amount+0.0000000000000000000001) * 1.609)) as tn_sfp_lbkm
	  ,sum(ploadeos / ((amount+0.0000000000000000000001) * 1.609)) as tp_sfp_lbkm
	  ,sum(sloadeos / ((amount+0.0000000000000000000001) * 1.609)) as tss_sfp_lbkm
	 from datasusqu.cbwsm_huc12_loading_sources 
	 where lu_code like 'sfp'
	 group by huc12
	) as sfp
on a.huc12 = sfp.huc12
where a.nord is not null 
and a.intersect_areasqkm / a.nhd_areasqkm > 0.05
	--and comid = 8538463
group by a.comid ,a.huc12

,tn_crp	,tp_crp	,tss_crp
,tn_fore,tp_fore,tss_fore
,tn_inr	,tp_inr	,tss_inr
,tn_ir	,tp_ir	,tss_ir
,tn_mo	,tp_mo	,tss_mo
,tn_pas	,tp_pas	,tss_pas
,tn_tci	,tp_tci	,tss_tci
,tn_tct	,tp_tct	,tss_tct
,tn_tg	,tp_tg	,tss_tg
,tn_wat	,tp_wat	,tss_wat
,tn_wlf	,tp_wlf	,tss_wlf
,tn_wlo	,tp_wlo	,tss_wlo

,tn_con_lbyr	,tp_con_lbyr	,tss_con_lbyr
,tn_rpd	,tp_rpd	,tss_rpd
,tn_shore_lbkm	,tp_shore_lbkm	,tss_shore_lbkm
,tn_shore_lbyr	,tp_shore_lbyr	,tss_shore_lbyr
,tn_sbb_lbkm	,tp_sbb_lbkm	,tss_sbb_lbkm
,tn_sbb_lbyr	,tp_sbb_lbyr	,tss_sbb_lbyr
,tn_sfp_lbkm	,tp_sfp_lbkm	,tss_sfp_lbkm
,tn_sfp_lbyr	,tp_sfp_lbyr	,tss_sfp_lbyr
,tn_cso_areaonly_lbyr	,tp_cso_areaonly_lbyr	,tss_cso_areaonly_lbyr
,tn_rib_lbyr	,tp_rib_lbyr
,tn_sep_lbyr	,tp_sep_lbyr

order by a.comid , a.huc12

) as t1

join spatial.nhdplus_maregion as nhd
on t1.nord = nhd.nord

group by t1.comid
,	nhd.maflowv	
,	nhd.catchment

order by t1.comid ) as t2

left join spatial.nhdplus_maregion as nhd
on t2.comid = nhd.comid

left join datasusqu.nhdplus_luproportions_rerun as lu_nhd
on t2.comid = lu_nhd.comid

--where comid = 8538463

order by t2.comid

;

/*  CACHE THE TABLE  */

drop table if exists analysissusqu.cache_final_lateralshed_04;
create table analysissusqu.cache_final_lateralshed_04
as
select * from analysissusqu.final_lateralshed_04;

alter table analysissusqu.cache_final_lateralshed_04 add constraint pk_cache_final_lateralshed_04 primary key (nord);

alter table analysissusqu.cache_final_lateralshed_04 add column huc12 varchar(12);
update analysissusqu.cache_final_lateralshed_04 as a set huc12 = b.huc12
from datasusqu.nhdxhuc12_rerun as b
where a.comid = b.comid
and b.intersect_areasqkm/b.nhd_areasqkm >= 0.95;

CREATE INDEX cache_final_lateralshed_04_geom_idx
  ON analysissusqu.cache_final_lateralshed_04
  USING gist
  (catchment);

CREATE INDEX cache_final_lateralshed_04_nord_idx
  ON analysissusqu.cache_final_lateralshed_04
  USING btree
  (nord);

CREATE INDEX cache_final_lateralshed_04_nordnordstop_idx
  ON analysissusqu.cache_final_lateralshed_04
  USING btree
  (nord,nordstop);

GRANT SELECT ON analysissusqu.cache_final_lateralshed_04 TO selectrole;


--- Point Sources Check upstream loading
/*
select idx.nord,idx.nordstop
,sum(tn_wwtp_mgl) as tn_wwtp_mgl
,sum(tp_wwtp_mgl) as tp_wwtp_mgl
,sum(tss_wwtp_mgl) as tss_wwtp_mgl
from (
select a.huc12, a.comid,a.nord,a.nordstop
, sum(tn_wwtp_lbyr)  * (intersect_areasqkm / h12_tot_sqkm) as tn_wwtp_lbyr
, sum(tp_wwtp_lbyr)  * (intersect_areasqkm / h12_tot_sqkm) as tp_wwtp_lbyr
, sum(tss_wwtp_lbyr) * (intersect_areasqkm / h12_tot_sqkm) as tss_wwtp_lbyr
, sum(tn_wwtp_mgl) as tn_wwtp_mgl
, sum(tp_wwtp_mgl) as tp_wwtp_mgl
, sum(tss_wwtp_mgl) as tss_wwtp_mgl
from datasusqu.nhdxhuc12_rerun as a
inner join
	( -- lb/year of nutrient loading from wastewater point sources
	 select huc12,comid
	  ,sum(tn_lbyr) as tn_wwtp_lbyr
	  ,sum(tp_lbyr) as tp_wwtp_lbyr
	  ,sum(tss_lbyr) as tss_wwtp_lbyr
	  ,sum(tn_mgl) as tn_wwtp_mgl
	  ,sum(tp_mgl) as tp_wwtp_mgl
	  ,sum(tss_mgl) as tss_wwtp_mgl
	 from datasusqu.ww_wwtp 
	 where loadsource like '%Industrial%' or loadsource like '%Municipal%'
	 group by huc12,comid
	 order by comid
	) as wwtp
on a.comid = wwtp.comid
group by a.comid, a.huc12
order by a.comid, a.huc12
) as t1
left join spatial.nhdplus_maregion_idx as idx
on t1.nord between idx.nord and idx.nordstop
group by idx.nord
*/

--- Final Watershed Nested Set Upstream Calculations, takes into factor extinction coef and upstream point sources

-- mg/L converts lb to mg, ft^(3) to L, seconds to year. Dividing lb/yr by mean annual flow -> mean annual concentration

drop view if exists analysissusqu.final_watershed_04;
create view analysissusqu.final_watershed_04
as
select t2.gnis_name,t1.nord, t2.nordstop, t2.comid, t2.huc12, t2.flow_cfs

,t1.tn_lbyr_ups as tn_lbyr
, case when flow_cfs < 0 then -9999
	else ( t1.tn_lbyr_ups * 453592 ) / (  flow_cfs  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) )
	end as tn_mgl
,tn_lbyr_surfaceload_ups / (sqm_ups / 4046.86) as tn_lbacre

,t1.tp_lbyr_ups as tp_lbyr
, case when flow_cfs < 0 then -9999
	else ( t1.tp_lbyr_ups * 453592 ) / (  flow_cfs  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) )
	end as tp_mgl
,tp_lbyr_surfaceload_ups / (sqm_ups / 4046.86) as tp_lbacre

,t1.tss_lbyr_ups as tss_lbyr
, case when flow_cfs < 0 then -9999
	else ( t1.tss_lbyr_ups * 453592 ) / (  flow_cfs  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) )
	end as tss_mgl
,tss_lbyr_surfaceload_ups / (sqm_ups / 4046.86) as tss_lbacre

,t2.sqm_ups      / 4046.86 		as ups_acres
,t2.crp_sqm_ups  / 4046.86 		as crp_ups_acres
,t2.fore_sqm_ups / 4046.86 		as fore_ups_acres
,t2.inr_sqm_ups  / 4046.86 		as inr_ups_acres
,t2.ir_sqm_ups   / 4046.86 		as ir_ups_acres
,t2.mo_sqm_ups   / 4046.86 		as mo_ups_acres
,t2.pas_sqm_ups  / 4046.86 		as pas_ups_acres
,t2.tci_sqm_ups  / 4046.86 		as tci_ups_acres
,t2.tct_sqm_ups  / 4046.86 		as tct_ups_acres
,t2.tg_sqm_ups   / 4046.86 		as tg_ups_acres
,t2.wat_sqm_ups  / 4046.86 		as wat_ups_acres
,t2.wlf_sqm_ups  / 4046.86 		as wlf_ups_acres
,t2.wlo_sqm_ups  / 4046.86 		as wlo_ups_acres
,t2.wlt_sqm_ups  / 4046.86 		as wlt_ups_acres
,t2.crp_sqm_ups  / t2.sqm_ups * 100 	as p_crp_ups
,t2.fore_sqm_ups / t2.sqm_ups * 100 	as p_fore_ups
,t2.inr_sqm_ups  / t2.sqm_ups * 100 	as p_inr_ups
,t2.ir_sqm_ups   / t2.sqm_ups * 100 	as p_ir_ups
,t2.mo_sqm_ups   / t2.sqm_ups * 100 	as p_mo_ups
,t2.pas_sqm_ups  / t2.sqm_ups * 100 	as p_pas_ups
,t2.tci_sqm_ups  / t2.sqm_ups * 100 	as p_tci_ups
,t2.tct_sqm_ups  / t2.sqm_ups * 100 	as p_tct_ups
,t2.tg_sqm_ups   / t2.sqm_ups * 100 	as p_tg_ups
,t2.wat_sqm_ups  / t2.sqm_ups * 100 	as p_wat_ups
,t2.wlf_sqm_ups  / t2.sqm_ups * 100 	as p_wlf_ups
,t2.wlo_sqm_ups  / t2.sqm_ups * 100 	as p_wlo_ups
,t2.wlt_sqm_ups  / t2.sqm_ups * 100 	as p_wlt_ups

,t2.geom
,t2.catchment
from (
	select idx.nord
	,sum(tn_lbyr  * tn_extict_coef)  as tn_lbyr_ups
	,sum(tp_lbyr  * tp_extict_coef)  as tp_lbyr_ups
	,sum(tss_lbyr * tss_extict_coef) as tss_lbyr_ups

	,sum(tn_lbyr_surfaceload  * tn_extict_coef)    as tn_lbyr_surfaceload_ups
	,sum(tp_lbyr_surfaceload  * tp_extict_coef)    as tp_lbyr_surfaceload_ups
	,sum(tss_lbyr_surfaceload  * tss_extict_coef)  as tss_lbyr_surfaceload_ups

	from (select nord, tn_lbyr, tp_lbyr, tss_lbyr,tn_lbyr_surfaceload,tp_lbyr_surfaceload,tss_lbyr_surfaceload,tn_extict_coef,tp_extict_coef,tss_extict_coef from analysissusqu.cache_final_lateralshed_04 group by nord order by nord) as a
	join spatial.nhdplus_maregion_idx as idx
	on a.nord between idx.nord and idx.nordstop
	group by idx.nord
	order by idx.nord
     ) as t1
left join ( 
	select a.*, b.sqm_ups, b.crp_sqm_ups, b.fore_sqm_ups, b.inr_sqm_ups, b.ir_sqm_ups, 
	b.mo_sqm_ups, b.pas_sqm_ups, b.tci_sqm_ups, b.tct_sqm_ups, b.tg_sqm_ups, 
	b.wat_sqm_ups, b.wlf_sqm_ups, b.wlo_sqm_ups, b.wlt_sqm_ups 
	from analysissusqu.cache_final_lateralshed_04 as a
	left join datasusqu.nhdplus_luproportions_rerun as b
	on a.nord = b.nord 
      ) as t2
on t1.nord = t2.nord
group by t2.gnis_name,t1.nord, t2.nordstop, t2.comid, t2.huc12, t2.flow_cfs
	,t1.tn_lbyr_ups,t1.tp_lbyr_ups,t1.tss_lbyr_ups,t1.tn_lbyr_surfaceload_ups,t1.tp_lbyr_surfaceload_ups,t1.tss_lbyr_surfaceload_ups
	,t2.sqm_ups,t2.crp_sqm_ups,t2.fore_sqm_ups,t2.inr_sqm_ups,t2.ir_sqm_ups
	,t2.mo_sqm_ups,t2.pas_sqm_ups,t2.tci_sqm_ups,t2.tct_sqm_ups,t2.tg_sqm_ups
	,t2.wat_sqm_ups,t2.wlf_sqm_ups,t2.wlo_sqm_ups,t2.wlt_sqm_ups
	,t2.geom,t2.catchment
;

/* CACHE THE TABLE */

drop table if exists analysissusqu.cache_final_watershed_03;
create table analysissusqu.cache_final_watershed_03
as
select * from analysissusqu.final_watershed_03;

select * from analysissusqu.cache_final_watershed_03 where nord is null;

alter table analysissusqu.cache_final_watershed_03 add constraint pk_cache_final_watershed_03 primary key (nord);

CREATE INDEX cache_final_watershed_03_geom_idx
  ON analysissusqu.cache_final_watershed_03
  USING gist
  (catchment);

CREATE INDEX cache_final_watershed_03_nord_idx
  ON analysissusqu.cache_final_watershed_03
  USING btree
  (nord);

CREATE INDEX cache_final_watershed_03_nordnordstop_idx
  ON analysissusqu.cache_final_watershed_03
  USING btree
  (nord,nordstop);

GRANT SELECT ON analysissusqu.cache_final_watershed_03 TO selectrole;

select sum(st_area(catchment)/2590000)
from analysissusqu.cache_final_lateralshed_04;
-- ~ 65,500 square miles
select sum(st_area(catchment)/2590000)
from datasusqu.z_temp_srat_comparison;
-- ~ 14,550 square miles
select count(*) from analysissusqu.cache_final_lateralshed_04;
select count(*) from analysissusqu.cache_final_watershed_03 ;

------------------------------------------------------------------------------------------------------------------------------

drop table if exists analysissusqu.cache_final_watershed_04;
create table analysissusqu.cache_final_watershed_04
as
select * from analysissusqu.final_watershed_04;

--select * from analysissusqu.cache_final_watershed_04 where nord is null;

alter table analysissusqu.cache_final_watershed_04 add constraint pk_cache_final_watershed_04 primary key (nord);

CREATE INDEX cache_final_watershed_04_geom_idx
  ON analysissusqu.cache_final_watershed_04
  USING gist
  (catchment);

CREATE INDEX cache_final_watershed_04_nord_idx
  ON analysissusqu.cache_final_watershed_04
  USING btree
  (nord);

CREATE INDEX cache_final_watershed_04_nordnordstop_idx
  ON analysissusqu.cache_final_watershed_04
  USING btree
  (nord,nordstop);

GRANT SELECT ON analysissusqu.cache_final_watershed_04 TO selectrole;

select sum(st_area(catchment)/2590000)
from analysissusqu.cache_final_lateralshed_04;
-- ~ 65,500 square miles
select sum(st_area(catchment)/2590000)
from datasusqu.z_temp_srat_comparison;
-- ~ 14,550 square miles
select count(*) from analysissusqu.cache_final_lateralshed_04;
select count(*) from analysissusqu.cache_final_watershed_04 ;











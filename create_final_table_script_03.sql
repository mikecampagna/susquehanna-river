
-- adding in lb/yr AS it may be useful later
-- this version will also CREATE the watershed calculations

DROP VIEW IF EXISTS analysissusqu.final_lateralshed_04;
CREATE VIEW analysissusqu.final_lateralshed_04 

AS

SELECT nhd.gnis_name
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
,1 - (((lu_nhd.wat_sum + lu_nhd.wlf_sum + lu_nhd.wlo_sum + lu_nhd.wlt_sum2) / lu_nhd.tot_cells) * 0.12) AS tn_extict_coef
,1 - (((lu_nhd.wat_sum + lu_nhd.wlf_sum + lu_nhd.wlo_sum + lu_nhd.wlt_sum2) / lu_nhd.tot_cells) * 0.29) AS tp_extict_coef
,1 - (((lu_nhd.wat_sum + lu_nhd.wlf_sum + lu_nhd.wlo_sum + lu_nhd.wlt_sum2) / lu_nhd.tot_cells) * 0.84) AS tss_extict_coef
,t2.*
,nhd.geom
		
from
(
select	t1.comid
,	nhd.maflowv						as flow_cfs	

	/*
,	sum(CASE WHEN t1.tn_crp_lbacre is null THEN 0 ESLE t1.tn_crp_lbacre end)		 								as tn_crp_lbacre
,	sum(CASE WHEN t1.tp_crp_lbacre is null THEN 0 ESLE t1.tp_crp_lbacre end)										as tp_crp_lbacre
,	sum(CASE WHEN t1.tss_crp_lbacre is null THEN 0 ESLE t1.tss_crp_lbacre end)		 								as tss_crp_lbacre
,	sum(CASE WHEN t1.tn_crp_lbyr is null THEN 0 ESLE t1.tn_crp_lbyr end)		 									as tn_crp_lbyr
,	sum(CASE WHEN t1.tp_crp_lbyr is null THEN 0 ESLE t1.tp_crp_lbyr end)											as tp_crp_lbyr
,	sum(CASE WHEN t1.tss_crp_lbyr is null THEN 0 ESLE t1.tss_crp_lbyr end)		 									as tss_crp_lbyr
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tn_crp_mgl is null THEN 0 ESLE t1.tn_crp_mgl end) end			 		as tn_crp_mgl
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tp_crp_mgl is null THEN 0 ESLE t1.tp_crp_mgl end) end			 		as tp_crp_mgl
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tss_crp_mgl is null THEN 0 ESLE t1.tss_crp_mgl end) end					as tss_crp_mgl

,	sum(CASE WHEN t1.tn_fore_lbacre is null THEN 0 ESLE t1.tn_fore_lbacre end)		 								as tn_fore_lbacre
,	sum(CASE WHEN t1.tp_fore_lbacre is null THEN 0 ESLE t1.tp_fore_lbacre end)		 								as tp_fore_lbacre
,	sum(CASE WHEN t1.tss_fore_lbacre is null THEN 0 ESLE t1.tss_fore_lbacre end)		 								as tss_fore_lbacre
,	sum(CASE WHEN t1.tn_fore_lbyr is null THEN 0 ESLE t1.tn_fore_lbyr end)		 									as tn_fore_lbyr
,	sum(CASE WHEN t1.tp_fore_lbyr is null THEN 0 ESLE t1.tp_fore_lbyr end)		 									as tp_fore_lbyr
,	sum(CASE WHEN t1.tss_fore_lbyr is null THEN 0 ESLE t1.tss_fore_lbyr end)		 								as tss_fore_lbyr
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tn_fore_mgl is null THEN 0 ESLE t1.tn_fore_mgl end) end					as tn_fore_mgl
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tp_fore_mgl is null THEN 0 ESLE t1.tp_fore_mgl end) end					as tp_fore_mgl
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tss_fore_mgl is null THEN 0 ESLE t1.tss_fore_mgl end) end					as tss_fore_mgl

,	sum(CASE WHEN t1.tn_inr_lbacre is null THEN 0 ESLE t1.tn_inr_lbacre end)		 								as tn_inr_lbacre
,	sum(CASE WHEN t1.tp_inr_lbacre is null THEN 0 ESLE t1.tp_inr_lbacre end)		 								as tp_inr_lbacre
,	sum(CASE WHEN t1.tss_inr_lbacre is null THEN 0 ESLE t1.tss_inr_lbacre end)		 								as tss_inr_lbacre
,	sum(CASE WHEN t1.tn_inr_lbyr is null THEN 0 ESLE t1.tn_inr_lbyr end)		 									as tn_inr_lbyr
,	sum(CASE WHEN t1.tp_inr_lbyr is null THEN 0 ESLE t1.tp_inr_lbyr end)		 									as tp_inr_lbyr
,	sum(CASE WHEN t1.tss_inr_lbyr is null THEN 0 ESLE t1.tss_inr_lbyr end)		 									as tss_inr_lbyr
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tn_inr_mgl is null THEN 0 ESLE t1.tn_inr_mgl end) end				 	as tn_inr_mgl
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tp_inr_mgl is null THEN 0 ESLE t1.tp_inr_mgl end) end				 	as tp_inr_mgl
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tss_inr_mgl is null THEN 0 ESLE t1.tss_inr_mgl end) end					as tss_inr_mgl

,	sum(CASE WHEN t1.tn_ir_lbacre is null THEN 0 ESLE t1.tn_ir_lbacre end)		 									as tn_ir_lbacre
,	sum(CASE WHEN t1.tp_ir_lbacre is null THEN 0 ESLE t1.tp_ir_lbacre end)		 									as tp_ir_lbacre
,	sum(CASE WHEN t1.tss_ir_lbacre is null THEN 0 ESLE t1.tss_ir_lbacre end)		 								as tss_ir_lbacre
,	sum(CASE WHEN t1.tn_ir_lbyr is null THEN 0 ESLE t1.tn_ir_lbyr end)		 									as tn_ir_lbyr
,	sum(CASE WHEN t1.tp_ir_lbyr is null THEN 0 ESLE t1.tp_ir_lbyr end)		 									as tp_ir_lbyr
,	sum(CASE WHEN t1.tss_ir_lbyr is null THEN 0 ESLE t1.tss_ir_lbyr end)		 									as tss_ir_lbyr
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tn_ir_mgl is null THEN 0 ESLE t1.tn_ir_mgl end) end		 				as tn_ir_mgl
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tp_ir_mgl is null THEN 0 ESLE t1.tp_ir_mgl end) end		 				as tp_ir_mgl
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tss_ir_mgl is null THEN 0 ESLE t1.tss_ir_mgl end) end		 			as tss_ir_mgl

,	sum(CASE WHEN t1.tn_mo_lbacre is null THEN 0 ESLE t1.tn_mo_lbacre end)		 									as tn_mo_lbacre
,	sum(CASE WHEN t1.tp_mo_lbacre is null THEN 0 ESLE t1.tp_mo_lbacre end)		 									as tp_mo_lbacre
,	sum(CASE WHEN t1.tss_mo_lbacre is null THEN 0 ESLE t1.tss_mo_lbacre end)		 								as tss_mo_lbacre
,	sum(CASE WHEN t1.tn_mo_lbyr is null THEN 0 ESLE t1.tn_mo_lbyr end)		 									as tn_mo_lbyr
,	sum(CASE WHEN t1.tp_mo_lbyr is null THEN 0 ESLE t1.tp_mo_lbyr end)		 									as tp_mo_lbyr
,	sum(CASE WHEN t1.tss_mo_lbyr is null THEN 0 ESLE t1.tss_mo_lbyr end)		 									as tss_mo_lbyr
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tn_mo_mgl is null THEN 0 ESLE t1.tn_mo_mgl end) end		 				as tn_mo_mgl
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tp_mo_mgl is null THEN 0 ESLE t1.tp_mo_mgl end) end		 				as tp_mo_mgl
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tss_mo_mgl is null THEN 0 ESLE t1.tss_mo_mgl end) end			 		as tss_mo_mgl

,	sum(CASE WHEN t1.tn_pas_lbacre is null THEN 0 ESLE t1.tn_pas_lbacre end)		 								as tn_pas_lbacre
,	sum(CASE WHEN t1.tp_pas_lbacre is null THEN 0 ESLE t1.tp_pas_lbacre end)		 								as tp_pas_lbacre
,	sum(CASE WHEN t1.tss_pas_lbacre is null THEN 0 ESLE t1.tss_pas_lbacre end)		 								as tss_pas_lbacre
,	sum(CASE WHEN t1.tn_pas_lbyr is null THEN 0 ESLE t1.tn_pas_lbyr end)		 									as tn_pas_lbyr
,	sum(CASE WHEN t1.tp_pas_lbyr is null THEN 0 ESLE t1.tp_pas_lbyr end)		 									as tp_pas_lbyr
,	sum(CASE WHEN t1.tss_pas_lbyr is null THEN 0 ESLE t1.tss_pas_lbyr end)		 									as tss_pas_lbyr
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tn_pas_mgl is null THEN 0 ESLE t1.tn_pas_mgl end) end			 		as tn_pas_mgl
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tp_pas_mgl is null THEN 0 ESLE t1.tp_pas_mgl end) end			 		as tp_pas_mgl
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tss_pas_mgl is null THEN 0 ESLE t1.tss_pas_mgl end) end					as tss_pas_mgl

,	sum(CASE WHEN t1.tn_tci_lbacre is null THEN 0 ESLE t1.tn_tci_lbacre end)		 								as tn_tci_lbacre
,	sum(CASE WHEN t1.tp_tci_lbacre is null THEN 0 ESLE t1.tp_tci_lbacre end)		 								as tp_tci_lbacre
,	sum(CASE WHEN t1.tss_tci_lbacre is null THEN 0 ESLE t1.tss_tci_lbacre end)		 								as tss_tci_lbacre
,	sum(CASE WHEN t1.tn_tci_lbyr is null THEN 0 ESLE t1.tn_tci_lbyr end)		 									as tn_tci_lbyr
,	sum(CASE WHEN t1.tp_tci_lbyr is null THEN 0 ESLE t1.tp_tci_lbyr end)		 									as tp_tci_lbyr
,	sum(CASE WHEN t1.tss_tci_lbyr is null THEN 0 ESLE t1.tss_tci_lbyr end)		 									as tss_tci_lbyr
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tn_tci_mgl is null THEN 0 ESLE t1.tn_tci_mgl end) end		 			as tn_tci_mgl
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tp_tci_mgl is null THEN 0 ESLE t1.tp_tci_mgl end) end		 			as tp_tci_mgl
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tss_tci_mgl is null THEN 0 ESLE t1.tss_tci_mgl end) end					as tss_tci_mgl

,	sum(CASE WHEN t1.tn_tct_lbacre is null THEN 0 ESLE t1.tn_tct_lbacre end)		 								as tn_tct_lbacre
,	sum(CASE WHEN t1.tp_tct_lbacre is null THEN 0 ESLE t1.tp_tct_lbacre end)		 								as tp_tct_lbacre
,	sum(CASE WHEN t1.tss_tct_lbacre is null THEN 0 ESLE t1.tss_tct_lbacre end)		 								as tss_tct_lbacre
,	sum(CASE WHEN t1.tn_tct_lbyr is null THEN 0 ESLE t1.tn_tct_lbyr end)		 									as tn_tct_lbyr
,	sum(CASE WHEN t1.tp_tct_lbyr is null THEN 0 ESLE t1.tp_tct_lbyr end)		 									as tp_tct_lbyr
,	sum(CASE WHEN t1.tss_tct_lbyr is null THEN 0 ESLE t1.tss_tct_lbyr end)		 									as tss_tct_lbyr
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tn_tct_mgl is null THEN 0 ESLE t1.tn_tct_mgl end) end		 			as tn_tct_mgl
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tp_tct_mgl is null THEN 0 ESLE t1.tp_tct_mgl end) end		 			as tp_tct_mgl
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tss_tct_mgl is null THEN 0 ESLE t1.tss_tct_mgl end) end					as tss_tct_mgl

,	sum(CASE WHEN t1.tn_tg_lbacre is null THEN 0 ESLE t1.tn_tg_lbacre end)		 									as tn_tg_lbacre
,	sum(CASE WHEN t1.tp_tg_lbacre is null THEN 0 ESLE t1.tp_tg_lbacre end)		 									as tp_tg_lbacre
,	sum(CASE WHEN t1.tss_tg_lbacre is null THEN 0 ESLE t1.tss_tg_lbacre end)		 								as tss_tg_lbacre
,	sum(CASE WHEN t1.tn_tg_lbyr is null THEN 0 ESLE t1.tn_tg_lbyr end)		 									as tn_tg_lbyr
,	sum(CASE WHEN t1.tp_tg_lbyr is null THEN 0 ESLE t1.tp_tg_lbyr end)		 									as tp_tg_lbyr
,	sum(CASE WHEN t1.tss_tg_lbyr is null THEN 0 ESLE t1.tss_tg_lbyr end)		 									as tss_tg_lbyr
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tn_tg_mgl is null THEN 0 ESLE t1.tn_tg_mgl end) end		 				as tn_tg_mgl
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tp_tg_mgl is null THEN 0 ESLE t1.tp_tg_mgl end) end		 				as tp_tg_mgl
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tss_tg_mgl is null THEN 0 ESLE t1.tss_tg_mgl end) end		 			as tss_tg_mgl

,	sum(CASE WHEN t1.tn_wat_lbacre is null THEN 0 ESLE t1.tn_wat_lbacre end)		 								as tn_wat_lbacre
,	sum(CASE WHEN t1.tp_wat_lbacre is null THEN 0 ESLE t1.tp_wat_lbacre end)		 								as tp_wat_lbacre
,	sum(CASE WHEN t1.tss_wat_lbacre is null THEN 0 ESLE t1.tss_wat_lbacre end)		 								as tss_wat_lbacre
,	sum(CASE WHEN t1.tn_wat_lbyr is null THEN 0 ESLE t1.tn_wat_lbyr end)		 									as tn_wat_lbyr
,	sum(CASE WHEN t1.tp_wat_lbyr is null THEN 0 ESLE t1.tp_wat_lbyr end)		 									as tp_wat_lbyr
,	sum(CASE WHEN t1.tss_wat_lbyr is null THEN 0 ESLE t1.tss_wat_lbyr end)		 									as tss_wat_lbyr
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tn_wat_mgl is null THEN 0 ESLE t1.tn_wat_mgl end) end		 			as tn_wat_mgl
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tp_wat_mgl is null THEN 0 ESLE t1.tp_wat_mgl end) end		 			as tp_wat_mgl
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tss_wat_mgl is null THEN 0 ESLE t1.tss_wat_mgl end) end					as tss_wat_mgl

,	sum(CASE WHEN t1.tn_wlf_lbacre is null THEN 0 ESLE t1.tn_wlf_lbacre end)		 								as tn_wlf_lbacre
,	sum(CASE WHEN t1.tp_wlf_lbacre is null THEN 0 ESLE t1.tp_wlf_lbacre end)		 								as tp_wlf_lbacre
,	sum(CASE WHEN t1.tss_wlf_lbacre is null THEN 0 ESLE t1.tss_wlf_lbacre end)		 								as tss_wlf_lbacre
,	sum(CASE WHEN t1.tn_wlf_lbyr is null THEN 0 ESLE t1.tn_wlf_lbyr end)		 									as tn_wlf_lbyr
,	sum(CASE WHEN t1.tp_wlf_lbyr is null THEN 0 ESLE t1.tp_wlf_lbyr end)		 									as tp_wlf_lbyr
,	sum(CASE WHEN t1.tss_wlf_lbyr is null THEN 0 ESLE t1.tss_wlf_lbyr end)		 									as tss_wlf_lbyr
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tn_wlf_mgl is null THEN 0 ESLE t1.tn_wlf_mgl end) end		 			as tn_wlf_mgl
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tp_wlf_mgl is null THEN 0 ESLE t1.tp_wlf_mgl end) end		 			as tp_wlf_mgl
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tss_wlf_mgl is null THEN 0 ESLE t1.tss_wlf_mgl end) end					as tss_wlf_mgl

,	sum(CASE WHEN t1.tn_wlo_lbacre is null THEN 0 ESLE t1.tn_wlo_lbacre end)		 								as tn_wlo_lbacre
,	sum(CASE WHEN t1.tp_wlo_lbacre is null THEN 0 ESLE t1.tp_wlo_lbacre end)		 								as tp_wlo_lbacre
,	sum(CASE WHEN t1.tss_wlo_lbacre is null THEN 0 ESLE t1.tss_wlo_lbacre end)		 								as tss_wlo_lbacre
,	sum(CASE WHEN t1.tn_wlo_lbyr is null THEN 0 ESLE t1.tn_wlo_lbyr end)		 									as tn_wlo_lbyr
,	sum(CASE WHEN t1.tp_wlo_lbyr is null THEN 0 ESLE t1.tp_wlo_lbyr end)		 									as tp_wlo_lbyr
,	sum(CASE WHEN t1.tss_wlo_lbyr is null THEN 0 ESLE t1.tss_wlo_lbyr end)		 									as tss_wlo_lbyr
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tn_wlo_mgl is null THEN 0 ESLE t1.tn_wlo_mgl end) end		 			as tn_wlo_mgl
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tp_wlo_mgl is null THEN 0 ESLE t1.tp_wlo_mgl end) end		 			as tp_wlo_mgl
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tss_wlo_mgl is null THEN 0 ESLE t1.tss_wlo_mgl end) end					as tss_wlo_mgl

,	sum(CASE WHEN t1.tn_construction_lbacre is null THEN 0 ESLE t1.tn_construction_lbacre end)		 						as tn_construction_lbacre
,	sum(CASE WHEN t1.tp_construction_lbacre is null THEN 0 ESLE t1.tp_construction_lbacre end)		 						as tp_construction_lbacre
,	sum(CASE WHEN t1.tss_construction_lbacre is null THEN 0 ESLE t1.tss_construction_lbacre end)		 						as tss_construction_lbacre
,	sum(CASE WHEN t1.tn_construction_lbyr is null THEN 0 ESLE t1.tn_construction_lbyr end)		 							as tn_construction_lbyr
,	sum(CASE WHEN t1.tp_construction_lbyr is null THEN 0 ESLE t1.tp_construction_lbyr end)		 							as tp_construction_lbyr
,	sum(CASE WHEN t1.tss_construction_lbyr is null THEN 0 ESLE t1.tss_construction_lbyr end)		 						as tss_construction_lbyr
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tn_construction_mgl is null THEN 0 ESLE t1.tn_construction_mgl end) end			as tn_construction_mgl
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tp_construction_mgl is null THEN 0 ESLE t1.tp_construction_mgl end) end			as tp_construction_mgl
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tss_construction_mgl is null THEN 0 ESLE t1.tss_construction_mgl end) end			as tss_construction_mgl

,	sum(CASE WHEN t1.tn_rpd_lbacre is null THEN 0 ESLE t1.tn_rpd_lbacre end)		 								as tn_rpd_lbacre
,	sum(CASE WHEN t1.tp_rpd_lbacre is null THEN 0 ESLE t1.tp_rpd_lbacre end)		 								as tp_rpd_lbacre
,	sum(CASE WHEN t1.tss_rpd_lbacre is null THEN 0 ESLE t1.tss_rpd_lbacre end)		 								as tss_rpd_lbacre
,	sum(CASE WHEN t1.tn_rpd_lbyr is null THEN 0 ESLE t1.tn_rpd_lbyr end)		 									as tn_rpd_lbyr
,	sum(CASE WHEN t1.tp_rpd_lbyr is null THEN 0 ESLE t1.tp_rpd_lbyr end)		 									as tp_rpd_lbyr
,	sum(CASE WHEN t1.tss_rpd_lbyr is null THEN 0 ESLE t1.tss_rpd_lbyr end)		 									as tss_rpd_lbyr
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tn_rpd_mgl is null THEN 0 ESLE t1.tn_rpd_mgl end) end		 			as tn_rpd_mgl
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tp_rpd_mgl is null THEN 0 ESLE t1.tp_rpd_mgl end) end		 			as tp_rpd_mgl
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tss_rpd_mgl is null THEN 0 ESLE t1.tss_rpd_mgl end) end					as tss_rpd_mgl

,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tn_shore_lbkm is null THEN 0 ESLE t1.tn_shore_lbkm end) end		 			as tn_shore_lbkm
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tp_shore_lbkm is null THEN 0 ESLE t1.tp_shore_lbkm end) end		 			as tp_shore_lbkm
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tss_shore_lbkm is null THEN 0 ESLE t1.tss_shore_lbkm end) end		 		as tss_shore_lbkm
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tn_shorelbyr is null THEN 0 ESLE t1.tn_shorelbyr end) end		 			as tn_shorelbyr
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tp_shorelbyr is null THEN 0 ESLE t1.tp_shorelbyr end) end		 			as tp_shorelbyr
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tss_shorelbyr is null THEN 0 ESLE t1.tss_shorelbyr end) end			 		as tss_shorelbyr
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tn_shore_mgl is null THEN 0 ESLE t1.tn_shore_mgl end) end		 			as tn_shore_mgl
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tp_shore_mgl is null THEN 0 ESLE t1.tp_shore_mgl end) end		 			as tp_shore_mgl
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tss_shore_mgl is null THEN 0 ESLE t1.tss_shore_mgl end) end		 			as tss_shore_mgl

,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tn_streambnb_lbkm is null THEN 0 ESLE t1.tn_streambnb_lbkm end) end		 		as tn_streambnb_lbkm
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tp_streambnb_lbkm is null THEN 0 ESLE t1.tp_streambnb_lbkm end) end		 		as tp_streambnb_lbkm
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tss_streambnb_lbkm is null THEN 0 ESLE t1.tss_streambnb_lbkm end) end		 	as tss_streambnb_lbkm
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tn_streambnblbyr is null THEN 0 ESLE t1.tn_streambnblbyr end) end		 		as tn_streambnblbyr
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tp_streambnblbyr is null THEN 0 ESLE t1.tp_streambnblbyr end) end		 		as tp_streambnblbyr
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tss_streambnblbyr is null THEN 0 ESLE t1.tss_streambnblbyr end) end		 		as tss_streambnblbyr
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tn_streambnb_mgl is null THEN 0 ESLE t1.tn_streambnb_mgl end) end		 		as tn_streambnb_mgl
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tp_streambnb_mgl is null THEN 0 ESLE t1.tp_streambnb_mgl end) end		 		as tp_streambnb_mgl
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tss_streambnb_mgl is null THEN 0 ESLE t1.tss_streambnb_mgl end) end		 		as tss_streambnb_mgl

,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tn_streamfp_lbkm is null THEN 0 ESLE t1.tn_streamfp_lbkm end) end		 		as tn_streamfp_lbkm
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tp_streamfp_lbkm is null THEN 0 ESLE t1.tp_streamfp_lbkm end) end				as tp_streamfp_lbkm
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tss_streamfp_lbkm is null THEN 0 ESLE t1.tss_streamfp_lbkm end) end		 		as tss_streamfp_lbkm
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tn_streamfplbyr is null THEN 0 ESLE t1.tn_streamfplbyr end) end		 		as tn_streamfplbyr
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tp_streamfplbyr is null THEN 0 ESLE t1.tp_streamfplbyr end) end				as tp_streamfplbyr
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tss_streamfplbyr is null THEN 0 ESLE t1.tss_streamfplbyr end) end		 		as tss_streamfplbyr
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tn_streamfp_mgl is null THEN 0 ESLE t1.tn_streamfp_mgl end) end		 		as tn_streamfp_mgl
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tp_streamfp_mgl is null THEN 0 ESLE t1.tp_streamfp_mgl end) end		 		as tp_streamfp_mgl
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tss_streamfp_mgl is null THEN 0 ESLE t1.tss_streamfp_mgl end) end		 		as tss_streamfp_mgl

,	sum(CASE WHEN t1.tn_cso_areaonly_lbacre is null THEN 0 ESLE t1.tn_cso_areaonly_lbacre end)		 						as tn_cso_areaonly_lbacre
,	sum(CASE WHEN t1.tp_cso_areaonly_lbacre is null THEN 0 ESLE t1.tp_cso_areaonly_lbacre end)		 						as tp_cso_areaonly_lbacre
,	sum(CASE WHEN t1.tss_cso_areaonly_lbacre is null THEN 0 ESLE t1.tss_cso_areaonly_lbacre end)		 						as tss_cso_areaonly_lbacre
,	sum(CASE WHEN t1.tn_cso_areaonly_lbyr is null THEN 0 ESLE t1.tn_cso_areaonly_lbyr end)		 							as tn_cso_areaonly_lbyr
,	sum(CASE WHEN t1.tp_cso_areaonly_lbyr is null THEN 0 ESLE t1.tp_cso_areaonly_lbyr end)		 							as tp_cso_areaonly_lbyr
,	sum(CASE WHEN t1.tss_cso_areaonly_lbyr is null THEN 0 ESLE t1.tss_cso_areaonly_lbyr end)		 						as tss_cso_areaonly_lbyr
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tn_cso_areaonly_mgl is null THEN 0 ESLE t1.tn_cso_areaonly_mgl end) end			as tn_cso_areaonly_mgl
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tp_cso_areaonly_mgl is null THEN 0 ESLE t1.tp_cso_areaonly_mgl end) end			as tp_cso_areaonly_mgl
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tss_cso_areaonly_mgl is null THEN 0 ESLE t1.tss_cso_areaonly_mgl end) end			as tss_cso_areaonly_mgl

,	sum(CASE WHEN t1.tn_wwi_lbacre is null THEN 0 ESLE t1.tn_wwi_lbacre end)		 								as tn_wwi_lbacre
,	sum(CASE WHEN t1.tp_wwi_lbacre is null THEN 0 ESLE t1.tp_wwi_lbacre end)		 								as tp_wwi_lbacre
,	sum(CASE WHEN t1.tss_wwi_lbacre is null THEN 0 ESLE t1.tss_wwi_lbacre end)		 								as tss_wwi_lbacre
,	sum(CASE WHEN t1.tn_wwi_lbyr is null THEN 0 ESLE t1.tn_wwi_lbyr end)		 									as tn_wwi_lbyr
,	sum(CASE WHEN t1.tp_wwi_lbyr is null THEN 0 ESLE t1.tp_wwi_lbyr end)		 									as tp_wwi_lbyr
,	sum(CASE WHEN t1.tss_wwi_lbyr is null THEN 0 ESLE t1.tss_wwi_lbyr end)		 									as tss_wwi_lbyr
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tn_wwi_mgl is null THEN 0 ESLE t1.tn_wwi_mgl end) end		 			as tn_wwi_mgl
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tp_wwi_mgl is null THEN 0 ESLE t1.tp_wwi_mgl end) end		 			as tp_wwi_mgl
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tss_wwi_mgl is null THEN 0 ESLE t1.tss_wwi_mgl end) end		 			as tss_wwi_mgl

,	sum(CASE WHEN t1.tn_wwm_lbacre is null THEN 0 ESLE t1.tn_wwm_lbacre end)		 								as tn_wwm_lbacre
,	sum(CASE WHEN t1.tp_wwm_lbacre is null THEN 0 ESLE t1.tp_wwm_lbacre end)		 								as tp_wwm_lbacre
,	sum(CASE WHEN t1.tss_wwm_lbacre is null THEN 0 ESLE t1.tss_wwm_lbacre end)		 								as tss_wwm_lbacre
,	sum(CASE WHEN t1.tn_wwm_lbyr is null THEN 0 ESLE t1.tn_wwm_lbyr end)		 									as tn_wwm_lbyr
,	sum(CASE WHEN t1.tp_wwm_lbyr is null THEN 0 ESLE t1.tp_wwm_lbyr end)		 									as tp_wwm_lbyr
,	sum(CASE WHEN t1.tss_wwm_lbyr is null THEN 0 ESLE t1.tss_wwm_lbyr end)		 									as tss_wwm_lbyr
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tn_wwm_mgl is null THEN 0 ESLE t1.tn_wwm_mgl end) end		 			as tn_wwm_mgl
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tp_wwm_mgl is null THEN 0 ESLE t1.tp_wwm_mgl end) end		 			as tp_wwm_mgl
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tss_wwm_mgl is null THEN 0 ESLE t1.tss_wwm_mgl end) end		 			as tss_wwm_mgl

,	sum(CASE WHEN t1.tn_rib_lbacre is null THEN 0 ESLE t1.tn_rib_lbacre end)		 								as tn_rib_lbacre
,	sum(CASE WHEN t1.tp_rib_lbacre is null THEN 0 ESLE t1.tp_rib_lbacre end)		 								as tp_rib_lbacre
,	sum(CASE WHEN t1.tn_rib_lbyr is null THEN 0 ESLE t1.tn_rib_lbyr end)		 									as tn_rib_lbyr
,	sum(CASE WHEN t1.tp_rib_lbyr is null THEN 0 ESLE t1.tp_rib_lbyr end)		 									as tp_rib_lbyr
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tn_rib_mgl is null THEN 0 ESLE t1.tn_rib_mgl end) end		 			as tn_rib_mgl
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tp_rib_mgl is null THEN 0 ESLE t1.tp_rib_mgl end) end		 			as tp_rib_mgl

,	sum(CASE WHEN t1.tn_sep_lbacre is null THEN 0 ESLE t1.tn_sep_lbacre end)		 								as tn_sep_lbacre
,	sum(CASE WHEN t1.tp_sep_lbacre is null THEN 0 ESLE t1.tp_sep_lbacre end)		 								as tp_sep_lbacre
,	sum(CASE WHEN t1.tn_sep_lbyr is null THEN 0 ESLE t1.tn_sep_lbyr end)		 									as tn_sep_lbyr
,	sum(CASE WHEN t1.tp_sep_lbyr is null THEN 0 ESLE t1.tp_sep_lbyr end)		 									as tp_sep_lbyr
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tn_sep_mgl is null THEN 0 ESLE t1.tn_sep_mgl end) end		 			as tn_sep_mgl
,	CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(CASE WHEN t1.tp_sep_mgl is null THEN 0 ESLE t1.tp_sep_mgl end) end		 			as tp_sep_mgl
	*/

,sum(	CASE WHEN t1.tn_crp_lbacre is null THEN 0 ESLE t1.tn_crp_lbyr end			
+	CASE WHEN t1.tn_fore_lbyr is null THEN 0 ESLE t1.tn_fore_lbyr end			
+	CASE WHEN t1.tn_inr_lbyr is null THEN 0 ESLE t1.tn_inr_lbyr end			
+	CASE WHEN t1.tn_ir_lbyr is null THEN 0 ESLE t1.tn_ir_lbyr end			
+	CASE WHEN t1.tn_mo_lbyr is null THEN 0 ESLE t1.tn_mo_lbyr end			
+	CASE WHEN t1.tn_pas_lbyr is null THEN 0 ESLE t1.tn_pas_lbyr end			
+	CASE WHEN t1.tn_tci_lbyr is null THEN 0 ESLE t1.tn_tci_lbyr end			
+	CASE WHEN t1.tn_tct_lbyr is null THEN 0 ESLE t1.tn_tct_lbyr end			
+	CASE WHEN t1.tn_tg_lbyr is null THEN 0 ESLE t1.tn_tg_lbyr end			
+	CASE WHEN t1.tn_wat_lbyr is null THEN 0 ESLE t1.tn_wat_lbyr end			
+	CASE WHEN t1.tn_wlf_lbyr is null THEN 0 ESLE t1.tn_wlf_lbyr end			
+	CASE WHEN t1.tn_wlo_lbyr is null THEN 0 ESLE t1.tn_wlo_lbyr end			
+	CASE WHEN t1.tn_construction_lbyr is null THEN 0 ESLE t1.tn_construction_lbyr end			
+	CASE WHEN t1.tn_rpd_lbyr is null THEN 0 ESLE t1.tn_rpd_lbyr end			
+	CASE WHEN t1.tn_shore_lbyr is null THEN 0 ESLE t1.tn_shore_lbyr end			
+	CASE WHEN t1.tn_streambnb_lbyr is null THEN 0 ESLE t1.tn_streambnb_lbyr end			
+	CASE WHEN t1.tn_streamfp_lbyr is null THEN 0 ESLE t1.tn_streamfp_lbyr end			
+	CASE WHEN t1.tn_cso_areaonly_lbyr is null THEN 0 ESLE t1.tn_cso_areaonly_lbyr end			
+	CASE WHEN t1.tn_wwi_lbyr is null THEN 0 ESLE t1.tn_wwi_lbyr end			
+	CASE WHEN t1.tn_wwm_lbyr is null THEN 0 ESLE t1.tn_wwm_lbyr end			
+	CASE WHEN t1.tn_rib_lbyr is null THEN 0 ESLE t1.tn_rib_lbyr end			
+	CASE WHEN t1.tn_sep_lbyr is null THEN 0 ESLE t1.tn_sep_lbyr end	)					as	tn_lbyr
				
				
,sum(	CASE WHEN t1.tn_crp_lbyr is null THEN 0 ESLE t1.tn_crp_lbyr end			
+	CASE WHEN t1.tn_pas_lbyr is null THEN 0 ESLE t1.tn_pas_lbyr end			
+	CASE WHEN t1.tn_rpd_lbyr is null THEN 0 ESLE t1.tn_rpd_lbyr end	)					as	tn_ag_lbyr
				
				
,sum(	CASE WHEN t1.tn_inr_lbyr is null THEN 0 ESLE t1.tn_inr_lbyr end			
+	CASE WHEN t1.tn_ir_lbyr is null THEN 0 ESLE t1.tn_ir_lbyr end			
+	CASE WHEN t1.tn_construction_lbyr is null THEN 0 ESLE t1.tn_construction_lbyr end			
+	CASE WHEN t1.tn_tci_lbyr is null THEN 0 ESLE t1.tn_tci_lbyr end			
+	CASE WHEN t1.tn_tct_lbyr is null THEN 0 ESLE t1.tn_tct_lbyr end			
+	CASE WHEN t1.tn_tg_lbyr is null THEN 0 ESLE t1.tn_tg_lbyr end	)					as	tn_urb_lbyr
				
				
,sum(	CASE WHEN t1.tn_fore_lbyr is null THEN 0 ESLE t1.tn_fore_lbyr end			
+	CASE WHEN t1.tn_mo_lbyr is null THEN 0 ESLE t1.tn_mo_lbyr end			
+	CASE WHEN t1.tn_wat_lbyr is null THEN 0 ESLE t1.tn_wat_lbyr end			
+	CASE WHEN t1.tn_wlf_lbyr is null THEN 0 ESLE t1.tn_wlf_lbyr end			
+	CASE WHEN t1.tn_wlo_lbyr is null THEN 0 ESLE t1.tn_wlo_lbyr end			
+	CASE WHEN t1.tn_shore_lbyr is null THEN 0 ESLE t1.tn_shore_lbyr end			
+	CASE WHEN t1.tn_streambnb_lbyr is null THEN 0 ESLE t1.tn_streambnb_lbyr end			
+	CASE WHEN t1.tn_streamfp_lbyr is null THEN 0 ESLE t1.tn_streamfp_lbyr end	)				as	tn_nat_lbyr
				
				
,sum(	CASE WHEN t1.tn_cso_areaonly_lbyr is null THEN 0 ESLE t1.tn_cso_areaonly_lbyr end			
+	CASE WHEN t1.tn_wwi_lbyr is null THEN 0 ESLE t1.tn_wwi_lbyr end			
+	CASE WHEN t1.tn_wwm_lbyr is null THEN 0 ESLE t1.tn_wwm_lbyr end	)					as	tn_waste_lbyr
				
				
,sum(	CASE WHEN t1.tn_rib_lbyr is null THEN 0 ESLE t1.tn_rib_lbyr end			
+	CASE WHEN t1.tn_sep_lbyr is null THEN 0 ESLE t1.tn_sep_lbyr end	)					as	tn_septic_lbyr
				
				
,sum(	CASE WHEN t1.tp_crp_lbyr is null THEN 0 ESLE t1.tp_crp_lbyr end			
+	CASE WHEN t1.tp_fore_lbyr is null THEN 0 ESLE t1.tp_fore_lbyr end			
+	CASE WHEN t1.tp_inr_lbyr is null THEN 0 ESLE t1.tp_inr_lbyr end			
+	CASE WHEN t1.tp_ir_lbyr is null THEN 0 ESLE t1.tp_ir_lbyr end			
+	CASE WHEN t1.tp_mo_lbyr is null THEN 0 ESLE t1.tp_mo_lbyr end			
+	CASE WHEN t1.tp_pas_lbyr is null THEN 0 ESLE t1.tp_pas_lbyr end			
+	CASE WHEN t1.tp_tci_lbyr is null THEN 0 ESLE t1.tp_tci_lbyr end			
+	CASE WHEN t1.tp_tct_lbyr is null THEN 0 ESLE t1.tp_tct_lbyr end			
+	CASE WHEN t1.tp_tg_lbyr is null THEN 0 ESLE t1.tp_tg_lbyr end			
+	CASE WHEN t1.tp_wat_lbyr is null THEN 0 ESLE t1.tp_wat_lbyr end			
+	CASE WHEN t1.tp_wlf_lbyr is null THEN 0 ESLE t1.tp_wlf_lbyr end			
+	CASE WHEN t1.tp_wlo_lbyr is null THEN 0 ESLE t1.tp_wlo_lbyr end			
+	CASE WHEN t1.tp_construction_lbyr is null THEN 0 ESLE t1.tp_construction_lbyr end			
+	CASE WHEN t1.tp_rpd_lbyr is null THEN 0 ESLE t1.tp_rpd_lbyr end			
+	CASE WHEN t1.tp_shore_lbyr is null THEN 0 ESLE t1.tp_shore_lbyr end			
+	CASE WHEN t1.tp_streambnb_lbyr is null THEN 0 ESLE t1.tp_streambnb_lbyr end			
+	CASE WHEN t1.tp_streamfp_lbyr is null THEN 0 ESLE t1.tp_streamfp_lbyr end			
+	CASE WHEN t1.tp_cso_areaonly_lbyr is null THEN 0 ESLE t1.tp_cso_areaonly_lbyr end			
+	CASE WHEN t1.tp_wwi_lbyr is null THEN 0 ESLE t1.tp_wwi_lbyr end			
+	CASE WHEN t1.tp_wwm_lbyr is null THEN 0 ESLE t1.tp_wwm_lbyr end			
+	CASE WHEN t1.tp_rib_lbyr is null THEN 0 ESLE t1.tp_rib_lbyr end			
+	CASE WHEN t1.tp_sep_lbyr is null THEN 0 ESLE t1.tp_sep_lbyr end	)					as	tp_lbyr
				
				
,sum(	CASE WHEN t1.tp_crp_lbyr is null THEN 0 ESLE t1.tp_crp_lbyr end			
+	CASE WHEN t1.tp_pas_lbyr is null THEN 0 ESLE t1.tp_pas_lbyr end			
+	CASE WHEN t1.tp_rpd_lbyr is null THEN 0 ESLE t1.tp_rpd_lbyr end	)					as	tp_ag_lbyr
				
				
,sum(	CASE WHEN t1.tp_inr_lbyr is null THEN 0 ESLE t1.tp_inr_lbyr end			
+	CASE WHEN t1.tp_ir_lbyr is null THEN 0 ESLE t1.tp_ir_lbyr end			
+	CASE WHEN t1.tp_construction_lbyr is null THEN 0 ESLE t1.tp_construction_lbyr end			
+	CASE WHEN t1.tp_tci_lbyr is null THEN 0 ESLE t1.tp_tci_lbyr end			
+	CASE WHEN t1.tp_tct_lbyr is null THEN 0 ESLE t1.tp_tct_lbyr end			
+	CASE WHEN t1.tp_tg_lbyr is null THEN 0 ESLE t1.tp_tg_lbyr end	)					as	tp_urb_lbyr
				
				
,sum(	CASE WHEN t1.tp_fore_lbyr is null THEN 0 ESLE t1.tp_fore_lbyr end			
+	CASE WHEN t1.tp_mo_lbyr is null THEN 0 ESLE t1.tp_mo_lbyr end			
+	CASE WHEN t1.tp_wat_lbyr is null THEN 0 ESLE t1.tp_wat_lbyr end			
+	CASE WHEN t1.tp_wlf_lbyr is null THEN 0 ESLE t1.tp_wlf_lbyr end			
+	CASE WHEN t1.tp_wlo_lbyr is null THEN 0 ESLE t1.tp_wlo_lbyr end			
+	CASE WHEN t1.tp_shore_lbyr is null THEN 0 ESLE t1.tp_shore_lbyr end			
+	CASE WHEN t1.tp_streambnb_lbyr is null THEN 0 ESLE t1.tp_streambnb_lbyr end			
+	CASE WHEN t1.tp_streamfp_lbyr is null THEN 0 ESLE t1.tp_streamfp_lbyr end	)				as	tp_nat_lbyr
				
				
,sum(	CASE WHEN t1.tp_cso_areaonly_lbyr is null THEN 0 ESLE t1.tp_cso_areaonly_lbyr end			
+	CASE WHEN t1.tp_wwi_lbyr is null THEN 0 ESLE t1.tp_wwi_lbyr end			
+	CASE WHEN t1.tp_wwm_lbyr is null THEN 0 ESLE t1.tp_wwm_lbyr end	)					as	tp_waste_lbyr
				
				
,sum(	CASE WHEN t1.tp_rib_lbyr is null THEN 0 ESLE t1.tp_rib_lbyr end			
+	CASE WHEN t1.tp_sep_lbyr is null THEN 0 ESLE t1.tp_sep_lbyr end	)					as	tp_septic_lbyr
				
				
,sum(	CASE WHEN t1.tss_crp_lbyr is null THEN 0 ESLE t1.tss_crp_lbyr end			
+	CASE WHEN t1.tss_fore_lbyr is null THEN 0 ESLE t1.tss_fore_lbyr end			
+	CASE WHEN t1.tss_inr_lbyr is null THEN 0 ESLE t1.tss_inr_lbyr end			
+	CASE WHEN t1.tss_ir_lbyr is null THEN 0 ESLE t1.tss_ir_lbyr end			
+	CASE WHEN t1.tss_mo_lbyr is null THEN 0 ESLE t1.tss_mo_lbyr end			
+	CASE WHEN t1.tss_pas_lbyr is null THEN 0 ESLE t1.tss_pas_lbyr end			
+	CASE WHEN t1.tss_tci_lbyr is null THEN 0 ESLE t1.tss_tci_lbyr end			
+	CASE WHEN t1.tss_tct_lbyr is null THEN 0 ESLE t1.tss_tct_lbyr end			
+	CASE WHEN t1.tss_tg_lbyr is null THEN 0 ESLE t1.tss_tg_lbyr end			
+	CASE WHEN t1.tss_wat_lbyr is null THEN 0 ESLE t1.tss_wat_lbyr end			
+	CASE WHEN t1.tss_wlf_lbyr is null THEN 0 ESLE t1.tss_wlf_lbyr end			
+	CASE WHEN t1.tss_wlo_lbyr is null THEN 0 ESLE t1.tss_wlo_lbyr end			
+	CASE WHEN t1.tss_construction_lbyr is null THEN 0 ESLE t1.tss_construction_lbyr end			
+	CASE WHEN t1.tss_rpd_lbyr is null THEN 0 ESLE t1.tss_rpd_lbyr end			
+	CASE WHEN t1.tss_shore_lbyr is null THEN 0 ESLE t1.tss_shore_lbyr end			
+	CASE WHEN t1.tss_streambnb_lbyr is null THEN 0 ESLE t1.tss_streambnb_lbyr end			
+	CASE WHEN t1.tss_streamfp_lbyr is null THEN 0 ESLE t1.tss_streamfp_lbyr end			
+	CASE WHEN t1.tss_cso_areaonly_lbyr is null THEN 0 ESLE t1.tss_cso_areaonly_lbyr end			
+	CASE WHEN t1.tss_wwi_lbyr is null THEN 0 ESLE t1.tss_wwi_lbyr end			
+	CASE WHEN t1.tss_wwm_lbyr is null THEN 0 ESLE t1.tss_wwm_lbyr end	)					as	tss_lbyr
				
				
,sum(	CASE WHEN t1.tss_crp_lbyr is null THEN 0 ESLE t1.tss_crp_lbyr end			
+	CASE WHEN t1.tss_pas_lbyr is null THEN 0 ESLE t1.tss_pas_lbyr end			
+	CASE WHEN t1.tss_rpd_lbyr is null THEN 0 ESLE t1.tss_rpd_lbyr end	)					as	tss_ag_lbyr
				
				
,sum(	CASE WHEN t1.tss_inr_lbyr is null THEN 0 ESLE t1.tss_inr_lbyr end			
+	CASE WHEN t1.tss_ir_lbyr is null THEN 0 ESLE t1.tss_ir_lbyr end			
+	CASE WHEN t1.tss_construction_lbyr is null THEN 0 ESLE t1.tss_construction_lbyr end			
+	CASE WHEN t1.tss_tci_lbyr is null THEN 0 ESLE t1.tss_tci_lbyr end			
+	CASE WHEN t1.tss_tct_lbyr is null THEN 0 ESLE t1.tss_tct_lbyr end			
+	CASE WHEN t1.tss_tg_lbyr is null THEN 0 ESLE t1.tss_tg_lbyr end	)					as	tss_urb_lbyr
				
				
,sum(	CASE WHEN t1.tss_fore_lbyr is null THEN 0 ESLE t1.tss_fore_lbyr end			
+	CASE WHEN t1.tss_mo_lbyr is null THEN 0 ESLE t1.tss_mo_lbyr end			
+	CASE WHEN t1.tss_wat_lbyr is null THEN 0 ESLE t1.tss_wat_lbyr end			
+	CASE WHEN t1.tss_wlf_lbyr is null THEN 0 ESLE t1.tss_wlf_lbyr end			
+	CASE WHEN t1.tss_wlo_lbyr is null THEN 0 ESLE t1.tss_wlo_lbyr end			
+	CASE WHEN t1.tss_shore_lbyr is null THEN 0 ESLE t1.tss_shore_lbyr end			
+	CASE WHEN t1.tss_streambnb_lbyr is null THEN 0 ESLE t1.tss_streambnb_lbyr end			
+	CASE WHEN t1.tss_streamfp_lbyr is null THEN 0 ESLE t1.tss_streamfp_lbyr end	)				as	tss_nat_lbyr
				
				
,sum(	CASE WHEN t1.tss_cso_areaonly_lbyr is null THEN 0 ESLE t1.tss_cso_areaonly_lbyr end			
+	CASE WHEN t1.tss_wwi_lbyr is null THEN 0 ESLE t1.tss_wwi_lbyr end			
+	CASE WHEN t1.tss_wwm_lbyr is null THEN 0 ESLE t1.tss_wwm_lbyr end	)					as	tss_waste_lbyr
				

,sum(	CASE WHEN t1.tn_crp_lbacre is null THEN 0 ESLE t1.tn_crp_lbacre end			
+	CASE WHEN t1.tn_fore_lbacre is null THEN 0 ESLE t1.tn_fore_lbacre end			
+	CASE WHEN t1.tn_inr_lbacre is null THEN 0 ESLE t1.tn_inr_lbacre end			
+	CASE WHEN t1.tn_ir_lbacre is null THEN 0 ESLE t1.tn_ir_lbacre end			
+	CASE WHEN t1.tn_mo_lbacre is null THEN 0 ESLE t1.tn_mo_lbacre end			
+	CASE WHEN t1.tn_pas_lbacre is null THEN 0 ESLE t1.tn_pas_lbacre end			
+	CASE WHEN t1.tn_tci_lbacre is null THEN 0 ESLE t1.tn_tci_lbacre end			
+	CASE WHEN t1.tn_tct_lbacre is null THEN 0 ESLE t1.tn_tct_lbacre end			
+	CASE WHEN t1.tn_tg_lbacre is null THEN 0 ESLE t1.tn_tg_lbacre end			
+	CASE WHEN t1.tn_wat_lbacre is null THEN 0 ESLE t1.tn_wat_lbacre end			
+	CASE WHEN t1.tn_wlf_lbacre is null THEN 0 ESLE t1.tn_wlf_lbacre end			
+	CASE WHEN t1.tn_wlo_lbacre is null THEN 0 ESLE t1.tn_wlo_lbacre end			
+	CASE WHEN t1.tn_construction_lbacre is null THEN 0 ESLE t1.tn_construction_lbacre end			
										)					as	tn_lbacre

,sum(	CASE WHEN t1.tn_crp_lbyr is null THEN 0 ESLE t1.tn_crp_lbyr end			
+	CASE WHEN t1.tn_fore_lbyr is null THEN 0 ESLE t1.tn_fore_lbyr end			
+	CASE WHEN t1.tn_inr_lbyr is null THEN 0 ESLE t1.tn_inr_lbyr end			
+	CASE WHEN t1.tn_ir_lbyr is null THEN 0 ESLE t1.tn_ir_lbyr end			
+	CASE WHEN t1.tn_mo_lbyr is null THEN 0 ESLE t1.tn_mo_lbyr end			
+	CASE WHEN t1.tn_pas_lbyr is null THEN 0 ESLE t1.tn_pas_lbyr end			
+	CASE WHEN t1.tn_tci_lbyr is null THEN 0 ESLE t1.tn_tci_lbyr end			
+	CASE WHEN t1.tn_tct_lbyr is null THEN 0 ESLE t1.tn_tct_lbyr end			
+	CASE WHEN t1.tn_tg_lbyr is null THEN 0 ESLE t1.tn_tg_lbyr end			
+	CASE WHEN t1.tn_wat_lbyr is null THEN 0 ESLE t1.tn_wat_lbyr end			
+	CASE WHEN t1.tn_wlf_lbyr is null THEN 0 ESLE t1.tn_wlf_lbyr end			
+	CASE WHEN t1.tn_wlo_lbyr is null THEN 0 ESLE t1.tn_wlo_lbyr end			
+	CASE WHEN t1.tn_construction_lbyr is null THEN 0 ESLE t1.tn_construction_lbyr end			
										)					as	tn_lbyr_surfaceload			
				
,sum(	CASE WHEN t1.tn_crp_lbacre is null THEN 0 ESLE t1.tn_crp_lbacre end			
+	CASE WHEN t1.tn_pas_lbacre is null THEN 0 ESLE t1.tn_pas_lbacre end			
										)					as	tn_ag_lbacre
				
				
,sum(	CASE WHEN t1.tn_inr_lbacre is null THEN 0 ESLE t1.tn_inr_lbacre end			
+	CASE WHEN t1.tn_ir_lbacre is null THEN 0 ESLE t1.tn_ir_lbacre end			
+	CASE WHEN t1.tn_construction_lbacre is null THEN 0 ESLE t1.tn_construction_lbacre end			
+	CASE WHEN t1.tn_tci_lbacre is null THEN 0 ESLE t1.tn_tci_lbacre end			
+	CASE WHEN t1.tn_tct_lbacre is null THEN 0 ESLE t1.tn_tct_lbacre end			
+	CASE WHEN t1.tn_tg_lbacre is null THEN 0 ESLE t1.tn_tg_lbacre end	)					as	tn_urb_lbacre
				
				
,sum(	CASE WHEN t1.tn_fore_lbacre is null THEN 0 ESLE t1.tn_fore_lbacre end			
+	CASE WHEN t1.tn_mo_lbacre is null THEN 0 ESLE t1.tn_mo_lbacre end			
+	CASE WHEN t1.tn_wat_lbacre is null THEN 0 ESLE t1.tn_wat_lbacre end			
+	CASE WHEN t1.tn_wlf_lbacre is null THEN 0 ESLE t1.tn_wlf_lbacre end			
+	CASE WHEN t1.tn_wlo_lbacre is null THEN 0 ESLE t1.tn_wlo_lbacre end			
											)				as	tn_nat_lbacre
							
,sum(	CASE WHEN t1.tp_crp_lbacre is null THEN 0 ESLE t1.tp_crp_lbacre end			
+	CASE WHEN t1.tp_fore_lbacre is null THEN 0 ESLE t1.tp_fore_lbacre end			
+	CASE WHEN t1.tp_inr_lbacre is null THEN 0 ESLE t1.tp_inr_lbacre end			
+	CASE WHEN t1.tp_ir_lbacre is null THEN 0 ESLE t1.tp_ir_lbacre end			
+	CASE WHEN t1.tp_mo_lbacre is null THEN 0 ESLE t1.tp_mo_lbacre end			
+	CASE WHEN t1.tp_pas_lbacre is null THEN 0 ESLE t1.tp_pas_lbacre end			
+	CASE WHEN t1.tp_tci_lbacre is null THEN 0 ESLE t1.tp_tci_lbacre end			
+	CASE WHEN t1.tp_tct_lbacre is null THEN 0 ESLE t1.tp_tct_lbacre end			
+	CASE WHEN t1.tp_tg_lbacre is null THEN 0 ESLE t1.tp_tg_lbacre end			
+	CASE WHEN t1.tp_wat_lbacre is null THEN 0 ESLE t1.tp_wat_lbacre end			
+	CASE WHEN t1.tp_wlf_lbacre is null THEN 0 ESLE t1.tp_wlf_lbacre end			
+	CASE WHEN t1.tp_wlo_lbacre is null THEN 0 ESLE t1.tp_wlo_lbacre end			
+	CASE WHEN t1.tp_construction_lbacre is null THEN 0 ESLE t1.tp_construction_lbacre end			
										)					as	tp_lbacre
				
,sum(	CASE WHEN t1.tp_crp_lbyr is null THEN 0 ESLE t1.tp_crp_lbyr end			
+	CASE WHEN t1.tp_fore_lbyr is null THEN 0 ESLE t1.tp_fore_lbyr end			
+	CASE WHEN t1.tp_inr_lbyr is null THEN 0 ESLE t1.tp_inr_lbyr end			
+	CASE WHEN t1.tp_ir_lbyr is null THEN 0 ESLE t1.tp_ir_lbyr end			
+	CASE WHEN t1.tp_mo_lbyr is null THEN 0 ESLE t1.tp_mo_lbyr end			
+	CASE WHEN t1.tp_pas_lbyr is null THEN 0 ESLE t1.tp_pas_lbyr end			
+	CASE WHEN t1.tp_tci_lbyr is null THEN 0 ESLE t1.tp_tci_lbyr end			
+	CASE WHEN t1.tp_tct_lbyr is null THEN 0 ESLE t1.tp_tct_lbyr end			
+	CASE WHEN t1.tp_tg_lbyr is null THEN 0 ESLE t1.tp_tg_lbyr end			
+	CASE WHEN t1.tp_wat_lbyr is null THEN 0 ESLE t1.tp_wat_lbyr end			
+	CASE WHEN t1.tp_wlf_lbyr is null THEN 0 ESLE t1.tp_wlf_lbyr end			
+	CASE WHEN t1.tp_wlo_lbyr is null THEN 0 ESLE t1.tp_wlo_lbyr end			
+	CASE WHEN t1.tp_construction_lbyr is null THEN 0 ESLE t1.tp_construction_lbyr end			
										)					as	tp_lbyr_surfaceload	
		
,sum(	CASE WHEN t1.tp_crp_lbacre is null THEN 0 ESLE t1.tp_crp_lbacre end			
+	CASE WHEN t1.tp_pas_lbacre is null THEN 0 ESLE t1.tp_pas_lbacre end			
										)					as	tp_ag_lbacre
				
				
,sum(	CASE WHEN t1.tp_inr_lbacre is null THEN 0 ESLE t1.tp_inr_lbacre end			
+	CASE WHEN t1.tp_ir_lbacre is null THEN 0 ESLE t1.tp_ir_lbacre end			
+	CASE WHEN t1.tp_construction_lbacre is null THEN 0 ESLE t1.tp_construction_lbacre end			
+	CASE WHEN t1.tp_tci_lbacre is null THEN 0 ESLE t1.tp_tci_lbacre end			
+	CASE WHEN t1.tp_tct_lbacre is null THEN 0 ESLE t1.tp_tct_lbacre end			
+	CASE WHEN t1.tp_tg_lbacre is null THEN 0 ESLE t1.tp_tg_lbacre end	)					as	tp_urb_lbacre
				
				
,sum(	CASE WHEN t1.tp_fore_lbacre is null THEN 0 ESLE t1.tp_fore_lbacre end			
+	CASE WHEN t1.tp_mo_lbacre is null THEN 0 ESLE t1.tp_mo_lbacre end			
+	CASE WHEN t1.tp_wat_lbacre is null THEN 0 ESLE t1.tp_wat_lbacre end			
+	CASE WHEN t1.tp_wlf_lbacre is null THEN 0 ESLE t1.tp_wlf_lbacre end			
+	CASE WHEN t1.tp_wlo_lbacre is null THEN 0 ESLE t1.tp_wlo_lbacre end			
											)				as	tp_nat_lbacre
				
,sum(	CASE WHEN t1.tss_crp_lbacre is null THEN 0 ESLE t1.tss_crp_lbacre end			
+	CASE WHEN t1.tss_fore_lbacre is null THEN 0 ESLE t1.tss_fore_lbacre end			
+	CASE WHEN t1.tss_inr_lbacre is null THEN 0 ESLE t1.tss_inr_lbacre end			
+	CASE WHEN t1.tss_ir_lbacre is null THEN 0 ESLE t1.tss_ir_lbacre end			
+	CASE WHEN t1.tss_mo_lbacre is null THEN 0 ESLE t1.tss_mo_lbacre end			
+	CASE WHEN t1.tss_pas_lbacre is null THEN 0 ESLE t1.tss_pas_lbacre end			
+	CASE WHEN t1.tss_tci_lbacre is null THEN 0 ESLE t1.tss_tci_lbacre end			
+	CASE WHEN t1.tss_tct_lbacre is null THEN 0 ESLE t1.tss_tct_lbacre end			
+	CASE WHEN t1.tss_tg_lbacre is null THEN 0 ESLE t1.tss_tg_lbacre end			
+	CASE WHEN t1.tss_wat_lbacre is null THEN 0 ESLE t1.tss_wat_lbacre end			
+	CASE WHEN t1.tss_wlf_lbacre is null THEN 0 ESLE t1.tss_wlf_lbacre end			
+	CASE WHEN t1.tss_wlo_lbacre is null THEN 0 ESLE t1.tss_wlo_lbacre end			
+	CASE WHEN t1.tss_construction_lbacre is null THEN 0 ESLE t1.tss_construction_lbacre end			
										)					as	tss_lbacre

,sum(	CASE WHEN t1.tss_crp_lbyr is null THEN 0 ESLE t1.tss_crp_lbyr end			
+	CASE WHEN t1.tss_fore_lbyr is null THEN 0 ESLE t1.tss_fore_lbyr end			
+	CASE WHEN t1.tss_inr_lbyr is null THEN 0 ESLE t1.tss_inr_lbyr end			
+	CASE WHEN t1.tss_ir_lbyr is null THEN 0 ESLE t1.tss_ir_lbyr end			
+	CASE WHEN t1.tss_mo_lbyr is null THEN 0 ESLE t1.tss_mo_lbyr end			
+	CASE WHEN t1.tss_pas_lbyr is null THEN 0 ESLE t1.tss_pas_lbyr end			
+	CASE WHEN t1.tss_tci_lbyr is null THEN 0 ESLE t1.tss_tci_lbyr end			
+	CASE WHEN t1.tss_tct_lbyr is null THEN 0 ESLE t1.tss_tct_lbyr end			
+	CASE WHEN t1.tss_tg_lbyr is null THEN 0 ESLE t1.tss_tg_lbyr end			
+	CASE WHEN t1.tss_wat_lbyr is null THEN 0 ESLE t1.tss_wat_lbyr end			
+	CASE WHEN t1.tss_wlf_lbyr is null THEN 0 ESLE t1.tss_wlf_lbyr end			
+	CASE WHEN t1.tss_wlo_lbyr is null THEN 0 ESLE t1.tss_wlo_lbyr end			
+	CASE WHEN t1.tss_construction_lbyr is null THEN 0 ESLE t1.tss_construction_lbyr end			
										)					as	tss_lbyr_surfaceload				
				
,sum(	CASE WHEN t1.tss_crp_lbacre is null THEN 0 ESLE t1.tss_crp_lbacre end			
+	CASE WHEN t1.tss_pas_lbacre is null THEN 0 ESLE t1.tss_pas_lbacre end			
										)					as	tss_ag_lbacre
				
				
,sum(	CASE WHEN t1.tss_inr_lbacre is null THEN 0 ESLE t1.tss_inr_lbacre end			
+	CASE WHEN t1.tss_ir_lbacre is null THEN 0 ESLE t1.tss_ir_lbacre end			
+	CASE WHEN t1.tss_construction_lbacre is null THEN 0 ESLE t1.tss_construction_lbacre end			
+	CASE WHEN t1.tss_tci_lbacre is null THEN 0 ESLE t1.tss_tci_lbacre end			
+	CASE WHEN t1.tss_tct_lbacre is null THEN 0 ESLE t1.tss_tct_lbacre end			
+	CASE WHEN t1.tss_tg_lbacre is null THEN 0 ESLE t1.tss_tg_lbacre end	)					as	tss_urb_lbacre
				
				
,sum(	CASE WHEN t1.tss_fore_lbacre is null THEN 0 ESLE t1.tss_fore_lbacre end			
+	CASE WHEN t1.tss_mo_lbacre is null THEN 0 ESLE t1.tss_mo_lbacre end			
+	CASE WHEN t1.tss_wat_lbacre is null THEN 0 ESLE t1.tss_wat_lbacre end			
+	CASE WHEN t1.tss_wlf_lbacre is null THEN 0 ESLE t1.tss_wlf_lbacre end			
+	CASE WHEN t1.tss_wlo_lbacre is null THEN 0 ESLE t1.tss_wlo_lbacre end			
											)				as	tss_nat_lbacre

,CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(	CASE WHEN t1.tn_crp_mgl is null THEN 0 ESLE t1.tn_crp_mgl end			
+	CASE WHEN t1.tn_fore_mgl is null THEN 0 ESLE t1.tn_fore_mgl end			
+	CASE WHEN t1.tn_inr_mgl is null THEN 0 ESLE t1.tn_inr_mgl end			
+	CASE WHEN t1.tn_ir_mgl is null THEN 0 ESLE t1.tn_ir_mgl end			
+	CASE WHEN t1.tn_mo_mgl is null THEN 0 ESLE t1.tn_mo_mgl end			
+	CASE WHEN t1.tn_pas_mgl is null THEN 0 ESLE t1.tn_pas_mgl end			
+	CASE WHEN t1.tn_tci_mgl is null THEN 0 ESLE t1.tn_tci_mgl end			
+	CASE WHEN t1.tn_tct_mgl is null THEN 0 ESLE t1.tn_tct_mgl end			
+	CASE WHEN t1.tn_tg_mgl is null THEN 0 ESLE t1.tn_tg_mgl end			
+	CASE WHEN t1.tn_wat_mgl is null THEN 0 ESLE t1.tn_wat_mgl end			
+	CASE WHEN t1.tn_wlf_mgl is null THEN 0 ESLE t1.tn_wlf_mgl end			
+	CASE WHEN t1.tn_wlo_mgl is null THEN 0 ESLE t1.tn_wlo_mgl end			
+	CASE WHEN t1.tn_construction_mgl is null THEN 0 ESLE t1.tn_construction_mgl end			
+	CASE WHEN t1.tn_rpd_mgl is null THEN 0 ESLE t1.tn_rpd_mgl end			
+	CASE WHEN t1.tn_shore_mgl is null THEN 0 ESLE t1.tn_shore_mgl end			
+	CASE WHEN t1.tn_streambnb_mgl is null THEN 0 ESLE t1.tn_streambnb_mgl end			
+	CASE WHEN t1.tn_streamfp_mgl is null THEN 0 ESLE t1.tn_streamfp_mgl end			
+	CASE WHEN t1.tn_cso_areaonly_mgl is null THEN 0 ESLE t1.tn_cso_areaonly_mgl end			
+	CASE WHEN t1.tn_wwi_mgl is null THEN 0 ESLE t1.tn_wwi_mgl end			
+	CASE WHEN t1.tn_wwm_mgl is null THEN 0 ESLE t1.tn_wwm_mgl end			
+	CASE WHEN t1.tn_rib_mgl is null THEN 0 ESLE t1.tn_rib_mgl end			
+	CASE WHEN t1.tn_sep_mgl is null THEN 0 ESLE t1.tn_sep_mgl end	) end						as	tn_mgl
				
				
,CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(	CASE WHEN t1.tn_crp_mgl is null THEN 0 ESLE t1.tn_crp_mgl end			
+	CASE WHEN t1.tn_pas_mgl is null THEN 0 ESLE t1.tn_pas_mgl end			
+	CASE WHEN t1.tn_rpd_mgl is null THEN 0 ESLE t1.tn_rpd_mgl end	) end						as	tn_ag_mgl
				
				
,CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(	CASE WHEN t1.tn_inr_mgl is null THEN 0 ESLE t1.tn_inr_mgl end			
+	CASE WHEN t1.tn_ir_mgl is null THEN 0 ESLE t1.tn_ir_mgl end			
+	CASE WHEN t1.tn_construction_mgl is null THEN 0 ESLE t1.tn_construction_mgl end			
+	CASE WHEN t1.tn_tci_mgl is null THEN 0 ESLE t1.tn_tci_mgl end			
+	CASE WHEN t1.tn_tct_mgl is null THEN 0 ESLE t1.tn_tct_mgl end			
+	CASE WHEN t1.tn_tg_mgl is null THEN 0 ESLE t1.tn_tg_mgl end	) end						as	tn_urb_mgl
				
				
,CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(	CASE WHEN t1.tn_fore_mgl is null THEN 0 ESLE t1.tn_fore_mgl end			
+	CASE WHEN t1.tn_mo_mgl is null THEN 0 ESLE t1.tn_mo_mgl end			
+	CASE WHEN t1.tn_wat_mgl is null THEN 0 ESLE t1.tn_wat_mgl end			
+	CASE WHEN t1.tn_wlf_mgl is null THEN 0 ESLE t1.tn_wlf_mgl end			
+	CASE WHEN t1.tn_wlo_mgl is null THEN 0 ESLE t1.tn_wlo_mgl end			
+	CASE WHEN t1.tn_shore_mgl is null THEN 0 ESLE t1.tn_shore_mgl end			
+	CASE WHEN t1.tn_streambnb_mgl is null THEN 0 ESLE t1.tn_streambnb_mgl end			
+	CASE WHEN t1.tn_streamfp_mgl is null THEN 0 ESLE t1.tn_streamfp_mgl end	) end				as	tn_nat_mgl
				
				
,CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(	CASE WHEN t1.tn_cso_areaonly_mgl is null THEN 0 ESLE t1.tn_cso_areaonly_mgl end			
+	CASE WHEN t1.tn_wwi_mgl is null THEN 0 ESLE t1.tn_wwi_mgl end			
+	CASE WHEN t1.tn_wwm_mgl is null THEN 0 ESLE t1.tn_wwm_mgl end	) end						as	tn_waste_mgl
				
				
,CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(	CASE WHEN t1.tn_rib_mgl is null THEN 0 ESLE t1.tn_rib_mgl end			
+	CASE WHEN t1.tn_sep_mgl is null THEN 0 ESLE t1.tn_sep_mgl end	) end						as	tn_septic_mgl
				
				
,CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(	CASE WHEN t1.tp_crp_mgl is null THEN 0 ESLE t1.tp_crp_mgl end			
+	CASE WHEN t1.tp_fore_mgl is null THEN 0 ESLE t1.tp_fore_mgl end			
+	CASE WHEN t1.tp_inr_mgl is null THEN 0 ESLE t1.tp_inr_mgl end			
+	CASE WHEN t1.tp_ir_mgl is null THEN 0 ESLE t1.tp_ir_mgl end			
+	CASE WHEN t1.tp_mo_mgl is null THEN 0 ESLE t1.tp_mo_mgl end			
+	CASE WHEN t1.tp_pas_mgl is null THEN 0 ESLE t1.tp_pas_mgl end			
+	CASE WHEN t1.tp_tci_mgl is null THEN 0 ESLE t1.tp_tci_mgl end			
+	CASE WHEN t1.tp_tct_mgl is null THEN 0 ESLE t1.tp_tct_mgl end			
+	CASE WHEN t1.tp_tg_mgl is null THEN 0 ESLE t1.tp_tg_mgl end			
+	CASE WHEN t1.tp_wat_mgl is null THEN 0 ESLE t1.tp_wat_mgl end			
+	CASE WHEN t1.tp_wlf_mgl is null THEN 0 ESLE t1.tp_wlf_mgl end			
+	CASE WHEN t1.tp_wlo_mgl is null THEN 0 ESLE t1.tp_wlo_mgl end			
+	CASE WHEN t1.tp_construction_mgl is null THEN 0 ESLE t1.tp_construction_mgl end			
+	CASE WHEN t1.tp_rpd_mgl is null THEN 0 ESLE t1.tp_rpd_mgl end			
+	CASE WHEN t1.tp_shore_mgl is null THEN 0 ESLE t1.tp_shore_mgl end			
+	CASE WHEN t1.tp_streambnb_mgl is null THEN 0 ESLE t1.tp_streambnb_mgl end			
+	CASE WHEN t1.tp_streamfp_mgl is null THEN 0 ESLE t1.tp_streamfp_mgl end			
+	CASE WHEN t1.tp_cso_areaonly_mgl is null THEN 0 ESLE t1.tp_cso_areaonly_mgl end			
+	CASE WHEN t1.tp_wwi_mgl is null THEN 0 ESLE t1.tp_wwi_mgl end			
+	CASE WHEN t1.tp_wwm_mgl is null THEN 0 ESLE t1.tp_wwm_mgl end			
+	CASE WHEN t1.tp_rib_mgl is null THEN 0 ESLE t1.tp_rib_mgl end			
+	CASE WHEN t1.tp_sep_mgl is null THEN 0 ESLE t1.tp_sep_mgl end	) end						as	tp_mgl
				
				
,CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(	CASE WHEN t1.tp_crp_mgl is null THEN 0 ESLE t1.tp_crp_mgl end			
+	CASE WHEN t1.tp_pas_mgl is null THEN 0 ESLE t1.tp_pas_mgl end			
+	CASE WHEN t1.tp_rpd_mgl is null THEN 0 ESLE t1.tp_rpd_mgl end	) end						as	tp_ag_mgl
				
				
,CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(	CASE WHEN t1.tp_inr_mgl is null THEN 0 ESLE t1.tp_inr_mgl end			
+	CASE WHEN t1.tp_ir_mgl is null THEN 0 ESLE t1.tp_ir_mgl end			
+	CASE WHEN t1.tp_construction_mgl is null THEN 0 ESLE t1.tp_construction_mgl end			
+	CASE WHEN t1.tp_tci_mgl is null THEN 0 ESLE t1.tp_tci_mgl end			
+	CASE WHEN t1.tp_tct_mgl is null THEN 0 ESLE t1.tp_tct_mgl end			
+	CASE WHEN t1.tp_tg_mgl is null THEN 0 ESLE t1.tp_tg_mgl end	) end						as	tp_urb_mgl
				
				
,CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(	CASE WHEN t1.tp_fore_mgl is null THEN 0 ESLE t1.tp_fore_mgl end			
+	CASE WHEN t1.tp_mo_mgl is null THEN 0 ESLE t1.tp_mo_mgl end			
+	CASE WHEN t1.tp_wat_mgl is null THEN 0 ESLE t1.tp_wat_mgl end			
+	CASE WHEN t1.tp_wlf_mgl is null THEN 0 ESLE t1.tp_wlf_mgl end			
+	CASE WHEN t1.tp_wlo_mgl is null THEN 0 ESLE t1.tp_wlo_mgl end			
+	CASE WHEN t1.tp_shore_mgl is null THEN 0 ESLE t1.tp_shore_mgl end			
+	CASE WHEN t1.tp_streambnb_mgl is null THEN 0 ESLE t1.tp_streambnb_mgl end			
+	CASE WHEN t1.tp_streamfp_mgl is null THEN 0 ESLE t1.tp_streamfp_mgl end	) end				as	tp_nat_mgl
				
				
,CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(	CASE WHEN t1.tp_cso_areaonly_mgl is null THEN 0 ESLE t1.tp_cso_areaonly_mgl end			
+	CASE WHEN t1.tp_wwi_mgl is null THEN 0 ESLE t1.tp_wwi_mgl end			
+	CASE WHEN t1.tp_wwm_mgl is null THEN 0 ESLE t1.tp_wwm_mgl end	) end						as	tp_waste_mgl
				
				
,CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(	CASE WHEN t1.tp_rib_mgl is null THEN 0 ESLE t1.tp_rib_mgl end			
+	CASE WHEN t1.tp_sep_mgl is null THEN 0 ESLE t1.tp_sep_mgl end	) end						as	tp_septic_mgl
				
				
,CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(	CASE WHEN t1.tss_crp_mgl is null THEN 0 ESLE t1.tss_crp_mgl end			
+	CASE WHEN t1.tss_fore_mgl is null THEN 0 ESLE t1.tss_fore_mgl end			
+	CASE WHEN t1.tss_inr_mgl is null THEN 0 ESLE t1.tss_inr_mgl end			
+	CASE WHEN t1.tss_ir_mgl is null THEN 0 ESLE t1.tss_ir_mgl end			
+	CASE WHEN t1.tss_mo_mgl is null THEN 0 ESLE t1.tss_mo_mgl end			
+	CASE WHEN t1.tss_pas_mgl is null THEN 0 ESLE t1.tss_pas_mgl end			
+	CASE WHEN t1.tss_tci_mgl is null THEN 0 ESLE t1.tss_tci_mgl end			
+	CASE WHEN t1.tss_tct_mgl is null THEN 0 ESLE t1.tss_tct_mgl end			
+	CASE WHEN t1.tss_tg_mgl is null THEN 0 ESLE t1.tss_tg_mgl end			
+	CASE WHEN t1.tss_wat_mgl is null THEN 0 ESLE t1.tss_wat_mgl end			
+	CASE WHEN t1.tss_wlf_mgl is null THEN 0 ESLE t1.tss_wlf_mgl end			
+	CASE WHEN t1.tss_wlo_mgl is null THEN 0 ESLE t1.tss_wlo_mgl end			
+	CASE WHEN t1.tss_construction_mgl is null THEN 0 ESLE t1.tss_construction_mgl end			
+	CASE WHEN t1.tss_rpd_mgl is null THEN 0 ESLE t1.tss_rpd_mgl end			
+	CASE WHEN t1.tss_shore_mgl is null THEN 0 ESLE t1.tss_shore_mgl end			
+	CASE WHEN t1.tss_streambnb_mgl is null THEN 0 ESLE t1.tss_streambnb_mgl end			
+	CASE WHEN t1.tss_streamfp_mgl is null THEN 0 ESLE t1.tss_streamfp_mgl end			
+	CASE WHEN t1.tss_cso_areaonly_mgl is null THEN 0 ESLE t1.tss_cso_areaonly_mgl end			
+	CASE WHEN t1.tss_wwi_mgl is null THEN 0 ESLE t1.tss_wwi_mgl end			
+	CASE WHEN t1.tss_wwm_mgl is null THEN 0 ESLE t1.tss_wwm_mgl end	) end						as	tss_mgl
				
				
,CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(	CASE WHEN t1.tss_crp_mgl is null THEN 0 ESLE t1.tss_crp_mgl end			
+	CASE WHEN t1.tss_pas_mgl is null THEN 0 ESLE t1.tss_pas_mgl end			
+	CASE WHEN t1.tss_rpd_mgl is null THEN 0 ESLE t1.tss_rpd_mgl end	) end						as	tss_ag_mgl
				
				
,CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(	CASE WHEN t1.tss_inr_mgl is null THEN 0 ESLE t1.tss_inr_mgl end			
+	CASE WHEN t1.tss_ir_mgl is null THEN 0 ESLE t1.tss_ir_mgl end			
+	CASE WHEN t1.tss_construction_mgl is null THEN 0 ESLE t1.tss_construction_mgl end			
+	CASE WHEN t1.tss_tci_mgl is null THEN 0 ESLE t1.tss_tci_mgl end			
+	CASE WHEN t1.tss_tct_mgl is null THEN 0 ESLE t1.tss_tct_mgl end			
+	CASE WHEN t1.tss_tg_mgl is null THEN 0 ESLE t1.tss_tg_mgl end	) end						as	tss_urb_mgl
				
				
,CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(	CASE WHEN t1.tss_fore_mgl is null THEN 0 ESLE t1.tss_fore_mgl end			
+	CASE WHEN t1.tss_mo_mgl is null THEN 0 ESLE t1.tss_mo_mgl end			
+	CASE WHEN t1.tss_wat_mgl is null THEN 0 ESLE t1.tss_wat_mgl end			
+	CASE WHEN t1.tss_wlf_mgl is null THEN 0 ESLE t1.tss_wlf_mgl end			
+	CASE WHEN t1.tss_wlo_mgl is null THEN 0 ESLE t1.tss_wlo_mgl end			
+	CASE WHEN t1.tss_shore_mgl is null THEN 0 ESLE t1.tss_shore_mgl end			
+	CASE WHEN t1.tss_streambnb_mgl is null THEN 0 ESLE t1.tss_streambnb_mgl end			
+	CASE WHEN t1.tss_streamfp_mgl is null THEN 0 ESLE t1.tss_streamfp_mgl end	) end				as	tss_nat_mgl
				
				
,CASE WHEN nhd.maflowv < 0 THEN -9999 ESLE sum(	CASE WHEN t1.tss_cso_areaonly_mgl is null THEN 0 ESLE t1.tss_cso_areaonly_mgl end			
+	CASE WHEN t1.tss_wwi_mgl is null THEN 0 ESLE t1.tss_wwi_mgl end			
+	CASE WHEN t1.tss_wwm_mgl is null THEN 0 ESLE t1.tss_wwm_mgl end	) end						as	tss_waste_mgl

,nhd.catchment


from
(
SELECT distinct a.comid, a.huc12,a.nhd_areasqkm, a.intersect_areasqkm, 
       a.h12_tot_sqkm, a.h12crp_sqkm, a.h12fore_sqkm, a.h12inr_sqkm,a. h12ir_sqkm, 
      a.h12mo_sqkm, a.h12pas_sqkm, a.h12tci_sqkm, a.h12tct_sqkm, a.h12tg_sqkm, 
       a.h12wat_sqkm, a.h12wlf_sqkm, a.h12wlo_sqkm, a.h12wlt_sqkm, a.nhdcrp_sqkm, 
       a.nhdfore_sqkm, a.nhdinr_sqkm, a.nhdir_sqkm, a.nhdmo_sqkm, a.nhdpas_sqkm, 
       a.nhdtci_sqkm, a.nhdtct_sqkm, a.nhdtg_sqkm, a.nhdwat_sqkm, a.nhdwlf_sqkm, 
       a.nhdwlo_sqkm, a.nhdwlt_sqkm, a.nord, a.nordstop, a.nhd_lengthkm, a.intersect_lengthkm, 
       a.huc12_lengthkm

-- lulc associated load_sources

-- CRP

, CASE WHEN h12crp_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdcrp_sqkm / h12crp_sqkm) * tn_crp) / (intersect_areasqkm * 247.105) 
	END  	as tn_crp_lbacre
, CASE WHEN h12crp_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdcrp_sqkm / h12crp_sqkm) * tp_crp) / (intersect_areasqkm * 247.105) 
	END  	as tp_crp_lbacre
, CASE WHEN h12crp_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdcrp_sqkm / h12crp_sqkm)  * tss_crp) / (intersect_areasqkm * 247.105) 
	END 	as tss_crp_lbacre

, CASE WHEN h12crp_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdcrp_sqkm / h12crp_sqkm) * tn_crp)
	END  	as tn_crp_lbyr
, CASE WHEN h12crp_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdcrp_sqkm / h12crp_sqkm) * tp_crp)
	END  	as tp_crp_lbyr
, CASE WHEN h12crp_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdcrp_sqkm / h12crp_sqkm)  * tss_crp)
	END 	as tss_crp_lbyr

, CASE WHEN h12crp_sqkm = 0 THEN 0 
	ESLE ( tn_crp * (nhdcrp_sqkm / h12crp_sqkm)  * 453592 ) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	END AS tn_crp_mgl
, CASE WHEN h12crp_sqkm = 0 THEN 0 
	ESLE ( tp_crp * (nhdcrp_sqkm / h12crp_sqkm)  * 453592 ) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	END AS tp_crp_mgl
, CASE WHEN h12crp_sqkm = 0 THEN 0 
	ESLE ( tss_crp * (nhdcrp_sqkm / h12crp_sqkm) * 453592 ) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	END AS tss_crp_mgl

-- FORE

, CASE WHEN h12fore_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdfore_sqkm / h12fore_sqkm) * tn_fore) / (intersect_areasqkm * 247.105) 
	END  	as tn_fore_lbacre
, CASE WHEN h12fore_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdfore_sqkm / h12fore_sqkm) * tp_fore) / (intersect_areasqkm * 247.105) 
	END  	as tp_fore_lbacre
, CASE WHEN h12fore_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdfore_sqkm / h12fore_sqkm)  * tss_fore) / (intersect_areasqkm * 247.105) 
	END 	as tss_fore_lbacre

, CASE WHEN h12fore_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdfore_sqkm / h12fore_sqkm) * tn_fore)
	END  	as tn_fore_lbyr
, CASE WHEN h12fore_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdfore_sqkm / h12fore_sqkm) * tp_fore)
	END  	as tp_fore_lbyr
, CASE WHEN h12fore_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdfore_sqkm / h12fore_sqkm)  * tss_fore)
	END 	as tss_fore_lbyr

, CASE WHEN h12fore_sqkm = 0 THEN 0 
	ESLE ( tn_fore * (nhdfore_sqkm / h12fore_sqkm)  * 453592 ) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	END AS tn_fore_mgl
, CASE WHEN h12fore_sqkm = 0 THEN 0 
	ESLE ( tp_fore * (nhdfore_sqkm / h12fore_sqkm)  * 453592 ) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	END AS tp_fore_mgl
, CASE WHEN h12fore_sqkm = 0 THEN 0 
	ESLE ( tss_fore * (nhdfore_sqkm / h12fore_sqkm) * 453592 ) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	END AS tss_fore_mgl

-- INR

, CASE WHEN h12inr_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdinr_sqkm / h12inr_sqkm) * tn_inr) / (intersect_areasqkm * 247.105) 
	END  	as tn_inr_lbacre
, CASE WHEN h12inr_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdinr_sqkm / h12inr_sqkm) * tp_inr) / (intersect_areasqkm * 247.105) 
	END  	as tp_inr_lbacre
, CASE WHEN h12inr_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdinr_sqkm / h12inr_sqkm)  * tss_inr) / (intersect_areasqkm * 247.105) 
	END 	as tss_inr_lbacre

, CASE WHEN h12inr_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdinr_sqkm / h12inr_sqkm) * tn_inr)
	END  	as tn_inr_lbyr
, CASE WHEN h12inr_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdinr_sqkm / h12inr_sqkm) * tp_inr)
	END  	as tp_inr_lbyr
, CASE WHEN h12inr_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdinr_sqkm / h12inr_sqkm)  * tss_inr)
	END 	as tss_inr_lbyr

, CASE WHEN h12inr_sqkm = 0 THEN 0 
	ESLE ( tn_inr * (nhdinr_sqkm / h12inr_sqkm)  * 453592 ) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	END AS tn_inr_mgl
, CASE WHEN h12inr_sqkm = 0 THEN 0 
	ESLE ( tp_inr * (nhdinr_sqkm / h12inr_sqkm)  * 453592 ) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	END AS tp_inr_mgl
, CASE WHEN h12inr_sqkm = 0 THEN 0 
	ESLE ( tss_inr * (nhdinr_sqkm / h12inr_sqkm) * 453592 ) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	END AS tss_inr_mgl

-- IR

, CASE WHEN h12ir_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdir_sqkm / h12ir_sqkm) * tn_ir) / (intersect_areasqkm * 247.105) 
	END  	as tn_ir_lbacre
, CASE WHEN h12ir_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdir_sqkm / h12ir_sqkm) * tp_ir) / (intersect_areasqkm * 247.105) 
	END  	as tp_ir_lbacre
, CASE WHEN h12ir_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdir_sqkm / h12ir_sqkm)  * tss_ir) / (intersect_areasqkm * 247.105) 
	END 	as tss_ir_lbacre

, CASE WHEN h12ir_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdir_sqkm / h12ir_sqkm) * tn_ir)
	END  	as tn_ir_lbyr
, CASE WHEN h12ir_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdir_sqkm / h12ir_sqkm) * tp_ir)
	END  	as tp_ir_lbyr
, CASE WHEN h12ir_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdir_sqkm / h12ir_sqkm)  * tss_ir)
	END 	as tss_ir_lbyr

, CASE WHEN h12ir_sqkm = 0 THEN 0 
	ESLE ( tn_ir * (nhdir_sqkm / h12ir_sqkm)  * 453592 ) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	END AS tn_ir_mgl
, CASE WHEN h12ir_sqkm = 0 THEN 0 
	ESLE ( tp_ir * (nhdir_sqkm / h12ir_sqkm)  * 453592 ) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	END AS tp_ir_mgl
, CASE WHEN h12ir_sqkm = 0 THEN 0 
	ESLE ( tss_ir * (nhdir_sqkm / h12ir_sqkm) * 453592 ) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	END AS tss_ir_mgl

-- MO

, CASE WHEN h12mo_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdmo_sqkm / h12mo_sqkm) * tn_mo) / (intersect_areasqkm * 247.105) 
	END  	as tn_mo_lbacre
, CASE WHEN h12mo_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdmo_sqkm / h12mo_sqkm) * tp_mo) / (intersect_areasqkm * 247.105) 
	END  	as tp_mo_lbacre
, CASE WHEN h12mo_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdmo_sqkm / h12mo_sqkm)  * tss_mo) / (intersect_areasqkm * 247.105) 
	END 	as tss_mo_lbacre

, CASE WHEN h12mo_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdmo_sqkm / h12mo_sqkm) * tn_mo)
	END  	as tn_mo_lbyr
, CASE WHEN h12mo_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdmo_sqkm / h12mo_sqkm) * tp_mo)
	END  	as tp_mo_lbyr
, CASE WHEN h12mo_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdmo_sqkm / h12mo_sqkm)  * tss_mo)
	END 	as tss_mo_lbyr

, CASE WHEN h12mo_sqkm = 0 THEN 0 
	ESLE ( tn_mo * (nhdmo_sqkm / h12mo_sqkm)  * 453592 ) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	END AS tn_mo_mgl
, CASE WHEN h12mo_sqkm = 0 THEN 0 
	ESLE ( tp_mo * (nhdmo_sqkm / h12mo_sqkm)  * 453592 ) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	END AS tp_mo_mgl
, CASE WHEN h12mo_sqkm = 0 THEN 0 
	ESLE ( tss_mo * (nhdmo_sqkm / h12mo_sqkm) * 453592 ) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	END AS tss_mo_mgl

-- PAS

, CASE WHEN h12pas_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdpas_sqkm / h12pas_sqkm) * tn_pas) / (intersect_areasqkm * 247.105) 
	END  	as tn_pas_lbacre
, CASE WHEN h12pas_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdpas_sqkm / h12pas_sqkm) * tp_pas) / (intersect_areasqkm * 247.105) 
	END  	as tp_pas_lbacre
, CASE WHEN h12pas_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdpas_sqkm / h12pas_sqkm)  * tss_pas) / (intersect_areasqkm * 247.105) 
	END 	as tss_pas_lbacre

, CASE WHEN h12pas_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdpas_sqkm / h12pas_sqkm) * tn_pas)
	END  	as tn_pas_lbyr
, CASE WHEN h12pas_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdpas_sqkm / h12pas_sqkm) * tp_pas)
	END  	as tp_pas_lbyr
, CASE WHEN h12pas_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdpas_sqkm / h12pas_sqkm)  * tss_pas)
	END 	as tss_pas_lbyr

, CASE WHEN h12pas_sqkm = 0 THEN 0 
	ESLE ( tn_pas * (nhdpas_sqkm / h12pas_sqkm)  * 453592 ) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	END AS tn_pas_mgl
, CASE WHEN h12pas_sqkm = 0 THEN 0 
	ESLE ( tp_pas * (nhdpas_sqkm / h12pas_sqkm)  * 453592 ) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	END AS tp_pas_mgl
, CASE WHEN h12pas_sqkm = 0 THEN 0 
	ESLE ( tss_pas * (nhdpas_sqkm / h12pas_sqkm) * 453592 ) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	END AS tss_pas_mgl

-- TCI

, CASE WHEN h12tci_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdtci_sqkm / h12tci_sqkm) * tn_tci) / (intersect_areasqkm * 247.105) 
	END  	as tn_tci_lbacre
, CASE WHEN h12tci_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdtci_sqkm / h12tci_sqkm) * tp_tci) / (intersect_areasqkm * 247.105) 
	END  	as tp_tci_lbacre
, CASE WHEN h12tci_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdtci_sqkm / h12tci_sqkm)  * tss_tci) / (intersect_areasqkm * 247.105) 
	END 	as tss_tci_lbacre

, CASE WHEN h12tci_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdtci_sqkm / h12tci_sqkm) * tn_tci)
	END  	as tn_tci_lbyr
, CASE WHEN h12tci_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdtci_sqkm / h12tci_sqkm) * tp_tci)
	END  	as tp_tci_lbyr
, CASE WHEN h12tci_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdtci_sqkm / h12tci_sqkm)  * tss_tci)
	END 	as tss_tci_lbyr

, CASE WHEN h12tci_sqkm = 0 THEN 0 
	ESLE ( tn_tci * (nhdtci_sqkm / h12tci_sqkm)  * 453592 ) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	END AS tn_tci_mgl
, CASE WHEN h12tci_sqkm = 0 THEN 0 
	ESLE ( tp_tci * (nhdtci_sqkm / h12tci_sqkm)  * 453592 ) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	END AS tp_tci_mgl
, CASE WHEN h12tci_sqkm = 0 THEN 0 
	ESLE ( tss_tci * (nhdtci_sqkm / h12tci_sqkm) * 453592 ) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	END AS tss_tci_mgl

-- TCT

, CASE WHEN h12tct_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdtct_sqkm / h12tct_sqkm) * tn_tct) / (intersect_areasqkm * 247.105) 
	END  	as tn_tct_lbacre
, CASE WHEN h12tct_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdtct_sqkm / h12tct_sqkm) * tp_tct) / (intersect_areasqkm * 247.105) 
	END  	as tp_tct_lbacre
, CASE WHEN h12tct_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdtct_sqkm / h12tct_sqkm)  * tss_tct) / (intersect_areasqkm * 247.105) 
	END 	as tss_tct_lbacre

, CASE WHEN h12tct_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdtct_sqkm / h12tct_sqkm) * tn_tct)
	END  	as tn_tct_lbyr
, CASE WHEN h12tct_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdtct_sqkm / h12tct_sqkm) * tp_tct)
	END  	as tp_tct_lbyr
, CASE WHEN h12tct_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdtct_sqkm / h12tct_sqkm)  * tss_tct)
	END 	as tss_tct_lbyr

, CASE WHEN h12tct_sqkm = 0 THEN 0 
	ESLE ( tn_tct * (nhdtct_sqkm / h12tct_sqkm)  * 453592 ) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	END AS tn_tct_mgl
, CASE WHEN h12tct_sqkm = 0 THEN 0 
	ESLE ( tp_tct * (nhdtct_sqkm / h12tct_sqkm)  * 453592 ) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	END AS tp_tct_mgl
, CASE WHEN h12tct_sqkm = 0 THEN 0 
	ESLE ( tss_tct * (nhdtct_sqkm / h12tct_sqkm) * 453592 ) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	END AS tss_tct_mgl

-- TG

, CASE WHEN h12tg_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdtg_sqkm / h12tg_sqkm) * tn_tg) / (intersect_areasqkm * 247.105) 
	END  	as tn_tg_lbacre
, CASE WHEN h12tg_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdtg_sqkm / h12tg_sqkm) * tp_tg) / (intersect_areasqkm * 247.105) 
	END  	as tp_tg_lbacre
, CASE WHEN h12tg_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdtg_sqkm / h12tg_sqkm)  * tss_tg) / (intersect_areasqkm * 247.105) 
	END 	as tss_tg_lbacre

, CASE WHEN h12tg_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdtg_sqkm / h12tg_sqkm) * tn_tg)
	END  	as tn_tg_lbyr
, CASE WHEN h12tg_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdtg_sqkm / h12tg_sqkm) * tp_tg)
	END  	as tp_tg_lbyr
, CASE WHEN h12tg_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdtg_sqkm / h12tg_sqkm)  * tss_tg)
	END 	as tss_tg_lbyr

, CASE WHEN h12tg_sqkm = 0 THEN 0 
	ESLE ( tn_tg * (nhdtg_sqkm / h12tg_sqkm)  * 453592 ) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	END AS tn_tg_mgl
, CASE WHEN h12tg_sqkm = 0 THEN 0 
	ESLE ( tp_tg * (nhdtg_sqkm / h12tg_sqkm)  * 453592 ) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	END AS tp_tg_mgl
, CASE WHEN h12tg_sqkm = 0 THEN 0 
	ESLE ( tss_tg * (nhdtg_sqkm / h12tg_sqkm) * 453592 ) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	END AS tss_tg_mgl

-- WAT

, CASE WHEN h12wat_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdwat_sqkm / h12wat_sqkm) * tn_wat) / (intersect_areasqkm * 247.105) 
	END  	as tn_wat_lbacre
, CASE WHEN h12wat_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdwat_sqkm / h12wat_sqkm) * tp_wat) / (intersect_areasqkm * 247.105) 
	END  	as tp_wat_lbacre
, CASE WHEN h12wat_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdwat_sqkm / h12wat_sqkm)  * tss_wat) / (intersect_areasqkm * 247.105) 
	END 	as tss_wat_lbacre

, CASE WHEN h12wat_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdwat_sqkm / h12wat_sqkm) * tn_wat)
	END  	as tn_wat_lbyr
, CASE WHEN h12wat_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdwat_sqkm / h12wat_sqkm) * tp_wat)
	END  	as tp_wat_lbyr
, CASE WHEN h12wat_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdwat_sqkm / h12wat_sqkm)  * tss_wat)
	END 	as tss_wat_lbyr

, CASE WHEN h12wat_sqkm = 0 THEN 0 
	ESLE ( tn_wat * (nhdwat_sqkm / h12wat_sqkm)  * 453592 ) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	END AS tn_wat_mgl
, CASE WHEN h12wat_sqkm = 0 THEN 0 
	ESLE ( tp_wat * (nhdwat_sqkm / h12wat_sqkm)  * 453592 ) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	END AS tp_wat_mgl
, CASE WHEN h12wat_sqkm = 0 THEN 0 
	ESLE ( tss_wat * (nhdwat_sqkm / h12wat_sqkm) * 453592 ) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	END AS tss_wat_mgl

-- WLF

, CASE WHEN h12wlf_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdwlf_sqkm / h12wlf_sqkm) * tn_wlf) / (intersect_areasqkm * 247.105) 
	END  	as tn_wlf_lbacre
, CASE WHEN h12wlf_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdwlf_sqkm / h12wlf_sqkm) * tp_wlf) / (intersect_areasqkm * 247.105) 
	END  	as tp_wlf_lbacre
, CASE WHEN h12wlf_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdwlf_sqkm / h12wlf_sqkm)  * tss_wlf) / (intersect_areasqkm * 247.105) 
	END 	as tss_wlf_lbacre

, CASE WHEN h12wlf_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdwlf_sqkm / h12wlf_sqkm) * tn_wlf)
	END  	as tn_wlf_lbyr
, CASE WHEN h12wlf_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdwlf_sqkm / h12wlf_sqkm) * tp_wlf)
	END  	as tp_wlf_lbyr
, CASE WHEN h12wlf_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdwlf_sqkm / h12wlf_sqkm)  * tss_wlf)
	END 	as tss_wlf_lbyr

, CASE WHEN h12wlf_sqkm = 0 THEN 0 
	ESLE ( tn_wlf * (nhdwlf_sqkm / h12wlf_sqkm)  * 453592 ) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	END AS tn_wlf_mgl
, CASE WHEN h12wlf_sqkm = 0 THEN 0 
	ESLE ( tp_wlf * (nhdwlf_sqkm / h12wlf_sqkm)  * 453592 ) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	END AS tp_wlf_mgl
, CASE WHEN h12wlf_sqkm = 0 THEN 0 
	ESLE ( tss_wlf * (nhdwlf_sqkm / h12wlf_sqkm) * 453592 ) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	END AS tss_wlf_mgl

-- WLO

, CASE WHEN h12wlo_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdwlo_sqkm / h12wlo_sqkm) * tn_wlo) / (intersect_areasqkm * 247.105) 
	END  	as tn_wlo_lbacre
, CASE WHEN h12wlo_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdwlo_sqkm / h12wlo_sqkm) * tp_wlo) / (intersect_areasqkm * 247.105) 
	END  	as tp_wlo_lbacre
, CASE WHEN h12wlo_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdwlo_sqkm / h12wlo_sqkm)  * tss_wlo) / (intersect_areasqkm * 247.105) 
	END 	as tss_wlo_lbacre

, CASE WHEN h12wlo_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdwlo_sqkm / h12wlo_sqkm) * tn_wlo)
	END  	as tn_wlo_lbyr
, CASE WHEN h12wlo_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdwlo_sqkm / h12wlo_sqkm) * tp_wlo)
	END  	as tp_wlo_lbyr
, CASE WHEN h12wlo_sqkm = 0 THEN 0 
	ESLE ((intersect_areasqkm / nhd_areasqkm) * (nhdwlo_sqkm / h12wlo_sqkm)  * tss_wlo)
	END 	as tss_wlo_lbyr

, CASE WHEN h12wlo_sqkm = 0 THEN 0 
	ESLE ( tn_wlo * (nhdwlo_sqkm / h12wlo_sqkm)  * 453592 ) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	END AS tn_wlo_mgl
, CASE WHEN h12wlo_sqkm = 0 THEN 0 
	ESLE ( tp_wlo * (nhdwlo_sqkm / h12wlo_sqkm)  * 453592 ) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	END AS tp_wlo_mgl
, CASE WHEN h12wlo_sqkm = 0 THEN 0 
	ESLE ( tss_wlo * (nhdwlo_sqkm / h12wlo_sqkm) * 453592 ) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) 
	END AS tss_wlo_mgl

-- WLT -- There are no load inputs or reductions noted for WLT

-- remaining load_sources
-- construction  /* CONSIDER LIMITING THIS TO MO/INR/IR LULC instead of a pure by area distribution*/
, tn_con_lbyr  * (intersect_areasqkm / h12_tot_sqkm) / (intersect_areasqkm * 247.105) AS tn_construction_lbacre
, tp_con_lbyr  * (intersect_areasqkm / h12_tot_sqkm) / (intersect_areasqkm * 247.105) AS tp_construction_lbacre
, tss_con_lbyr * (intersect_areasqkm / h12_tot_sqkm) / (intersect_areasqkm * 247.105) AS tss_construction_lbacre

, tn_con_lbyr  * (intersect_areasqkm / h12_tot_sqkm) AS tn_construction_lbyr
, tp_con_lbyr  * (intersect_areasqkm / h12_tot_sqkm) AS tp_construction_lbyr
, tss_con_lbyr * (intersect_areasqkm / h12_tot_sqkm) AS tss_construction_lbyr

, ( tn_con_lbyr  * 453592 )  *  (intersect_areasqkm / h12_tot_sqkm) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) AS tn_construction_mgl
, ( tp_con_lbyr  * 453592 )  *  (intersect_areasqkm / h12_tot_sqkm) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) AS tp_construction_mgl
, ( tss_con_lbyr * 453592 )  *  (intersect_areasqkm / h12_tot_sqkm) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) AS tss_construction_mgl

-- riparian pasture deposition - rpd is in lb/yr direct load into streams
, tn_rpd   * (intersect_areasqkm / h12_tot_sqkm) / (intersect_areasqkm * 247.105) AS tn_rpd_lbacre
, tp_rpd   * (intersect_areasqkm / h12_tot_sqkm) / (intersect_areasqkm * 247.105) AS tp_rpd_lbacre
, tss_rpd  * (intersect_areasqkm / h12_tot_sqkm) / (intersect_areasqkm * 247.105) AS tss_rpd_lbacre

, tn_rpd   * (intersect_areasqkm / h12_tot_sqkm) AS tn_rpd_lbyr
, tp_rpd   * (intersect_areasqkm / h12_tot_sqkm) AS tp_rpd_lbyr
, tss_rpd  * (intersect_areasqkm / h12_tot_sqkm) AS tss_rpd_lbyr

, ( tn_rpd   * 453592 )  *  (intersect_areasqkm / h12_tot_sqkm) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) AS tn_rpd_mgl
, ( tp_rpd   * 453592 )  *  (intersect_areasqkm / h12_tot_sqkm) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) AS tp_rpd_mgl
, ( tss_rpd  * 453592 )  *  (intersect_areasqkm / h12_tot_sqkm) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) AS tss_rpd_mgl

-- shoreline
, tn_shore_lbkm  * (intersect_lengthkm / huc12_lengthkm) AS tn_shore_lbkm
, tp_shore_lbkm  * (intersect_lengthkm / huc12_lengthkm) AS tp_shore_lbkm
, tss_shore_lbkm * (intersect_lengthkm / huc12_lengthkm) AS tss_shore_lbkm

, tn_shore_lbyr  * (intersect_lengthkm / huc12_lengthkm) AS tn_shore_lbyr
, tp_shore_lbyr  * (intersect_lengthkm / huc12_lengthkm) AS tp_shore_lbyr
, tss_shore_lbyr * (intersect_lengthkm / huc12_lengthkm) AS tss_shore_lbyr

, ( tn_shore_lbyr    * 453592 )  *  (intersect_areasqkm / h12_tot_sqkm) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) AS tn_shore_mgl
, ( tp_shore_lbyr    * 453592 )  *  (intersect_areasqkm / h12_tot_sqkm) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) AS tp_shore_mgl
, ( tss_shore_lbyr   * 453592 )  *  (intersect_areasqkm / h12_tot_sqkm) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) AS tss_shore_mgl

-- stream bed and bank
, tn_sbb_lbkm  * (intersect_lengthkm / huc12_lengthkm) AS tn_streambnb_lbkm
, tp_sbb_lbkm  * (intersect_lengthkm / huc12_lengthkm) AS tp_streambnb_lbkm
, tss_sbb_lbkm * (intersect_lengthkm / huc12_lengthkm) AS tss_streambnb_lbkm

, tn_sbb_lbyr  * (intersect_lengthkm / huc12_lengthkm) AS tn_streambnb_lbyr
, tp_sbb_lbyr  * (intersect_lengthkm / huc12_lengthkm) AS tp_streambnb_lbyr
, tss_sbb_lbyr * (intersect_lengthkm / huc12_lengthkm) AS tss_streambnb_lbyr

, ( tn_sbb_lbyr    * 453592 )  * (intersect_lengthkm / huc12_lengthkm) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) AS tn_streambnb_mgl
, ( tp_sbb_lbyr    * 453592 )  * (intersect_lengthkm / huc12_lengthkm) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) AS tp_streambnb_mgl
, ( tss_sbb_lbyr   * 453592 )  * (intersect_lengthkm / huc12_lengthkm) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) AS tss_streambnb_mgl

-- stream flood plain
, tn_sfp_lbkm  * (intersect_lengthkm / huc12_lengthkm) AS tn_streamfp_lbkm
, tp_sfp_lbkm  * (intersect_lengthkm / huc12_lengthkm) AS tp_streamfp_lbkm
, tss_sfp_lbkm * (intersect_lengthkm / huc12_lengthkm) AS tss_streamfp_lbkm

, tn_sfp_lbyr  * (intersect_lengthkm / huc12_lengthkm) AS tn_streamfp_lbyr
, tp_sfp_lbyr  * (intersect_lengthkm / huc12_lengthkm) AS tp_streamfp_lbyr
, tss_sfp_lbyr * (intersect_lengthkm / huc12_lengthkm) AS tss_streamfp_lbyr

, ( tn_sfp_lbyr    * 453592 )  * (intersect_lengthkm / huc12_lengthkm) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) AS tn_streamfp_mgl
, ( tp_sfp_lbyr    * 453592 )  * (intersect_lengthkm / huc12_lengthkm) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) AS tp_streamfp_mgl
, ( tss_sfp_lbyr   * 453592 )  * (intersect_lengthkm / huc12_lengthkm) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) AS tss_streamfp_mgl

-- wastewater load_sources

-- cso - tn_cso_lbyr - tn_cso_areaonly_lbyr (strict by area from huc12 to nhd) vs tn_cso_npdes_lbyr (accuonts for cso location within huc12 and assigns load to those nhd catchments)
, tn_cso_areaonly_lbyr  * (intersect_areasqkm / h12_tot_sqkm) / (intersect_areasqkm * 247.105) AS tn_cso_areaonly_lbacre
, tp_cso_areaonly_lbyr  * (intersect_areasqkm / h12_tot_sqkm) / (intersect_areasqkm * 247.105) AS tp_cso_areaonly_lbacre
, tss_cso_areaonly_lbyr * (intersect_areasqkm / h12_tot_sqkm) / (intersect_areasqkm * 247.105) AS tss_cso_areaonly_lbacre

, tn_cso_areaonly_lbyr  * (intersect_areasqkm / h12_tot_sqkm) AS tn_cso_areaonly_lbyr
, tp_cso_areaonly_lbyr  * (intersect_areasqkm / h12_tot_sqkm) AS tp_cso_areaonly_lbyr
, tss_cso_areaonly_lbyr * (intersect_areasqkm / h12_tot_sqkm) AS tss_cso_areaonly_lbyr

, ( tn_cso_areaonly_lbyr    * 453592 )  * (intersect_lengthkm / huc12_lengthkm) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) AS tn_cso_areaonly_mgl
, ( tp_cso_areaonly_lbyr    * 453592 )  * (intersect_lengthkm / huc12_lengthkm) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) AS tp_cso_areaonly_mgl
, ( tss_cso_areaonly_lbyr   * 453592 )  * (intersect_lengthkm / huc12_lengthkm) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) AS tss_cso_areaonly_mgl

-- wwi - tn_wwi_lbyr
, sum(tn_wwi_lbyr)  * (intersect_areasqkm / h12_tot_sqkm) / (intersect_areasqkm * 247.105) AS tn_wwi_lbacre
, sum(tp_wwi_lbyr)  * (intersect_areasqkm / h12_tot_sqkm) / (intersect_areasqkm * 247.105) AS tp_wwi_lbacre
, sum(tss_wwi_lbyr) * (intersect_areasqkm / h12_tot_sqkm) / (intersect_areasqkm * 247.105) AS tss_wwi_lbacre

, sum(tn_wwi_lbyr)  * (intersect_areasqkm / h12_tot_sqkm) AS tn_wwi_lbyr
, sum(tp_wwi_lbyr)  * (intersect_areasqkm / h12_tot_sqkm) AS tp_wwi_lbyr
, sum(tss_wwi_lbyr) * (intersect_areasqkm / h12_tot_sqkm) AS tss_wwi_lbyr

, sum(tn_wwi_mgl) AS tn_wwi_mgl
, sum(tp_wwi_mgl) AS tp_wwi_mgl
, sum(tss_wwi_mgl) AS tss_wwi_mgl

-- wwm - tn_wwm_lbyr
, sum(tn_wwm_lbyr)   * (intersect_areasqkm / h12_tot_sqkm) / (intersect_areasqkm * 247.105) AS tn_wwm_lbacre
, sum(tp_wwm_lbyr)   * (intersect_areasqkm / h12_tot_sqkm) / (intersect_areasqkm * 247.105) AS tp_wwm_lbacre
, sum(tss_wwm_lbyr)  * (intersect_areasqkm / h12_tot_sqkm) / (intersect_areasqkm * 247.105) AS tss_wwm_lbacre

, sum(tn_wwm_lbyr)   * (intersect_areasqkm / h12_tot_sqkm) AS tn_wwm_lbyr
, sum(tp_wwm_lbyr)   * (intersect_areasqkm / h12_tot_sqkm) AS tp_wwm_lbyr
, sum(tss_wwm_lbyr)  * (intersect_areasqkm / h12_tot_sqkm) AS tss_wwm_lbyr

, sum(tn_wwm_mgl) AS tn_wwm_mgl
, sum(tp_wwm_mgl) AS tp_wwm_mgl
, sum(tss_wwm_mgl) AS tss_wwm_mgl

-- rib - tn_rib_lbyr
, tn_rib_lbyr    * (intersect_areasqkm / h12_tot_sqkm) / (intersect_areasqkm * 247.105) AS tn_rib_lbacre
, tp_rib_lbyr    * (intersect_areasqkm / h12_tot_sqkm) / (intersect_areasqkm * 247.105) AS tp_rib_lbacre

, tn_rib_lbyr    * (intersect_areasqkm / h12_tot_sqkm) AS tn_rib_lbyr
, tp_rib_lbyr    * (intersect_areasqkm / h12_tot_sqkm) AS tp_rib_lbyr

, ( tn_rib_lbyr    * 453592 )  * (intersect_lengthkm / huc12_lengthkm) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) AS tn_rib_mgl
, ( tp_rib_lbyr    * 453592 )  * (intersect_lengthkm / huc12_lengthkm) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) AS tp_rib_mgl

-- sep - tn_sep_lbyr
, tn_sep_lbyr     * (intersect_areasqkm / h12_tot_sqkm) / (intersect_areasqkm * 247.105) AS tn_sep_lbacre
, tp_sep_lbyr     * (intersect_areasqkm / h12_tot_sqkm) / (intersect_areasqkm * 247.105) AS tp_sep_lbacre

, tn_sep_lbyr     * (intersect_areasqkm / h12_tot_sqkm) AS tn_sep_lbyr
, tp_sep_lbyr     * (intersect_areasqkm / h12_tot_sqkm) AS tp_sep_lbyr

, ( tn_sep_lbyr    * 453592 )  * (intersect_lengthkm / huc12_lengthkm) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) AS tn_sep_mgl
, ( tp_sep_lbyr    * 453592 )  * (intersect_lengthkm / huc12_lengthkm) / (  CASE WHEN maflowv > 0 THEN maflowv ESLE null END  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) ) AS tp_sep_mgl


FROM datasusqu.nhdxhuc12_rerun AS a--(SELECT * FROM datasusqu.nhdxhuc12_rerun WHERE (intersect_areasqkm / nhd_areasqkm) > 0.05) AS a

-- lulc joins

LEFT JOIN
	( -- lb/year of nutrient loading from cropland
	 SELECT huc12
	  ,sum(nloadeos) AS tn_crp
	  ,sum(ploadeos) AS tp_crp
	  ,sum(sloadeos) AS tss_crp 
	 FROM datasusqu.cbwsm_huc12_loading_sources 
	 WHERE lu_code like 'crp'
	 GROUP BY huc12
	) AS crp
on a.huc12 = crp.huc12
LEFT JOIN
	( -- lb/year of nutrient loading from forest
	 SELECT huc12
	  ,sum(nloadeos) AS tn_fore
	  ,sum(ploadeos) AS tp_fore
	  ,sum(sloadeos) AS tss_fore 
	 FROM datasusqu.cbwsm_huc12_loading_sources 
	 WHERE lu_code like 'fore'
	 GROUP BY huc12
	) AS fore
on a.huc12 = fore.huc12
LEFT JOIN
	( -- lb/year of nutrient loading from impervious non-roads
	 SELECT huc12
	  ,sum(nloadeos) AS tn_inr
	  ,sum(ploadeos) AS tp_inr
	  ,sum(sloadeos) AS tss_inr 
	 FROM datasusqu.cbwsm_huc12_loading_sources 
	 WHERE lu_code like 'inr'
	 GROUP BY huc12
	) AS inr
on a.huc12 = inr.huc12
LEFT JOIN
	( -- lb/year of nutrient loading from impervious roads
	 SELECT huc12
	  ,sum(nloadeos) AS tn_ir
	  ,sum(ploadeos) AS tp_ir
	  ,sum(sloadeos) AS tss_ir 
	 FROM datasusqu.cbwsm_huc12_loading_sources 
	 WHERE lu_code like 'ir'
	 GROUP BY huc12
	) AS ir
on a.huc12 = ir.huc12
LEFT JOIN
	( -- lb/year of nutrient loading from mixed open
	 SELECT huc12
	  ,sum(nloadeos) AS tn_mo
	  ,sum(ploadeos) AS tp_mo
	  ,sum(sloadeos) AS tss_mo 
	 FROM datasusqu.cbwsm_huc12_loading_sources 
	 WHERE lu_code like 'mo'
	 GROUP BY huc12
	) AS mo
on a.huc12 = mo.huc12
LEFT JOIN
	( -- lb/year of nutrient loading from pasture
	 SELECT huc12
	  ,sum(nloadeos) AS tn_pas
	  ,sum(ploadeos) AS tp_pas
	  ,sum(sloadeos) AS tss_pas 
	 FROM datasusqu.cbwsm_huc12_loading_sources 
	 WHERE lu_code like 'pas'
	 GROUP BY huc12
	) AS pas
on a.huc12 = pas.huc12
LEFT JOIN
	( -- lb/year of nutrient loading from tree canopy over impervious
	 SELECT huc12
	  ,sum(nloadeos) AS tn_tci
	  ,sum(ploadeos) AS tp_tci
	  ,sum(sloadeos) AS tss_tci 
	 FROM datasusqu.cbwsm_huc12_loading_sources 
	 WHERE lu_code like 'tci'
	 GROUP BY huc12
	) AS tci
on a.huc12 = tci.huc12
LEFT JOIN
	( -- lb/year of nutrient loading from tree cover over turf grass
	 SELECT huc12
	  ,sum(nloadeos) AS tn_tct
	  ,sum(ploadeos) AS tp_tct
	  ,sum(sloadeos) AS tss_tct 
	 FROM datasusqu.cbwsm_huc12_loading_sources
	 WHERE lu_code like 'tct'
	 GROUP BY huc12
	) AS tct
on a.huc12 = tct.huc12
LEFT JOIN
	( -- lb/year of nutrient loading from turf grass
	 SELECT huc12
	  ,sum(nloadeos) AS tn_tg
	  ,sum(ploadeos) AS tp_tg
	  ,sum(sloadeos) AS tss_tg 
	 FROM datasusqu.cbwsm_huc12_loading_sources 
	 WHERE lu_code like 'tg'
	 GROUP BY huc12
	) AS tg
on a.huc12 = tg.huc12
LEFT JOIN
	( -- lb/year of nutrient loading from water
	 SELECT huc12
	  ,sum(nloadeos) AS tn_wat
	  ,sum(ploadeos) AS tp_wat
	  ,sum(sloadeos) AS tss_wat 
	 FROM datasusqu.cbwsm_huc12_loading_sources 
	 WHERE lu_code like 'wat'
	 GROUP BY huc12
	) AS wat
on a.huc12 = wat.huc12
LEFT JOIN
	( -- lb/year of nutrient loading from floodplain wetlands
	 SELECT huc12
	  ,sum(nloadeos) AS tn_wlf
	  ,sum(ploadeos) AS tp_wlf
	  ,sum(sloadeos) AS tss_wlf 
	 FROM datasusqu.cbwsm_huc12_loading_sources 
	 WHERE lu_code like 'wlf'
	 GROUP BY huc12
	) AS wlf
on a.huc12 = wlf.huc12
LEFT JOIN
	( -- lb/year of nutrient loading from open non-tidal wetlands
	 SELECT huc12
	  ,sum(nloadeos) AS tn_wlo
	  ,sum(ploadeos) AS tp_wlo
	  ,sum(sloadeos) AS tss_wlo 
	 FROM datasusqu.cbwsm_huc12_loading_sources 
	 WHERE lu_code like 'wlo'
	 GROUP BY huc12
	) AS wlo
on a.huc12 = wlo.huc12

-------------------------------------------------------------------------------------------------------------------------

-- wastewater and septic JOINs

/*THIS CALC IS NOT DONE, NEED TO FIGURE OUT CSO POINT SOURCE DATASET*/
LEFT JOIN
	( -- lb/year of nutrient loading from combined sewer overflow, using NPDES outfall locations and their MGD -> mg/L nutrient concentrations
	 SELECT huc12,comid,npdes_id
	  ,sum(tn_lbyr) AS tn_cso_npdes_lbyr
	  ,sum(tp_lbyr) AS tp_cso_npdes_lbyr
	  ,sum(tss_lbyr) AS tss_cso_npdes_lbyr
	  ,sum(tn_mgl) AS tn_cso_npdes_mgl
	  ,sum(tp_mgl) AS tp_cso_npdes_mgl
	  ,sum(tss_mgl) AS tss_cso_npdes_mgl
	 FROM datasusqu.ww_cso 
	 GROUP BY huc12,comid ,npdes_id
	 ORDER BY huc12
	) AS cso_npdes
on a.huc12 = cso_npdes.huc12
LEFT JOIN
	( -- lb/year of nutrient loading from combined sewer overflow, using the CBWSM HUC12 output
	 SELECT huc12
	  ,sum(nloadeos) AS tn_cso_areaonly_lbyr
	  ,sum(ploadeos) AS tp_cso_areaonly_lbyr
	  ,sum(sloadeos) AS tss_cso_areaonly_lbyr
	 FROM datasusqu.cbwsm_huc12_loading_sources 
	 WHERE lu_code like 'cso'
	 GROUP BY huc12
	 ORDER BY huc12
	) AS cso_areaonly
on a.huc12 = cso_areaonly.huc12
/*END THIS CALC IS NOT DONE*/

LEFT JOIN
	( -- lb/year of nutrient loading from industrial wastewater
	 SELECT huc12,comid
	  ,sum(tn_lbyr) AS tn_wwi_lbyr
	  ,sum(tp_lbyr) AS tp_wwi_lbyr
	  ,sum(tss_lbyr) AS tss_wwi_lbyr
	  ,sum(tn_mgl) AS tn_wwi_mgl
	  ,sum(tp_mgl) AS tp_wwi_mgl
	  ,sum(tss_mgl) AS tss_wwi_mgl
	 FROM datasusqu.ww_wwtp 
	 WHERE loadsource like '%Industrial%'
	 GROUP BY huc12,comid
	 ORDER BY comid
	) AS wwi
on a.comid = wwi.comid
LEFT JOIN
	( -- lb/year of nutrient loading from municipal wastewater
	 SELECT comid,huc12
	  ,sum(tn_lbyr) AS tn_wwm_lbyr
	  ,sum(tp_lbyr) AS tp_wwm_lbyr
	  ,sum(tss_lbyr) AS tss_wwm_lbyr
	  ,sum(tn_mgl) AS tn_wwm_mgl
	  ,sum(tp_mgl) AS tp_wwm_mgl
	  ,sum(tss_mgl) AS tss_wwm_mgl
	 FROM datasusqu.ww_wwtp 
	 WHERE loadsource like '%Municipal%'
	 GROUP BY comid,huc12
	 ORDER BY comid
	) AS wwm
on a.comid = wwm.comid
LEFT JOIN
	( -- lb/year of nutrient loading from rapid infiltration basin
	 SELECT huc12
	  ,sum(tn_lbyr) AS tn_rib_lbyr
	  ,sum(tp_lbyr) AS tp_rib_lbyr
	 FROM datasusqu.ww_septic_rapidinfilbasin 
	 WHERE loadsource like '%Rapid%' 
	 GROUP BY huc12
	) AS rib
on a.huc12 = rib.huc12
LEFT JOIN
	( -- lb/year of nutrient loading from septic
	 SELECT huc12
	  ,sum(tn_lbyr) AS tn_sep_lbyr
	  ,sum(tp_lbyr) AS tp_sep_lbyr
	 FROM datasusqu.ww_septic_rapidinfilbasin 
	 WHERE loadsource like '%Septic%' 
	 GROUP BY huc12
	) AS sep
on a.huc12 = sep.huc12

-------------------------------------------------------------------------------------------------------------------------

-- remaining JOINs: stream bed and bank, stream flood plain, shoreline, riparian pasture deposistion and construction JOINs
LEFT JOIN
	( -- lb/year of nutrient loading from construction
	 SELECT huc12
	  ,sum(nloadeos) AS tn_con_lbyr
	  ,sum(ploadeos) AS tp_con_lbyr
	  ,sum(sloadeos) AS tss_con_lbyr 
	  ,sum(nloadeos / (amount)) AS tn_con_lbacre
	  ,sum(ploadeos / (amount)) AS tp_con_lbacre
	  ,sum(sloadeos / (amount)) AS tss_con_lbacre 
	 FROM datasusqu.cbwsm_huc12_loading_sources 
	 WHERE lu_code like 'con' and amount > 0
	 GROUP BY huc12
	) AS con
on a.huc12 = con.huc12
LEFT JOIN
	( -- lb/year of nutrient loading from riparian pasture deposition (direct, unit-less area, load) lb/year
	 SELECT huc12
	  ,sum(nloadeos) AS tn_rpd
	  ,sum(ploadeos) AS tp_rpd
	  ,sum(sloadeos) AS tss_rpd
	 FROM datasusqu.cbwsm_huc12_loading_sources 
	 WHERE lu_code like 'rpd'
	 GROUP BY huc12
	) AS rpd
on a.huc12 = rpd.huc12
LEFT JOIN
	( -- lb/year of nutrient loading from shoreline
	 SELECT huc12
	  ,sum(nloadeos) AS tn_shore_lbyr
	  ,sum(ploadeos) AS tp_shore_lbyr
	  ,sum(sloadeos) AS tss_shore_lbyr
	  ,sum(nloadeos / ((amount+0.0000000000000000000001) * 1.609)) AS tn_shore_lbkm
	  ,sum(ploadeos / ((amount+0.0000000000000000000001) * 1.609)) AS tp_shore_lbkm
	  ,sum(sloadeos / ((amount+0.0000000000000000000001) * 1.609)) AS tss_shore_lbkm
	 FROM datasusqu.cbwsm_huc12_loading_sources 
	 WHERE lu_code like 'shore'
	 GROUP BY huc12
	) AS shore
on a.huc12 = shore.huc12
LEFT JOIN
	( -- lb/year of nutrient loading from the stream bed and stream bank
	 SELECT huc12
	  ,sum(nloadeos) AS tn_sbb_lbyr
	  ,sum(ploadeos) AS tp_sbb_lbyr
	  ,sum(sloadeos) AS tss_sbb_lbyr
	  ,sum(nloadeos / ((amount+0.0000000000000000000001) * 1.609)) AS tn_sbb_lbkm
	  ,sum(ploadeos / ((amount+0.0000000000000000000001) * 1.609)) AS tp_sbb_lbkm
	  ,sum(sloadeos / ((amount+0.0000000000000000000001) * 1.609)) AS tss_sbb_lbkm
	 FROM datasusqu.cbwsm_huc12_loading_sources 
	 WHERE lu_code like 'sbb'
	 GROUP BY huc12
	) AS sbb
on a.huc12 = sbb.huc12
LEFT JOIN
	( -- lb/year of nutrient loading from the stream flood plain
	 SELECT huc12
	  ,sum(nloadeos) AS tn_sfp_lbyr
	  ,sum(ploadeos) AS tp_sfp_lbyr
	  ,sum(sloadeos) AS tss_sfp_lbyr
	  ,sum(nloadeos / ((amount+0.0000000000000000000001) * 1.609)) AS tn_sfp_lbkm
	  ,sum(ploadeos / ((amount+0.0000000000000000000001) * 1.609)) AS tp_sfp_lbkm
	  ,sum(sloadeos / ((amount+0.0000000000000000000001) * 1.609)) AS tss_sfp_lbkm
	 FROM datasusqu.cbwsm_huc12_loading_sources 
	 WHERE lu_code like 'sfp'
	 GROUP BY huc12
	) AS sfp
on a.huc12 = sfp.huc12
WHERE a.nord is not null 
and a.intersect_areasqkm / a.nhd_areasqkm > 0.05
	--and comid = 8538463
GROUP BY a.comid ,a.huc12

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

ORDER BY a.comid , a.huc12

) AS t1

JOIN spatial.nhdplus_maregion AS nhd
on t1.nord = nhd.nord

GROUP BY t1.comid
,	nhd.maflowv	
,	nhd.catchment

ORDER BY t1.comid ) AS t2

LEFT JOIN spatial.nhdplus_maregion AS nhd
on t2.comid = nhd.comid

LEFT JOIN datasusqu.nhdplus_luproportions_rerun AS lu_nhd
on t2.comid = lu_nhd.comid

--WHERE comid = 8538463

ORDER BY t2.comid

;

/*  CACHE THE TABLE  */

DROP TABLE IF EXISTS analysissusqu.cache_final_lateralshed_04;
CREATE TABLE analysissusqu.cache_final_lateralshed_04
as
SELECT * FROM analysissusqu.final_lateralshed_04;

alter TABLE analysissusqu.cache_final_lateralshed_04 add constraint pk_cache_final_lateralshed_04 primary key (nord);

alter TABLE analysissusqu.cache_final_lateralshed_04 add column huc12 varchar(12);
update analysissusqu.cache_final_lateralshed_04 AS a set huc12 = b.huc12
FROM datasusqu.nhdxhuc12_rerun AS b
WHERE a.comid = b.comid
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
SELECT idx.nord,idx.nordstop
,sum(tn_wwtp_mgl) AS tn_wwtp_mgl
,sum(tp_wwtp_mgl) AS tp_wwtp_mgl
,sum(tss_wwtp_mgl) AS tss_wwtp_mgl
FROM (
SELECT a.huc12, a.comid,a.nord,a.nordstop
, sum(tn_wwtp_lbyr)  * (intersect_areasqkm / h12_tot_sqkm) AS tn_wwtp_lbyr
, sum(tp_wwtp_lbyr)  * (intersect_areasqkm / h12_tot_sqkm) AS tp_wwtp_lbyr
, sum(tss_wwtp_lbyr) * (intersect_areasqkm / h12_tot_sqkm) AS tss_wwtp_lbyr
, sum(tn_wwtp_mgl) AS tn_wwtp_mgl
, sum(tp_wwtp_mgl) AS tp_wwtp_mgl
, sum(tss_wwtp_mgl) AS tss_wwtp_mgl
FROM datasusqu.nhdxhuc12_rerun AS a
INNER JOIN
	( -- lb/year of nutrient loading from wastewater point sources
	 SELECT huc12,comid
	  ,sum(tn_lbyr) AS tn_wwtp_lbyr
	  ,sum(tp_lbyr) AS tp_wwtp_lbyr
	  ,sum(tss_lbyr) AS tss_wwtp_lbyr
	  ,sum(tn_mgl) AS tn_wwtp_mgl
	  ,sum(tp_mgl) AS tp_wwtp_mgl
	  ,sum(tss_mgl) AS tss_wwtp_mgl
	 FROM datasusqu.ww_wwtp 
	 WHERE loadsource like '%Industrial%' or loadsource like '%Municipal%'
	 GROUP BY huc12,comid
	 ORDER BY comid
	) AS wwtp
on a.comid = wwtp.comid
GROUP BY a.comid, a.huc12
ORDER BY a.comid, a.huc12
) AS t1
LEFT JOIN spatial.nhdplus_maregion_idx AS idx
on t1.nord between idx.nord and idx.nordstop
GROUP BY idx.nord
*/

--- Final Watershed Nested Set Upstream Calculations, takes into factor extinction coef and upstream point sources

-- mg/L converts lb to mg, ft^(3) to L, seconds to year. Dividing lb/yr BY mean annual flow -> mean annual concentration

DROP VIEW IF EXISTS analysissusqu.final_watershed_04;
CREATE VIEW analysissusqu.final_watershed_04
as
SELECT t2.gnis_name,t1.nord, t2.nordstop, t2.comid, t2.huc12, t2.flow_cfs

,t1.tn_lbyr_ups AS tn_lbyr
, CASE WHEN flow_cfs < 0 THEN -9999
	ESLE ( t1.tn_lbyr_ups * 453592 ) / (  flow_cfs  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) )
	END AS tn_mgl
,tn_lbyr_surfaceload_ups / (sqm_ups / 4046.86) AS tn_lbacre

,t1.tp_lbyr_ups AS tp_lbyr
, CASE WHEN flow_cfs < 0 THEN -9999
	ESLE ( t1.tp_lbyr_ups * 453592 ) / (  flow_cfs  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) )
	END AS tp_mgl
,tp_lbyr_surfaceload_ups / (sqm_ups / 4046.86) AS tp_lbacre

,t1.tss_lbyr_ups AS tss_lbyr
, CASE WHEN flow_cfs < 0 THEN -9999
	ESLE ( t1.tss_lbyr_ups * 453592 ) / (  flow_cfs  * 28.3168 * ( 365.25 * 24 * 60 * 60  ) )
	END AS tss_mgl
,tss_lbyr_surfaceload_ups / (sqm_ups / 4046.86) AS tss_lbacre

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
FROM (
	SELECT idx.nord
	,sum(tn_lbyr  * tn_extict_coef)  AS tn_lbyr_ups
	,sum(tp_lbyr  * tp_extict_coef)  AS tp_lbyr_ups
	,sum(tss_lbyr * tss_extict_coef) AS tss_lbyr_ups

	,sum(tn_lbyr_surfaceload  * tn_extict_coef)    AS tn_lbyr_surfaceload_ups
	,sum(tp_lbyr_surfaceload  * tp_extict_coef)    AS tp_lbyr_surfaceload_ups
	,sum(tss_lbyr_surfaceload  * tss_extict_coef)  AS tss_lbyr_surfaceload_ups

	FROM (SELECT nord, tn_lbyr, tp_lbyr, tss_lbyr,tn_lbyr_surfaceload,tp_lbyr_surfaceload,tss_lbyr_surfaceload,tn_extict_coef,tp_extict_coef,tss_extict_coef FROM analysissusqu.cache_final_lateralshed_04 GROUP BY nord ORDER BY nord) AS a
	JOIN spatial.nhdplus_maregion_idx AS idx
	on a.nord between idx.nord and idx.nordstop
	GROUP BY idx.nord
	ORDER BY idx.nord
     ) AS t1
LEFT JOIN ( 
	SELECT a.*, b.sqm_ups, b.crp_sqm_ups, b.fore_sqm_ups, b.inr_sqm_ups, b.ir_sqm_ups, 
	b.mo_sqm_ups, b.pas_sqm_ups, b.tci_sqm_ups, b.tct_sqm_ups, b.tg_sqm_ups, 
	b.wat_sqm_ups, b.wlf_sqm_ups, b.wlo_sqm_ups, b.wlt_sqm_ups 
	FROM analysissusqu.cache_final_lateralshed_04 AS a
	LEFT JOIN datasusqu.nhdplus_luproportions_rerun AS b
	on a.nord = b.nord 
      ) AS t2
on t1.nord = t2.nord
GROUP BY t2.gnis_name,t1.nord, t2.nordstop, t2.comid, t2.huc12, t2.flow_cfs
	,t1.tn_lbyr_ups,t1.tp_lbyr_ups,t1.tss_lbyr_ups,t1.tn_lbyr_surfaceload_ups,t1.tp_lbyr_surfaceload_ups,t1.tss_lbyr_surfaceload_ups
	,t2.sqm_ups,t2.crp_sqm_ups,t2.fore_sqm_ups,t2.inr_sqm_ups,t2.ir_sqm_ups
	,t2.mo_sqm_ups,t2.pas_sqm_ups,t2.tci_sqm_ups,t2.tct_sqm_ups,t2.tg_sqm_ups
	,t2.wat_sqm_ups,t2.wlf_sqm_ups,t2.wlo_sqm_ups,t2.wlt_sqm_ups
	,t2.geom,t2.catchment
;

/* CACHE THE TABLE */

DROP TABLE IF EXISTS analysissusqu.cache_final_watershed_03;
CREATE TABLE analysissusqu.cache_final_watershed_03
as
SELECT * FROM analysissusqu.final_watershed_03;

SELECT * FROM analysissusqu.cache_final_watershed_03 WHERE nord is null;

alter TABLE analysissusqu.cache_final_watershed_03 add constraint pk_cache_final_watershed_03 primary key (nord);

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

SELECT sum(st_area(catchment)/2590000)
FROM analysissusqu.cache_final_lateralshed_04;
-- ~ 65,500 square miles
SELECT sum(st_area(catchment)/2590000)
FROM datasusqu.z_temp_srat_comparison;
-- ~ 14,550 square miles
SELECT count(*) FROM analysissusqu.cache_final_lateralshed_04;
SELECT count(*) FROM analysissusqu.cache_final_watershed_03 ;

------------------------------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS analysissusqu.cache_final_watershed_04;
CREATE TABLE analysissusqu.cache_final_watershed_04
as
SELECT * FROM analysissusqu.final_watershed_04;

--SELECT * FROM analysissusqu.cache_final_watershed_04 WHERE nord is null;

alter TABLE analysissusqu.cache_final_watershed_04 add constraint pk_cache_final_watershed_04 primary key (nord);

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

SELECT sum(st_area(catchment)/2590000)
FROM analysissusqu.cache_final_lateralshed_04;
-- ~ 65,500 square miles
SELECT sum(st_area(catchment)/2590000)
FROM datasusqu.z_temp_srat_comparison;
-- ~ 14,550 square miles
SELECT count(*) FROM analysissusqu.cache_final_lateralshed_04;
SELECT count(*) FROM analysissusqu.cache_final_watershed_04 ;











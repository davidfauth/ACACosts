/**
 * healthcareCosts
 */
 
/** 
 * Parameters - set default values here; you can override with -p on the command-line.
 */
 
%default INPUT_PATH '/Users/davidfauth/healthcareCosts/QHP_Individual_Medical_Landscape.csv'
%default OUTPUT_PATH '/Users/davidfauth/MortarHealthCareCostsOut'
%default OUTPUT_PATH_PLANDELTAS '/Users/davidfauth/MortarHealthCareCostsOut/PlanDeltas'
%default OUTPUT_PATH_PLANSTANDARD '/Users/davidfauth/MortarHealthCareCostsOut/PlanStandard'

/**
 * User-Defined Functions (UDFs)
 */

-- Load the input data from the CSV file
raw_data = LOAD '$INPUT_PATH' 
          USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER')
             AS (State:chararray,
				County:chararray,
				MetalLevel:chararray,
				IssuerName:chararray,
				PlanMarketingName:chararray,
				PlanType:chararray,
				RatingArea:chararray,
				PremiumAdultIndividualAge27:double,
				PremiumAdultIndividualAge50:double,
				PremiumFamily:double,
				PremiumSingleParentFamily:double,
				PremiumCouple:double,
				PremiumChild:double);
				
-- Limit to a subset of data				
	
initialSubsetData = FOREACH raw_data GENERATE State as newState,
MetalLevel as newMetalLevel,
PremiumAdultIndividualAge27 as newPremiumAdultIndividualAge27,
PremiumAdultIndividualAge50 as newPremiumAdultIndividualAge50,
PremiumFamily as newPremiumFamily,
PremiumSingleParentFamily as newPremiumSingleParentFamily,
PremiumCouple as newPremiumCouple,
PremiumChild as newPremiumChild; 

noCurrencySymbol = DISTINCT initialSubsetData;
	
/* Group together identical tuples */
PlansByMetalLevel = GROUP noCurrencySymbol BY (newMetalLevel, newState);

--calculate Max, Min and Avg costs
costsByPlansPAA27 = FOREACH PlansByMetalLevel GENERATE FLATTEN(group), 
	COUNT(noCurrencySymbol) as planCount,
    AVG(noCurrencySymbol.newPremiumAdultIndividualAge27) as avgPremiumAdultAge27,
	MIN(noCurrencySymbol.newPremiumAdultIndividualAge27) as minPremiumAdultAge27,
	MAX(noCurrencySymbol.newPremiumAdultIndividualAge27) as maxPremiumAdultAge27,
	AVG(noCurrencySymbol.newPremiumAdultIndividualAge50) as avgPremiumAdultAge50,
	MIN(noCurrencySymbol.newPremiumAdultIndividualAge50) as minPremiumAdultAge50,
	MAX(noCurrencySymbol.newPremiumAdultIndividualAge50) as maxPremiumAdultAge50,
	AVG(noCurrencySymbol.newPremiumFamily) as avgPremiumAdultAgePFAM,
	MIN(noCurrencySymbol.newPremiumFamily) as minPremiumAdultAgePFAM,
	MAX(noCurrencySymbol.newPremiumFamily) as maxPremiumAdultAgePFAM,
    AVG(noCurrencySymbol.newPremiumSingleParentFamily) as avgPremiumAdultAgePSPFAM,
    MIN(noCurrencySymbol.newPremiumSingleParentFamily) as minPremiumAdultAgePSPFAM,
	MAX(noCurrencySymbol.newPremiumSingleParentFamily) as maxPremiumAdultAgePSPFAM,
    AVG(noCurrencySymbol.newPremiumCouple) as avgPremiumAdultAgePC,
    MIN(noCurrencySymbol.newPremiumCouple) as minPremiumAdultAgePC,
	MAX(noCurrencySymbol.newPremiumCouple) as maxPremiumAdultAgePC,
    AVG(noCurrencySymbol.newPremiumChild) as avgPremiumAdultAgePCh,
    MIN(noCurrencySymbol.newPremiumChild) as minPremiumAdultAgePCh,
	MAX(noCurrencySymbol.newPremiumChild) as maxPremiumAdultAgePCh;

	--calculate deltas
deltasByPlan = FOREACH costsByPlansPAA27 GENERATE newMetalLevel, newState,
avgPremiumAdultAge27 - minPremiumAdultAge27 as deltaMinAvgAge27,
avgPremiumAdultAge50 - minPremiumAdultAge50 as deltaMinAvgAge50,
avgPremiumAdultAgePFAM - minPremiumAdultAgePFAM as deltaMinAvgAgePFAM,
avgPremiumAdultAgePSPFAM - minPremiumAdultAgePSPFAM as deltaMinAvgAgePSPFAM,
avgPremiumAdultAgePC - minPremiumAdultAgePC as deltaMinAvgAgePC,
avgPremiumAdultAgePCh- minPremiumAdultAgePCh as deltaMinAvgAgePCh;

-- calculate variance and Standard Deviations
mean = foreach PlansByMetalLevel {
        sum27 = SUM(noCurrencySymbol.newPremiumAdultIndividualAge27);
        sum50 = SUM(noCurrencySymbol.newPremiumAdultIndividualAge50);
        sumPFAM = SUM(noCurrencySymbol.newPremiumFamily);
        sumPSPFAM = SUM(noCurrencySymbol.newPremiumSingleParentFamily);
        sumPC = SUM(noCurrencySymbol.newPremiumCouple);
        sumPCh = SUM(noCurrencySymbol.newPremiumChild);
		count = COUNT(noCurrencySymbol);
        generate flatten(noCurrencySymbol), sum27/count as avg27, sum50/count as avg50, 
        sumPFAM/count as avgPFAM, sumPSPFAM/count as avgPSPFAM, sumPC/count as avgPC, 
		sumPCh/count as avgPCh, count as count;
};

tmp = foreach mean {
    dif27 = (newPremiumAdultIndividualAge27 - avg27) * (newPremiumAdultIndividualAge27 - avg27) ;
    dif50 = (newPremiumAdultIndividualAge50 - avg50) * (newPremiumAdultIndividualAge50 - avg50) ;
    difPFAM = (newPremiumFamily - avgPFAM) * (newPremiumFamily - avgPFAM) ;
    difPSPFAM = (newPremiumSingleParentFamily - avgPSPFAM) * (newPremiumSingleParentFamily - avgPSPFAM) ;
    difPC = (newPremiumCouple - avgPC) * (newPremiumCouple - avgPC) ;
    difPCh = (newPremiumChild - avgPCh) * (newPremiumChild - avgPCh) ;
     generate newMetalLevel, newState, count, dif27 as dif27,
	dif50 as dif50, difPFAM as difPFAM, difPSPFAM as difPSPFAM, difPC as difPC, difPCh as difPCh;
};


grp = group tmp by (newMetalLevel, newState);
standard_tmp = foreach grp generate flatten(tmp), SUM(tmp.dif27) as sqr_sum27, SUM(tmp.dif50) as sqr_sum50,
	SUM(tmp.difPFAM) as sqr_sumPFAM, SUM(tmp.difPSPFAM) as sqr_sumPSPFAM, SUM(tmp.difPC) as sqr_sumPC, 
	SUM(tmp.difPCh) as sqr_sumPCh; 
	
standard = foreach standard_tmp generate newState, newMetalLevel,
sqr_sum27 / count as variance27, SQRT(sqr_sum27 / count) as standard27,
sqr_sum50 / count as variance50, SQRT(sqr_sum50 / count) as standard50,
sqr_sumPFAM / count as variancePFAM, SQRT(sqr_sumPFAM / count) as standardPFAM,
sqr_sumPSPFAM / count as variancePSPFAM, SQRT(sqr_sumPSPFAM / count) as standardPSPFAM,
sqr_sumPC / count as variancePC, SQRT(sqr_sumPC / count) as standardPC,
sqr_sumPCh / count as variancePCh, SQRT(sqr_sumPCh / count) as standardPCh;

distinctStandard = DISTINCT standard;

-- remove any existing data
rmf $OUTPUT_PATH;

-- store the results
STORE costsByPlansPAA27 INTO '$OUTPUT_PATH' USING PigStorage('|');
STORE deltasByPlan INTO '$OUTPUT_PATH_PLANDELTAS' USING PigStorage('|');
STORE distinctStandard INTO '$OUTPUT_PATH_PLANSTANDARD' USING PigStorage('|');

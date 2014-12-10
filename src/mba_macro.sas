/*----------------------------------------------------------------------*
 *                    Market Basket Analysis Macro                      *
 * Copyright(c) 2014 by Clario Analytics, Eden Prairie, MN              *
 * Please feel free to use and modify this code in whatever manner      *
 * you deem appropriate. All Clario asks is that, in return, you share  *
 * valuable enhancements/efficiencies so that Clario can integrate and  *
 * continue sharing sharing them.                                       *
 *----------------------------------------------------------------------*
 *  Macro Definitions:                                                  *
 *  Define lib, set, basket_dimension, and analysis_unit where 'lib'    *
 *  is the library containing the transactional dataset (use libname    *
 *  if necessary), 'set' is the transactional dataset name,             *
 *  'basket_dimension' is the unique basket dimension identifier        *
 *  (i.e. customer identifier) and 'analysis_unit' is the unique        *
 *  anlysis unit identifier (i.e. product identifier).                  *
 *----------------------------------------------------------------------*
 *  Output:                                                             *
 *  'mba_results' dataset in the defined library containing             *
 *  unfiltered affinity findings.                                       *
 *----------------------------------------------------------------------*
 *  Created:  01 Sep 2002                                               *
 *----------------------------------------------------------------------*/

/*libname libref 'library location';*/ 

%let lib = Library Name;  *Library Name;
%let set = Dataset Name;  *Dataset Name;
%let basket_dimension = CUSTOMER_ID;  *Basket Dimension Identifier (ie customer id);
%let analysis_unit = PRODUCT_ID;  *Analysis Unit Identifier (ie product id);

*Modifications are seldom required below this point;

*Builds dataset containing distinct analysis units and basket dimension
frequencies for each;
proc sql;
	create table &lib..analysis_unit as
    select
    &analysis_unit,
    count(distinct(&basket_dimension)) as ANALYSIS_UNIT_FREQ
    from &lib..&set
    group by &analysis_unit;
quit;
*Builds dataset containing distinct basket dimensions and analysis unit
frequencies for each;
proc sql;
	create table &lib..basket_dimension as
    select
    &basket_dimension,
    count(&analysis_unit) as &basket_dimension._freq
    from &lib..&set
    group by &basket_dimension;
quit;
*Builds a new dataset containing only those basket dimensions with more
than one distinct analysis unit to accelerate processing;
proc sql;
	create table &lib..&set._reduced as
    select a.&basket_dimension, a.&analysis_unit
    from &lib..&set a, &lib..basket_dimension b
    where a.&basket_dimension = b.&basket_dimension
    and b.&basket_dimension._freq > 1;
quit;
*Creates simple indexes to accelerate processing;
proc datasets library = &lib;
	modify &set._reduced;
    index create &analysis_unit;
    index create &basket_dimension;
quit;
*Defines a macro variable containing a count of distinct basket dimensions;
%let dsid=%sysfunc(open(&lib..basket_dimension,i));
	%let tot_basket_dimensions=%sysfunc(attrn(&dsid,nobs));
%let rc=%sysfunc(close(&dsid));
%put ;%put Count of distinct &basket_dimension: &tot_basket_dimensions;%put ;
*Defines a macro variable containing a count of distinct analysis units;
%let dsid=%sysfunc(open(&lib..analysis_unit,i));
    %let tot_analysis_units=%sysfunc(attrn(&dsid,nobs));
%let rc=%sysfunc(close(&dsid));
%put ;%put Count of distinct &analysis_unit: &tot_analysis_units;%put ;

%macro marketbasket;
	%do analysis_unit_nb = 1 %to &tot_analysis_units;
		%*Defines macro variables containing current iteration analysis unit
          and analysis unit frequency;
        data _null_;
        	set &lib..analysis_unit;
            if _n_ = &analysis_unit_nb then do;
            	call symput ('curr_analysis_unit',compress(&analysis_unit));
            	call symput ('curr_analysis_unit_freq',ANALYSIS_UNIT_FREQ);
            end;
        run;
        %put ;%put >>> Working on analysis unit #&analysis_unit_nb (&curr_analysis_unit);
        options notes;
        %*Builds a new dataset containing all analysis units for basket dimensions 
		containing the current analysis unit (aka basket donors);
        proc sql;
        	create table &lib..basket_donors as
            select
            &basket_dimension, &analysis_unit
            from &lib..&set._reduced
            where &basket_dimension in(select
            &basket_dimension from &lib..&set._reduced
            where &analysis_unit = /*"*/&curr_analysis_unit/*"*/);
        quit;
        options nonotes;
        %*Counts frequency of co-occurance between the current analysis unit and 
		  all other analysis units;
        proc sql;
        	create table &lib..co_occurance as
            select
            &analysis_unit as ASSOC_ANALYSIS_UNIT,
			count(distinct(&basket_dimension)) as FREQ_CO_OCCUR
            from &lib..basket_donors
            where &analysis_unit ^= /*"*/&curr_analysis_unit/*"*/
            group by &analysis_unit;
        quit;
        %*Adds variables to co_occurance dataset for later affinity calculations;
        data &lib..co_occurance;
        	set &lib..co_occurance;
            ANALYSIS_UNIT_FREQ = &curr_analysis_unit_freq;
            TOT_BASKET_DIMENSIONS = &tot_basket_dimensions;
        run;
        %*Affinity calculations;
        proc sql;
            create table &lib..affinity_calc as
            select
            b.ASSOC_ANALYSIS_UNIT,
            b.FREQ_CO_OCCUR,
            b.TOT_BASKET_DIMENSIONS,
            b.ANALYSIS_UNIT_FREQ,
            a.ANALYSIS_UNIT_FREQ as ASSOC_ANALYSIS_UNIT_FREQ,
            (b.FREQ_CO_OCCUR/b.ANALYSIS_UNIT_FREQ) as CONFIDENCE,
            (b.FREQ_CO_OCCUR/b.TOT_BASKET_DIMENSIONS) as SUPPORT,
            ((ASSOC_ANALYSIS_UNIT_FREQ)/b.TOT_BASKET_DIMENSIONS) as EXPECTED_CONFIDENCE,
            ((calculated CONFIDENCE)/(calculated EXPECTED_CONFIDENCE)) as LIFT
            from &lib..analysis_unit a, &lib..co_occurance b
            where a.&analysis_unit = b.ASSOC_ANALYSIS_UNIT;
        quit;
		%*Descriptive variable added to output dataset;
        data &lib..affinity_calc;
            set &lib..affinity_calc;
        	ANALYSIS_UNIT = /*"*/&curr_analysis_unit/*"*/;
        run;
		%*Reorder variables and gather results;
        proc sql;
        	create table &lib..affinity_calc as
            select
            ANALYSIS_UNIT, ANALYSIS_UNIT_FREQ, ASSOC_ANALYSIS_UNIT, 
			ASSOC_ANALYSIS_UNIT_FREQ,
            FREQ_CO_OCCUR, TOT_BASKET_DIMENSIONS,
            SUPPORT, CONFIDENCE, EXPECTED_CONFIDENCE, LIFT
            from &lib..affinity_calc
            order by LIFT desc;
        quit;
		%if &analysis_unit_nb = 1 %then %do;
			%if %sysfunc(exist(&lib..mba_results)) %then %do;
				proc delete data = &lib..mba_results;
  				run;
			%end;
        	data &lib..mba_results;
            	set &lib..affinity_calc;
          	run;
        %end;
        %else %do;
        	data &lib..mba_results;
          		set &lib..mba_results &lib..affinity_calc;
        	run;
        %end;
	%end;
	proc delete data = &lib..analysis_unit &lib..basket_dimension &lib..&set._reduced
			&lib..basket_donors &lib..co_occurance &lib..affinity_calc;
	run;
%mend marketbasket;
%marketbasket

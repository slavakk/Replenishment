/* 
+-------------+--------------------------------------------------------------------------------------------------------+
| SYSTEM:     | Integrated Prognos Analytics                                                                           |
| APPLICATION:| REPLENISHMENT                                                                                          |
| OS:         | Windows, Unix                                                                                          |
+----------------------------------------------------------------------------------------------------------------------+
| DESCRIPTION:  													                                                   |
+----------------------------------------------------------------------------------------------------------------------+
| SAS VERSION: SAS 9.3                                                                                                 |
+----------------------------------------------------------------------------------------------------------------------+
| Developed by Prognos Inc. R&D 2016                                                                                   |
-----------------------------------------------------------------------------------------------------------------------+
*/

%macro r_collect / store secure;

/*opening the log file*/
/**************************************************/
proc printto log="C:\Users\Slava\Documents\My_Logs\r_collect_&bu..log" new;
run;
/****************************************************/

%local repl_holdouts         /* archived holdouts to be used in calculating safety stock */
	   archive_list          /* the list of time data points driven from macro 'relp_holdout' for which data (output ot final output) exists */
	   bound                 /* macro used in the condition of the while loop based on the sign (+/-) of step*/
	   to                    /* variable used in macro _cmap_ */
	   return                /* this macro has value 1 if the data (output or final output) was not uncompressed; it has value 0, otherwise */
	   num                   /* # of elements in archive_list */
	   w_step
	   w_end
	   w
       ;

/*assigning value to macro variable repl_holdouts*/
/****************************************************/
libname repl_tab "C:\Users\Slava\Documents\Replaneshment_Peter\Replenishment_Tables";
data _null_;
	 set repl_tab.io_user_bu_params(keep= name default
							       where=(name="repl_holdouts")
							        )
	 ;
	 call symput("repl_holdouts", default);
run;
/****************************************************/

%_global_params_(master_job_name=FORECAST, log_file_name=C:\Users\Slava\Documents\My_Logs\r_collect_&bu..log); 
run; 

/* asigning values to macro variables 'bound' and 'to' */
/*******************************************************/
%if %scan(&repl_holdouts,3,' ') > 0 %then %do;
    %let bound= <= %scan(&repl_holdouts,2,' ');
%end;
%else %do;
    %let bound= >= %scan(&repl_holdouts,2,' ');
%end;

%let to=%scan(&repl_holdouts,1,' ');
/*******************************************************/

/*uncompressing the data and updating the value of macro variable 'archive_list'*/
/*********************************************************************************************************************************************/
/*********************************************************************************************************************************************/
%do %while(&to &bound);

    /*uncompressing 'xxxx_final_output'*/
    /**********************************************************************************************************************************/
    %_uncompress_(file_path=&archive_path&bu&separater.forecast&separater&to&separater.output&separater.f_b&bu._final_output.sas7bdat,
                  mvar=return
                  );
    run;
	
	/**********************************************************************************************************************************/

	/*if uncompressing was sucsessful*/
    /****************************************************************************************************************************/
    %if &return=0 %then %do;
	  
	    /*updating value of macro variable 'archive_list' and assigning value to macro variable 'input_&num'*/
	    /************************************************************************************************************************/
        %let archive_list=&archive_list &to;
		%let num = %_countw_(sentence=&archive_list);
		%local input_&num; 
        %let input_&num=&archive_path&bu&separater.forecast&separater&to&separater.output&separater.f_b&bu._final_output.sas7bdat; 
        /************************************************************************************************************************/

	%end;
    /****************************************************************************************************************************/

    /*uncompressing 'xxxx_output'*/
    /************************************************************************************************************************************/
	%if &return=1 %then %do;

        /*uncompressing 'xxxx_output'*/
        /***************************************************************************************************************************/
        %_uncompress_(file_path=&archive_path&bu&separater.forecast&separater&to&separater.output&separater.f_b&bu._output.sas7bdat,
                      mvar=return
                      );
        run;
        /***************************************************************************************************************************/

		/*if uncompressing was sucsessful*/
		/****************************************************************************************************************************/
        %if &return=0 %then %do;

			/*updating value of macro variable 'archive_list' and assigning value to macro variable 'input_&num'*/
			/********************************************************************************************************************/
            %let archive_list=&archive_list &to;
            %let num = %_countw_(sentence=&archive_list);
		    %local input_&num;
            %let input_&num=&archive_path&bu&separater.forecast&separater&to&separater.output&separater.f_b&bu._output.sas7bdat; 
            /********************************************************************************************************************/

		%end;
		/****************************************************************************************************************************/

	%end;
	/************************************************************************************************************************************/
	
	/*assigning value to w_step and creating variable w_end*/
	/**********************************************************************/
	%let w_step = %eval(&t_review + &t_lead + &t_sales - 1);
	
	%_cmap_(from=fiscal, 
            to=fiscal, 
            start=%scan(&archive_list,%_countw_(sentence=&archive_list)), 
            step=&w_step, 
            mvar=w_end
            );
    run;
    /**********************************************************************/

	/*loading hash object and updating variable w */
	/************************************************************************************************************************/
	%_list_(from=%scan(&archive_list,%_countw_(sentence=&archive_list)), to=&w_end, step=,order=a, type=fiscal, mvar=list);
	run;  

	if _n_ = 1 then do;
		declare hash w_&num(dataset:"&&input_&num");
		w_&num..definekey(%_comma_sep_(&f_geo_level &f_prod_level, quoted=1));
		w_&num..definedata(%_comma_sep_(&list, quoted=1));
		w_&num..definedone();
	end;

	%let w = &w &list;
	/************************************************************************************************************************/

	/*updating variable &to for the while loop*/
	/*********************************************/
	%_cmap_(from=fiscal, 
            to=fiscal, 
            start=&to, 
            step=%scan(&repl_holdouts,3,' '), 
            mvar=to
            );
    run;
    /*********************************************/

%end;
/*********************************************************************************************************************************************/
/*********************************************************************************************************************************************/

/*if none of the 'xxxx_final_output' or 'xxxx_output' from the archive were uncompressed*/
/********************************************************************************************/
%if %length(&archive_list)=0 %then %do;
    %_log_message_(text=PROGNOSERROR: BASED ON THE VALUE OF PARAMETER REPL_HOLDOUTS);
    %_log_message_(text=PROGNOSERROR: NONE OF THE DATA SETS WERE UNCOMPRESSED.);
    %_log_message_(text=PROGNOSERROR: THIS ERROR MESSAGE WAS ISSUED BY MACRO R_COLLECT.);

	proc printto;
	run;
    %_update_status_(log_file_name=r_collect_&bu);
    %_abort_(rc=2);
%end;
/********************************************************************************************/


proc printto;
run;

    
%return;  
/**********************************************************************************/



/*time data point at which we calculate replenishment statistics*/
/****************************************************************/
%let rep_time=%scan(&repl_holdouts,1); /**?**/
/****************************************************************/


/*collecting data, calculating errors and normalizing them */
/************************************************************************************************/
data stat.r_b&bu._collect(keep=&sales_low_hierarchy
                               error_: 
                          );
set input.f_b&bu._sales(keep=&sales_low_hierarchy/*we might change it*/ 
                             _&rep_time
					    rename=(_&rep_time=actual)
                        );
attrib %do i=1 %to %_countw_(sentence=&archive_list_refine);
           error_&i
       %end;
	   length=8 /*hard coded*/+
;
array error{*} error_: ;
if _n_=1 then do;
   %do i=1 %to %_countw_(sentence=&archive_list_refine);
       declare hash h&i("&&input&i(keep=&sales_low_hierarchy where=(scan(type,2)='forecast'))");
       h&i..definekey();
       h&i..definedata();
       h&i..definedone(); 
   %end;
end;
%do i=1 %to %_countw_(sentence=&archive_list_refine);
    if h&i..find()=0 then error_&i=actual-&rep_time; 
%end;

/*normalizing*/
/************************************************************/
%do i=1 %to %_countw_(sentence=&archive_list_refine);
    error_&i=(error_&i-mean(of error{*}))/std(of error{*}); 
%end;
/************************************************************/

run;
/************************************************************************************************/

/*compressing data*/
/*******************************************************************************************************/
%do i=1 %to %_countw_(sentence=&archive_list_refine);
    %_compress_(folder_path=&archive_path&bu&separater.forecast&separater%scan(&archive_list_refine,&i));
%end;
/*******************************************************************************************************/
/*	%_compress_(folder_path=&&input_&num);*/
%mend r_collect;

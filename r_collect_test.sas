/* 
+-------------+--------------------------------------------------------------------------------------------------------+
| SYSTEM:     | Integrated Prognos Analytics                                                                           |
| APPLICATION:| REPLENISHMENT                                                                                          |
| OS:         | Windows, Unix                                                                                          |
+----------------------------------------------------------------------------------------------------------------------+
| DESCRIPTION: testing version												                                           |
+----------------------------------------------------------------------------------------------------------------------+
| SAS VERSION: SAS 9.3                                                                                                 |
+----------------------------------------------------------------------------------------------------------------------+
| Developed by Prognos Inc. R&D 2016                                                                                   |
-----------------------------------------------------------------------------------------------------------------------+
*/

%macro r_collect / store secure minoperator;

/*opening the log file*/
/**************************************************/
proc printto log="C:\Users\Slava\Documents\My_Logs\r_collect_&bu..log" new;
run;
/****************************************************/

%local repl_holdouts         /*archived holdouts to be used in calculating safety stock*/
	   archive_list          /*the list of time data points driven from macro 'relp_holdout' for which data (output ot final output) exists*/
	   bound                 /*macro used in the condition of the while loop based on the sign (+/-) of step*/
	   to                    /*variable used in macro _cmap_*/
	   return                /*this macro has value 1 if the data (output or final output) was not uncompressed; it has value 0, otherwise*/
	   num                   /*# of elements in archive_list*/
       total_win             /*union of win_num's*/
	   i					 /*counter*/
	   win_last              /*last fiscal date in forecast window 'win_num', determined by 'lag'*/
	   forecast_last         /*last available fiscal date for a given forecast*/
	   sales_last            /*last available fiscal date from 'input.f_b&bu._sales'*/
	   missing_list          /*missing fiscal dates from total_win*/
       ;


%_global_params_(master_job_name=FORECAST, log_file_name=C:\Users\Slava\Documents\My_Logs\r_collect_&bu..log); 
run;

/*macro variables that are needed*/
/*************************************/
%let repl_holdouts=200901 200902 1;
%let t_review = 1;
%let t_lead = 1;
%let t_sales= 1;
%let agg_lvl = %_remove_(sentence=&f_geo_level &f_prod_level, remove_list=upc, modifier=e);
/*************************************/

/*asigning values to macro variables 'bound' and 'to'*/
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
	  
	    /*1.updating value of macro variable 'archive_list'*/
        /*2.assigning value to macro variables 'input_&num' and '_&to'*/
		/*3.assigning libname*/
	    /************************************************************************************************************************/
        %let archive_list=&archive_list &to;
        %let num = %_countw_(sentence=&archive_list);
		%local input_&num 
               _&to
        ;
	    libname _&to "&archive_path&bu&separater.forecast&separater&to&separater.output&separater.";
        %let input_&num=_&&to..f_b&bu._final_output; 
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

			/*1.updating value of macro variable 'archive_list'*/
            /*2.assigning value to macro variables 'input_&num' and '_&to'*/
		    /*3.assigning libname*/
			/********************************************************************************************/
            %let archive_list=&archive_list &to;
            %let num = %_countw_(sentence=&archive_list);
		    %local input_&num 
                   _&to
            ;
	        libname _&to "&archive_path&bu&separater.forecast&separater&to&separater.output&separater.";
            %let input_&num=_&&to..f_b&bu._output; 
            /********************************************************************************************/

		%end;
		/****************************************************************************************************************************/

	%end;
	/************************************************************************************************************************************/

	%if &return=0 %then %do;

	    /*assigning value to variable win_&num*/
	    /*****************************************************/
        %local win_&num;
        %_list_(from=&to, 
                to=, 
                step=%eval(&t_review + &t_lead + &t_sales - 1),
                order=a, 
                type=fiscal, 
                mvar=win_&num
                );
        run; 
	    /*****************************************************/ 
		
		/*1.assigning value to variables win_last, forecast_last*/
	    /*2.adjusting the value of 'win_&num' if needed*/
	    /*********************************************************************************************************/
		%let win_last = %scan(&&win_&num,-1);
		%let forecast_last = %scan(%_vars_list_(data=&&input_&num,keep=_:,drop=),-1,'_');
		
		%if (&win_last > &forecast_last) %then %do;

			%_list_(from= &to,
        			to=&forecast_last, 
        			step=,
        			order=a, 
        			type=fiscal, 
        			mvar=win_&num
        			);
			run;   

			%_log_message_(text=PROGNOSWARNING: FISCALS AFTER %scan(&&win_&num,%_countw_(sentence=&&win_&num)));
    		%_log_message_(text=PROGNOSWARNING: ARE MISSING.);
    		%_log_message_(text=PROGNOSWARNING: THIS WARNING WAS ISSUED BY MACRO R_COLLECT.);

		%end;
	    /*********************************************************************************************************/

		/*assigning value to variable 'total_win'*/
		/********************************************/
    	%let total_win=%_combine_(list1=&total_win, 
                              	  list2=&&win_&num,
		            		  	  delimiter=%str( )
                                  );
		/********************************************/

	%end;
	/*******************************************************************************************************************************************************************************************************/

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

/*checking wheather sales data is missing some columns*/
/*assigning value to variable missing_list*/
/***********************************************************************************************************/
%do i = 1 %to %_countw_(sentence=&total_win);
	%if not( %_format_list_(%scan(&total_win,&i), before=_) in %_vars_list_(data=input.f_b&bu._sales,keep=_:,drop=) ) %then %do;
		%let missing_list = &missing_list %scan(&total_win,&i);
	%end; 
%end;
/***********************************************************************************************************/

/*if sales data is missing some columns, then shut down sas*/
/********************************************************************************************/
%if %length(&missing_list) > 0 %then %do;
    %_log_message_(text=PROGNOSERROR: SOME COLUMNS FROM %upcase(input.f_b&bu._sales) ARE MISSING);
    %_log_message_(text=PROGNOSERROR: THIS ERROR MESSAGE WAS ISSUED BY MACRO R_COLLECT.);

	proc printto;
	run;
    %_update_status_(log_file_name=r_collect_&bu);
    %_abort_(rc=2);
%end;
/********************************************************************************************/

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

/*creating dataset with actuals, forecast and error*/
/**********************************************************************************************/
data except.exceptions(keep=&f_geo_level &f_prod_level); /*change the name of data set?????*/

/*setting up PDV*/
/********************************************************************************************************/
if 0 then set input.f_b&bu._sales(keep=&f_geo_level &f_prod_level %_format_list_(&total_win, before=_));
attrib %_format_list_(&archive_list, before=a_)
       %_format_list_(&archive_list, before=f_)
       %_format_list_(&archive_list, before=e_) 
       %_format_list_(&archive_list, before=p_e_)
       %_format_list_(&archive_list, before=n_e_)
       %_format_list_(&archive_list, before=n_p_e_)
       m_e std_e m_p_e std_p_e rc                   length=&length_xxxxxx
;
flag=0;/*flag for determining which geo/prods we are using*/
/********************************************************************************************************/

/*setting up arrays*/
/****************************************************************/
%do i=1 %to %_countw_(sentence=&archive_list);
    array win&i{*} %_format_list_(&&win_&i, before=_);
%end;
array f_win{*} %_format_list_(&archive_list, before=f_); 
array a_win{*} %_format_list_(&archive_list, before=a_);
array e_win{*} %_format_list_(&archive_list, before=e_);
array p_e_win{*} %_format_list_(&archive_list, before=p_e_);
array n_e_win{*} %_format_list_(&archive_list, before=n_e_);
array n_p_e_win{*} %_format_list_(&archive_list, before=n_p_e_); 
/****************************************************************/

/*loading hash objects*/
/*****************************************************************************************/
if _n_= 1 then do;

	/*creating forecast hashes*/
	/*********************************************************************************/
	%do i = 1 %to %_countw_(sentence=&archive_list);

		declare hash h_&i(dataset:"&&input_&i(where=(scan(type,2,' ')='forecast'))");
		h_&i..definekey(%_comma_sep_(&f_geo_level &f_prod_level, quoted=1));
		h_&i..definedata(%_comma_sep_(%_format_list_(&&win_&i, before=_), quoted=1));
		h_&i..definedone();

	%end;
	/*********************************************************************************/

	/*creating actuals-forecast-error hash*/
	/*********************************************************************/
	declare hash h();
	h.definekey(%_comma_sep_(&agg_lvl, quoted=1));
	h.definedata(%_comma_sep_(&agg_lvl 
                              %_format_list_(&archive_list, before=a_) 
							  %_format_list_(&archive_list, before=f_) 
							  %_format_list_(&archive_list, before=e_)
                              %_format_list_(&archive_list, before=p_e_) 
							  %_format_list_(&archive_list, before=n_e_) 
							  %_format_list_(&archive_list, before=n_p_e_) 
                              m_e std_e m_p_e std_p_e , quoted=1
                              )
			     );
	h.definedone();
    declare hiter h_iter('h');
	/*********************************************************************/

end;
/*****************************************************************************************/

/*******************************/
set input.f_b&bu._sales end=last;
/*******************************/

rc=h.find();

/*taking care of actuals*/
/*********************************************/
%do i=1 %to %_countw_(sentence=&archive_list);
    a_win{&i}=sum(sum(of win&i{*}),a_win{&i});    
%end;   
/*********************************************/

/*taking care of forecast*/
/***********************************************/
%do i = 1 %to %_countw_(sentence=&archive_list);
    if h_&i..find()=0 then do;
	   flag=flag+1;
       f_win{&i}=sum(sum(of win&i{*}),f_win{&i});
	end;
%end;
/***********************************************/

/*outputing record to hash*/
/***************************/
if flag=0 then delete;
if rc=0 then h.replace();
else if rc^= 0 then h.add();
/***************************/

if last then do;

   rc=h_iter.first();
   do while(rc=0);  

      /*computing errors*/
      /**********************************************/
      %do i = 1 %to %_countw_(sentence=&archive_list);
          e_win{&i} = a_win{&i} - f_win{&i};
      %end;
      /**********************************************/

	  /*outputing  records with not enough data*/
	  /*removing those records from the hash will be taken care later*/
	  /*we can't remove them now because of iterated component*/
	  /***************************************************************/
	  if max(of e_win{*})=. then do;
         output except.exceptions;
		 goto skip;
	  end;
      /***************************************************************/

      /*computing the rest of stat*/
      /************************************************************************/
      %do i = 1 %to %_countw_(sentence=&archive_list);

		  /*mean of error*/
		  /***********************/
		  m_e = mean(of e_win{*});
		  /***********************/

		  /*standard deviation of error*/
		  /******************************/
		  std_e = std(of e_win{*});
		  /******************************/

		  /*n_e*/
		  /********************************************************/
          if std_e > 0 then n_e_win{&i}=( e_win{&i} - m_e ) / std_e;
		  else n_e_win{&i}=e_win{&i} - m_e;
          /********************************************************/

	      /*p_e*/
	      /*******************************************************/
	      if a_win{&i}=0 and f_win{&i}=0 then p_e_win{&i}=0;
		  else if a_win{&i} > 0 and f_win{&i}=0 then p_e_win{&i}=1;
          else p_e_win{&i} = e_win{&i} / f_win{&i};
		  /*******************************************************/

		  /*mean of p_e*/
		  /***************************/
		  m_p_e = mean(of p_e_win{*});
		  /***************************/

		  /*standard deviation of p_e*/
		  /****************************/
		  std_p_e = std(of p_e_win{*});
		  /****************************/

		  /*n_p_e*/
		  /******************************************************************/
		  if std_p_e > 0 then n_p_e_win{&i}=( p_e_win{&i} - m_p_e ) / std_p_e;
		  else n_p_e_win{&i}=p_e_win{&i} - m_p_e;
          /******************************************************************/

	  %end;
      /************************************************************************/

	  rc = h.replace();
	  skip:;
      rc = h_iter.next();

	end;
    
	if max(of e_win{*})^= . then do;
	   h.output(dataset:"work.test_err");
	end;

end;

run;
/**********************************************************************************************/

/*clearing the libname*/
/*********************************************/
%do i=1 %to %_countw_(sentence=&archive_list);
    libname _%scan(&archive_list,&i) clear ;
%end;
/*********************************************/


proc printto;
run;

%mend r_collect;

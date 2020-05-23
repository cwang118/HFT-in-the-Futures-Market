option nosource nonotes;
libname cmehft "C:\Chao\Dataset";
libname sumtab "C:\Chao\Tables New";
/* Get file list of all the dataset */
/**** make sure no previous runs   ******/
proc datasets lib=work kill noprint;
run;
quit;

/* Import the information of all dates + contracts 2018 */
%Let datadict=C:\Chao\DatesAndFutures.csv;

proc import datafile="&datadict."
	out=dates
	dbms=csv
	replace;
run;

data timefmt1m;
   keep fmtname start end label eexcl;
   begin='00:00'T;   /*  starting time  */
   stop='24:00'T;    /*  ending time    */
   num=intck('minute',begin,stop); /* the standard of interval tick between begin and stop ,here since you need 1 min interval, set "minute" */
   fmtname='timefmt1m';      /*make sure this name is the same as data cmehft.timefmtm5*/
   eexcl='N';
   do i=0 to num-1 by 1; /*1 min interval*/
      start=intnx('minute',begin,i);
      end=intnx('minute',begin,i+1);
      label=i/1+1;
      output;
   end;
run;  

proc format library=work cntlin=work.timefmt1m fmtlib;                            
   select timefmt1m;                                                      
run;  

data timefmt30m;
   keep fmtname start end label eexcl;
   begin='00:00'T;   /*  starting time  */
   stop='24:00'T;    /*  ending time    */
   num=intck('minute',begin,stop); /* the standard of interval tick between begin and stop ,here since you need 30 min interval, set "minute" */
   fmtname='timefmt30m';      /*make sure this name is the same as data cmehft.timefmtm5*/
   eexcl='N';
   do i=0 to num-30 by 30; /*30 min interval*/
      start=intnx('minute',begin,i);
      end=intnx('minute',begin,i+30);
      label=i/30+1;
      output;
   end;
run;  

proc format library=work cntlin=work.timefmt30m fmtlib;                            
   select timefmt30m;
run;  

/* The root direction keywords of all 1st and 2nd nearest futures */
data cmehft.tickers;
	input ticker $6. tick 8.;
	cards;
GE1_	0.25
GE2_	0.5
CL1_	1
CL2_	1
GC1_	1
GC2_	1
ES1_	25
ES2_	25
ZC1_	0.25
ZC2_	0.25
ZS1_	0.25
ZS2_	0.25
;
run;

data tickers;
	set cmehft.tickers;
	call symput("nticks",_N_);
run;

%put &nticks.;

data dates;
	set dates;
	numdate = compress(put(date,yymmdd10.),'-');
	drop date;
	rename numdate = date;
	format numdate 8.;
/*	if _N_;*/
run;

%LET Lequiv=10;
%LET Low=_p10_;
%LET High=_p90_;
%LET warmup=100;
%Let template=C:\Chao\tables1ab.xlsx;
%Let Detail=YES;
%Let Eslack=.10;
%Let TimeToNothing=5;
%let gap=0;
/*******************this need to be changed for each product*************************/

data dates2;
	set dates;
		if holiday = 1 or weekday > 5 then workday = 0;
		else workday = 1;
	array quest(*) dtm_zc1_ dtm_zc2_ dtm_zs1_ dtm_zs2_ dtm_cl1_ dtm_cl2_ dtm_gc1_ dtm_gc2_ dtm_es1_ dtm_es2_ dtm_ge1_ dtm_ge2_
	;
	do i=1 to dim(quest); *loop over vars;
	    if quest(i) > 14 and quest(i) < 29 then quest(i) = 1;
	    else if quest(i) > 28 and quest(i) < 43 then quest(i) = 2;
	    else if quest(i) > 42 and quest(i) < 57 then quest(i) = 3;
	    else if quest(i) > 56 and quest(i) < 71 then quest(i) = 4;
	    else if quest(i) > 70 and quest(i) < 85 then quest(i) = 5;
	    else if quest(i) > 84 then quest(i) = 6;
	    else quest(i) = 0; *can avoid if/else with formula;
	end;
	drop holiday weekday i;
run;

%let ndates=365;

%macro create_agg_tables;
/*%let j = 1;%let k = 1;*/
	%do j=1 %to &nticks.;
		data tickers;
			set tickers;
			if _N_ eq &j then do;
			call symput("ticker", ticker);
			call symput("tick", tick);
			end;
		run;
        %let ticker=%trim(&ticker);
        %let tick=%trim(&tick);
		%let rank = %substr(&ticker,3,1);

		data dates;
			set dates2;
			call symput("ndates",_N_);
		run;

/*		%let ticker = cl1_;*/
		%do k=1 %to &ndates.;
		data dates;
			set dates;
			if _N_ eq &k then do; 
			call symput("date", date);
			call symput("workday", workday);
			call symput("dtm2", dtm_&ticker.);
			end;
		run;
		%put &date &workday &dtm2 &ticker &rank;

		%let date=%trim(&date);
		/* Below from Dr. Cooper's code*/

		/* First Step is to remove ! items from consderation; They were not part of the original itch feed
		   Also, remove observations at 9:00:00.000000000 or before or after 16:00:00.000000000    */
		/* Also, there are duplicate rows, which we need to remove, these were put in by the data provider because of an coding error. It has been verified that they 
		   do not affect data integrity */

		%let date = 20180710;
		%let ticker = ge2_;
		%let tick = 0.5;
		%let workday = 1;
		%let dtm = 0;
		%let rank = 1;

		%if %sysfunc(exist(cmehft.&ticker.&date.)) %then %do;
		
		proc sort data = cmehft.&ticker.&date. tagsort;
			by date mtime;
		run;

		data dset (drop=lns ltoken ltoken_new);
		  	set cmehft.&ticker.&date.;
			retain obnum 0;
			Lns=lag1(ns);
			Ltoken=lag1(token);
			Ltoken_new=lag1(token_new);
		    if ns ne lns or
				token ne ltoken or
				token_new ne ltoken_new;
			obnum + 1;
		run;

		data dset (drop = groups groups2);
			set dset;
			spread=ask1-bid1;
			spread_scaled=(2*spread)/(ask1+bid1);
			vol1=bidv1+askv1;
			quantity1=bidq1+askq1;
			if vol1 ^= 0 then IB=(bidv1-askv1)/vol1;
			else IB=.;
			groups=mtime;
			format groups timefmt1m.;
			groups2=mtime;
			format groups2 timefmt30m.;
			groups1=input(vvalue(groups),8.);
			groups30=input(vvalue(groups2),8.);
		run;

		/*we need to add to the data set the resting time of every token that has a non-zerop resting time */
		/* Most of the time there is either 1 or 2 entries for a token. An order was added to the book and either cancelled, */
		/* deleted, or executed, ocassionally there are more like a partial fill, then another. The last occurance time of a token */
		/* versus the first occurance time is what is used to calculate the resting time. This resting time is placed on every row that has */
		/* that token */
		/*This is made tougher by the change order type. The change order type means that one token ends and another begins on the same line */
		/* Therefore there will be a rest time and a rest time_new for every row that has a valid token for each field called (rtime and rtime_new) */

		proc sort data=dset tagsort; by token; run;

		data tokset (keep=token ns);
		     set dset;
		run;

		data tokset_new (keep=token ns);
			set dset(drop=token);
			if token_new ^= .;
			rename token_new=token;
		run;

		data tokset;
			set tokset tokset_new;
		run;

		proc sort data=tokset tagsort; by token ns; run;

		data tokset_beg;
			set tokset;
		    by token;
			if first.token;
			rename ns=begns;
		run;

		data tokset_end;
			set tokset;
			by token;
			if last.token;
			rename ns=endns;
		run;

		data tokset (drop=begns endns);
		    merge tokset_beg tokset_end;
			by token;
			rtime=(endns-begns)/1000000000;
		run;

		/* Left rtime=0 observations untouched. These are sigle observation tokens. That is they never got filled */
		data dset;
			merge dset tokset;
			by token;
		run;

		proc sort data=dset tagsort; by token_new; run;

		data tokset;
			set tokset;
			rename token=token_new;
			rename rtime=rtime_new;
		run;

		data dset;
			merge dset tokset;
			by token_new;
			if obnum ne .;
		run;

		/* restore dset to original order, now each token and token_new has a resting time associated with it */
		proc sort data=dset tagsort; by obnum; run;

		/* At this point the dset has a resting time added for every single observation rtime for token and rtime_new for token_new :/ 
		/***** Identify all level clears *******/
		/******move them to their correct place in the dataset, they are identified one row late ***********/
		/******create a variable that has the 1/2 hour bucket of the clear *********************************/

		data clrset;
			set dset(keep = Obnum Xcomment Volume Price Bid1 Ask1 Side Mtime Case Rank Comment);
			format Xcomment lastax lastbx $20.;
			retain Lastbidp 0 Lastaskp 0; /*these will contain last rows bid and ask top of book price*/
			/*these will contain the last action on the top of each side of the book*/
			retain lastbX "" lastaX "";
			retain executeb 0 executea 0;
				if _N_ eq 1 then do; lastbidp=bid1; lastaskp=ask1; end;
			    if bid1 lt lastbidp then dclearb=1; else dclearb=0;
				if bid1 lt lastbidp and (lastbx eq "DELETE") then dclearbc=1; else dclearbc=0;
				if bid1 lt lastbidp and dclearbc=0 then dclearbe=1; else dclearbe=0;
				if ask1 gt lastaskp then dcleara=1; else dcleara=0;
				if ask1 gt lastaskp and (lastax eq "DELETE") then dclearac=1; else dclearac=0;
				if ask1 gt lastaskp and dclearac=0 then dclearae=1; else dclearae=0;
			lastbidp=bid1;
			lastaskp=ask1;
				if side eq "B" then do; lastbx = Xcomment; end;
				if side eq "S" then do; lastax = Xcomment; end;
				if side eq "B" and Xcomment eq "TRADE" then executeb=executeb+1;
				if side eq "S" and Xcomment eq "TRADE" then executea=executea+1;
		run;

		/** We need to know the last observation number; we can get it here **/

		proc sort data=clrset tagsort; by descending obnum; run;
		data clrset(keep=obnum cleara clearae clearac clearb clearbe clearbc);
			set clrset;
			if _N_ eq 1 then do;
               call symput("lastob", obnum);
			   call symput("executea",executea);
			   call symput("executeb",executeb);
			end;
			cleara=lag1(dcleara);
			clearae=lag1(dclearae);
			clearac=lag1(dclearac);
			clearb=lag1(dclearb);
			clearbc=lag1(dclearbc);
			clearbe=lag1(dclearbe);
		run;

		proc sort data=clrset tagsort; by obnum; run;

		data dset;
		   merge clrset dset;
		   by obnum;
		run;

		/********* All of the clears have been identified ****************************/
		/********* The goal here is to make and EWMA of each observation *************/
		/******** Also to make a list of what happens after each clear ******* ******/

		/********** U's need to be carefully considered U's that reduce the volume or move deeper in the book are cancels
		            U's that go into the gap are considered adds   *****/
		Data dset;
		    set dset;
			retain B_cancel 0 A_cancel 0; /*these are the ewma variables */
			retain clrswtchb 0 clrswtcha 0; /* used in this procedure to know when we are searching for resolution to a clear */  
			retain lastbidp 0 lastaskp 0;
			retain obtrka 0 obtrkb 0; /****  used in this procedure to tell which clear the after event matches ****/
		 	format clrswtchb time. clrswtcha time.;
			if _N_ eq 1 then do; lastbidp=bid1; lastaskp=ask1; end;
		    /*** if a clear on the bid occurs mark the start time and the observation number or mark it as a double clear ***/
			if clearb eq 1 and clrswtchb gt 0 then dblclrb=1;
			else if clearb eq 1 then do; 
				clrswtchb=ns/1000000000; 
				obtrkb=obnum; 
			end;
			/*** if it is a dblclear then we have some business to take care of ************/
			/**** rack up results and reset the counters                        ************/
		 	if dblclrb eq 1 then do;
				reacttimeb=ns/1000000000-clrswtchb; /* the last event time l-ess the time of the clear before that, reactiontime */
				obclrb=obtrkb; /* the observation number of the clear */
				if clearbc eq 1 then afterclrb=1; /* flag that tells us the clear followed by another cancel clear */
				else if clearbe eq 1 then afterexeb=1; /* flag that tells us the clear is followed by an execute, it happened to clear also*/
				clrswtchb=ns/1000000000; /* now we update the clrswtch variable */
				obtrkb=obnum; /* now we update the observation the clear occurred variable */
			end;	

			/* Now we are into the after a clear event logic */
			/* If we are in the period of dealing with an after clear event */
			if clrswtchb gt 0 and obnum gt obtrkb then do;
		       	/* First we make sure that not too much time has passed since the clear */
			   	if ns/1000000000-clrswtchb ge &timetonothing. then do; /*if nothing has happened for awhile*/
					afternormb=1; /*record statistics reset switch to zero*/
					clrswtchb=0;												
					obclrb=obtrkb; /*this is the observation the after came from */
			    end;					
				/*if it is a level fill, add record statistics */
				else if bid1 gt lastbidp then do;										
					if rtime ne . then afteraddb=rtime; 						
					else afteraddb=rtime_new;
					reacttimeb=ns/1000000000-clrswtchb;
					obclrb=obtrkb; 										
					clrswtchb=0;
					end;
				/* it it is a gap close from other side, record statistics */
				else if  ask1 lt lastaskp then do;								
					if rtime ne . then afterclsb=rtime; /* reset switch to zero */
					else afterclsb=rtime_new;
					reacttimeb=ns/1000000000-clrswtchb;
					obclrb=obtrkb;
					clrswtchb=0;
				end;
				/*if it is an execute that did not result in a double clear, record statistics*/
			    else if side eq "B" and xcomment="TRADE" then do;   
					afterexeb=1; /*same report as an execute that did clear the level*/								 
					reacttimeb=ns/1000000000-clrswtchb;
					obclrb=obtrkb; 
					clrswtchb=0;
			    end;
			end;
			/******** repeat for ask *****/
			if cleara eq 1 and clrswtcha gt 0 then dblclra=1;
			else if cleara eq 1 then do; 
				clrswtcha=ns/1000000000; 
				obtrka=obnum; 
			end;
		 	if dblclra eq 1 then do;
				reacttimea=ns/1000000000-clrswtcha;
				obclra=obtrka;
				if clearac eq 1 then afterclra=1;
				else if clearae eq 1 then afterexea=1;
				clrswtcha=ns/1000000000;
				obtrka=obnum;
			end;
			if clrswtcha gt 0 and obnum gt obtrka then do;
			   if ns/1000000000-clrswtcha ge &timetonothing. then do;                   
					afternorma=1;
					obclra=obtrka; 
					clrswtcha=0;												
			    end;									
				else if ask1 lt lastaskp then do;					 			
					if rtime ne . then afteradda=rtime; 						
					else afteradda=rtime_new;
					reacttimea=ns/1000000000-clrswtcha;
					obclra=obtrka;
					clrswtcha=0;
				end;
				else if bid1 gt lastbidp then do;								
				 	if rtime ne . then afterclsa=rtime;							
				 	else afterclsa=rtime_new;
					reacttimea=ns/1000000000-clrswtcha;
					obclra=obtrka;
					clrswtcha=0;
				end;
			    else if side eq "S" and xcomment="TRADE" then do;   
				 	afterexea=1;																			 
				 	reacttimea=ns/1000000000-clrswtcha;
					obclra=obtrka; 
					clrswtcha=0;
			    end;
			end;
			/* Now to make an EWMA of the cancels (Partial cancellations of existing orders, Executions with price improvements, 
			Cancellations of existing limit orders, Updates of existing orders and new price exceed original price)rate */
	    	if side eq "B" then
				do;
				if xcomment eq "DELETE" then Bval=1; else Bval=0;
				B_cancel=B_cancel + 2/(&Lequiv + 1)*(Bval-B_cancel);
				end;
			if side eq "S" then
				do;
				if xcomment eq "DELETE" then Aval=1; else Aval=0;
				A_cancel=A_cancel + 2/(&Lequiv + 1)*(Aval-A_cancel);
				end;
			lastbidp=bid1;
			lastaskp=ask1;
		run;

		/* variables created to talk about what happens after a trade */
		/* reacttimea reacttimeb -- time since the clear at which resolution takes place */
		/* afterclra afterclrb -- 1 if the resolution of the level clear is another clear */
		/* afteradda afteraddb -- contains the resting time of the order if the resolution of the previous clear is a level fill */
		/* afterexea afterexeb -- contains 1 if an execute at the next level was the resoluton of the clear */
		/* afternorma afternormb -- contains a 1 if the resoluton was nothing above for %timetonothing seconds. This almost certainly means
		/* obclra obclrb -- the observation number of thclear that the after event corresponds to
		   that the level filled from the other side and nothing siginificant happened because of the clear */


		/*  This determines the EWMA level to declare a "Clear" or "Dormant" cluster */
		proc univariate data=dset noprint outtable=aewmastat;
			var A_cancel;
		run;
		proc univariate data=dset noprint outtable=bewmastat;
			var B_cancel;
		run;

		data Aewmastat;
		 	set Aewmastat;
		 	call symput("Alow",&Low);
		 	call symput("Ahigh",&High);
		run;

		data Bewmastat;
		 	set Bewmastat;
		 	call symput("Blow",&Low);
		 	call symput("Bhigh",&High);
		run;

		/********** Now we want to eliminate unused variables
		also we want to move the afterclear statistics onto the rows of the clears they are associated with
		this will allow us to easily catagorize them into the appropriate clusters ****************************/

		data after_clear_ask(keep=obnum afteradda afterexea afterclra afternorma afterclsa reacttimea);
			set dset (drop=obnum);
			if obclra ne .;
			rename obclra=obnum;
		run;

		data after_clear_bid(keep=obnum afteraddb afterexeb afterclrb afternormb afterclsb reacttimeb);
			set dset (drop=obnum);
			if obclrb ne .;
			rename obclrb=obnum;
		run;

		data dset;
			merge dset(drop=afteradda afterexea afterclra afternorma afterclsa reacttimea afteraddb afterexeb afterclrb
			afternormb afterclsb reacttimeb obclra obclrb obtrka obtrkb lastbidp lastaskp clrswtcha clrswtchb dblclra 
			dblclrb aval bval) after_clear_ask after_clear_bid;
			by obnum;
		run;

		/*create the four Quadrants */
		/*This data step identifies low and high cancel clusters */
		data dset;
		 	set dset;
		 	retain tbeginal 0 tbeginah 0 tbeginbl 0 tbeginbh 0;
		 	retain cleara_s 0 clearac_s 0 clearae_s 0 clearb_s 0 clearbc_s 0 clearbe_s 0;
			retain mlastclrA 0 mlastclrB 0 tbucka 0 tbuckb 0 executea 0 executeb 0;
			retain obbeginah obbeginal obbeginbh obbeginbl;
			/**** section determines if you are in a cancel cluster, or calm cluster *******/
			/**** the time in each cluster is also computed */
			/**** the type and number of each level clear is also computed *****************/
		    /**** the time to last clear in a cluster is also calculated *************/
		   if _N_ LE &warmup. then do;
		   		cleara=.; clearb=.; clearae=.; clearac=.; clearbe=.; clearbc=.;
				afteradda=.; afteraddb=.; afterclra=.; afterclrb=.;
				afterclsa=.; afterclsb=.; afternorma=.; afternormb=.;
				afterexea=.; afterexeb=.; reacttimea=.;reacttimeb=.; tbucka=.; tbuckb=.;
			end;
			else do;
				/* This is the start of a cancel cluster -- record the start time */
				/*set counters to 0 */
				/*record time bucket for start of cluster */
				if A_cancel ge &Ahigh. and tbeginah eq 0 then
				do;
					beginah=1;
					obbeginah=obnum;
		       		tbeginah=ns;       
		   			cleara_s=0;				   
		   			clearac_s=0;			   
		   			clearae_s=0;
					mlastclrA=0;
					tbucka=groups30;
					executea=0; /* it can't be an execute at the start, it must be a cancel */
				end;
				
				/* If we are in a cancel cluster-- keep compiling stats */
				/* unless its the last observation then close the cluster */
				if A_cancel ge &Ahigh.-&eslack. and tbeginah gt  0 then
					do;
		   			if cleara=1 then do; cleara_s=cleara_s+1; mlastclrA=(ns-tbeginah)/1000000000; end;
		   			if clearac=1 then clearac_s=clearac_s+1;
		   			if clearae=1 then clearae_s=clearae_s+1;
					if side="S" and xcomment="TRADE" then executea=executea+1;
					if obnum=&lastob. then elapsedtimeah=(ns-tbeginah)/1000000000; 
		       		end;

				/* if we have fallen out of a high cluster */
		    	else if A_cancel lt &Ahigh.-&eslack. and tbeginah gt  0 then 
					do;
					elapsedtimeah=(ns-tbeginah)/1000000000; 
					tbeginah=0;
					end;
				else do; obbeginah=.;end;
						/***repeat for non-cancel cluster **/
				if A_cancel lt &Alow. and tbeginal eq 0 then 
					do;
					beginal=1;
					obbeginal=obnum;
		       		tbeginal=ns;       
		   			cleara_s=0;				  
		   			clearac_s=0;			   
		   			clearae_s=0;
					mlastclrA=0;
					tbucka=groups30;
					executea=0; /* it can't be an execute at the start, it must be a cancel */
		   			end;
				if A_cancel lt &Alow.+&eslack. and tbeginal gt 0 then
					do;
		   			if cleara=1 then do; cleara_s=cleara_s+1; mlastclrA=(ns-tbeginal)/1000000000; end;
		    		if clearac=1 then clearac_s=clearac_s+1;
		    		if clearae=1 then clearae_s=clearae_s+1;
					if side="S" and xcomment="TRADE" then executea=executea+1;
					if obnum=&lastob. then elapsedtimeal=(ns-tbeginal)/1000000000; 
					end;
				else if A_cancel gt &Alow.+&eslack. and tbeginal gt 0 then 
					do;
					elapsedtimeal=(ns-tbeginal)/1000000000; 
					tbeginal=0;
					end;
				else do; obbeginal=.;end;
						/**** Bid cancel Cluster ***/
				if B_cancel ge &Bhigh. and tbeginbh eq 0 then 
					do;
					beginbh=1;
					obbeginbh=obnum;
		       		tbeginbh=ns;       
					clearb_s=0;
					clearbc_s=0;
					clearbe_s=0;
					mlastclrB=0;
					tbuckb=groups30;
					executeb=0; /* it can't be an execute at the start, it must be a cancel */
					end;
				if B_cancel ge &Bhigh.-&eslack. and tbeginbh gt  0 then 
					do; 
		   			if clearb=1 then do; clearb_s=clearb_s+1; mlastclrB=(ns-tbeginbh)/1000000000; end;
		    		if clearbc=1 then clearbc_s=clearbc_s+1;
		    		if clearbe=1 then clearbe_s=clearbe_s+1;
					if side="B" and xcomment="TRADE" then executeb=executeb+1;
					if obnum=&lastob. then elapsedtimebh=(ns-tbeginbh)/1000000000; 
					end;
		    	else if B_cancel lt &Bhigh.-&eslack. and tbeginbh gt 0 then 
					do;
					elapsedtimebh=(ns-tbeginbh)/1000000000; 
					tbeginbh=0;
					end;
				else do; obbeginbh=.;end;
		               /*  Bid non-cancel **/
				if B_cancel lt &Blow. and tbeginbl eq 0 then 
					do;
					beginbl=1;
					obbeginbl=obnum;
		       		tbeginbl=ns;       
					clearb_s=0;
					clearbc_s=0;
					clearbe_s=0;
					mlastclrB=0;
					tbuckb=groups30;
					executeb=0; /* it can't be an execute at the start, it must be a cancel */
					end;
				if B_cancel lt &Blow.+&eslack. and tbeginbl gt 0 then 
					do;
		   			if clearb=1 then do; clearb_s=clearb_s+1; mlastclrB=(ns-tbeginbl)/1000000000; end;
		    		if clearbc=1 then clearbc_s=clearbc_s+1;
		    		if clearbe=1 then clearbe_s=clearbe_s+1;
					if side="B" and xcomment="TRADE" then executeb=executeb+1; 
					if obnum=&lastob. then elapsedtimebl=(ns-tbeginbl)/1000000000; 
					end;
				else if B_cancel gt &Blow.+&eslack. and tbeginbl gt 0 then 
					do;
					elapsedtimebl=(ns-tbeginbl)/1000000000;
					tbeginbl=0;
					end;
				else do; obbeginbl=.;end;
			end;
		run;


		proc sql;
			create table dset2 as select
			/*mb_inf is the 1-min average of EWMA of bid side, vice versa for ma_inf*/
			avg(b_cancel) as mb_inf, avg(a_cancel) as ma_inf, * from dset group by groups1;
		quit;

		/*Note that the clear counters keep going after the end of a cluster until reset by another cluster*/
		/*mlastclrX will also not reset until the next cluster */
		/*likewise the time of last clear since beginning of the cluster */
		/*also tbucka and tbuckb/
		/*We keep tbeginah, etc. around because they will be used to tell if the afterevent is in a cancel or
		  dormant cluster */

		/**************************************************************************************/
		/**               Now We Start Putting together the specific tables                  **/
		/**************************************************************************************/

		/******* make a dataset only of the cluster stats ***********/
		data cluster_set (drop= clearb_s cleara_s clearbc_s clearac_s clearbe_s clearae_s elapsedtimeah elapsedtimebh 
		elapsedtimeal elapsedtimebl	mlastclrA mlastclrB tbuckb tbucka executea executeb);
		    set dset2;
			if elapsedtimebl ne . or elapsedtimeal ne . or elapsedtimebh ne . or elapsedtimeah ne .;
			if elapsedtimebh ne . then
				do;
/*				obbegin=obbeginbh;*/
				Tableside="B";
				elapsedtime=elapsedtimebh;
				clears=clearb_s;
				clearcs=clearbc_s;
				cleares=clearbe_s;
				mlastclr=mlastclrB;
				tbucket=tbuckb;
				execute=executeb;
				if clears gt 0 then Quad=1; else Quad=4;
				end;
			else if elapsedtimebl ne . then
				do;
/*				obbegin=obbeginbl;*/
				Tableside="B";
				elapsedtime=elapsedtimebl;
				clears=clearb_s;
				clearcs=clearbc_s;
				cleares=clearbe_s;
				mlastclr=mlastclrB;
				tbucket=tbuckb;
				execute=executeb;
				if clears gt 0 then Quad=2; else Quad=3;
				end;
			if elapsedtimeah ne . then
				do;
/*				obbegin=obbeginah;*/
				Tableside="A";
				elapsedtime=elapsedtimeah;
				clears=cleara_s;
				clearcs=clearac_s;
				cleares=clearae_s;
				mlastclr=mlastclrA;
				tbucket=tbucka;
				execute=executea;
				if clears gt 0 then Quad=1; else Quad=4;
				end;
			else if elapsedtimeal ne . then
				do;
/*				obbegin=obbeginal;*/
				Tableside="A";
				elapsedtime=elapsedtimeal;
				clears=cleara_s;
				clearcs=clearac_s;
				cleares=clearae_s;
				mlastclr=mlastclrA;
				tbucket=tbucka;
				execute=executea;
				if clears gt 0 then Quad=2; else Quad=3;
				end;
		run;

/*		data rectime;set rectime;rename obnum=obbegin;run;*/
/*		proc sort data=cluster_set; by obbegin; run;*/
/*		data cluster_set(drop=obbeginah obbeginal obbeginbh obbeginbl);merge cluster_set rectime;by obbegin;run;*/
/*		proc sort data=cluster_set;by obnum;run;*/
		proc sort data=cluster_set tagsort; by tableside quad obnum; run;

		proc means data=cluster_set noprint;
  			var elapsedtime clears clearcs cleares mlastclr execute;
		  	output out=tables1ab_&ticker.&date.
		    N=count
			mean=avetime avecl aveclcs avecles lclrtime aveexe;
		  	by tableside quad;
		run;

		data sumtab.tables1ab_&ticker.&date.;
			format tk $8.;
			set tables1ab_&ticker.&date.;
			dt=&date.;
			tk="&ticker.";
			pct_day=(count*avetime)/24/3600;
			if tableside="" then delete;
			if tableside="A" then pct_exe=aveexe/&executea.;
			if tableside="B" then pct_exe=aveexe/&executeb.;
			marketopen = &workday.;
			dtm = &dtm2.;
			rank = &rank.;
		run;
			
/*		proc append base=tables1ab_all data=tables1ab_&ticker.&date. force; run; quit;*/

		/*** Now tables1ab BY TIME BUCKET must now be calculated *****/

		proc sort data=cluster_set tagsort; by tbucket tableside quad; run;

		proc means data=cluster_set noprint;
  			var elapsedtime clears clearcs cleares mlastclr execute;
		  	output out=tables1ab_time_&ticker.&date.
		    N=count
			mean=avetime avecl aveclcs avecles lclrtime aveexe;
		  	by tbucket tableside quad;
		run;

		data sumtab.tables1ab_time_&ticker.&date.;
			format tk $8.;
			set tables1ab_time_&ticker.&date;
			dt=&date.;
			tk="&ticker.";
			pct_day=(count*avetime)/24/3600;
			if tableside="" then delete;
			if tableside="A" then pct_exe=aveexe/&executea.;
			if tableside="B" then pct_exe=aveexe/&executeb.;
		run;

/*		proc append base=tables1ab_time_all data=tables1ab_time_&ticker.&date. force; run; quit;*/

		/*  make a data set of what happens after the clear -- by cluster where the clear occured */

		data after_set (keep=tableside Quad result afteradd afterexe aftercls afterclr afternorm reacttime tbucket);
		    set dset2;
			if afteradda ne . or afterclsa ne . or afterexea ne . or afternorma ne . or afterclra;
			tableside="A";
			afteradd=afteradda;
			afterexe=afterexea;
			aftercls=afterclsa;
			afterclr=afterclra;
			afternorm=afternorma;
			reacttime=reacttimea;
			tbucket=tbucka;
			if tbeginah gt 0 then Quad=1; 
		    else if tbeginal gt 0 then Quad=2;
		    else Quad=6;
			if afteradd       ne . then result=1;
			else if aftercls  ne . then result=2;
			else if afterclr  ne . then result=3;
			else if afterexe  ne . then result=4;
			else if afternorm ne . then result=5;
			else result=-999;
			if Quad=6 then delete;
		run;

		data after_set_b (keep=tableside Quad result afteradd afterexe aftercls afterclr afternorm reacttime tbucket);
		    set dset2;
			If  afteraddb ne . or afterexeb ne . or afterclsb ne . or afternormb ne . or afterclrb ne .;
			tableside="B";
			afteradd=afteraddb;
			afterexe=afterexeb;
			aftercls=afterclsb;
			afterclr=afterclrb;
			afternorm=afternormb;
			reacttime=reacttimeb;
			tbucket=tbuckb;
		    if tbeginbh gt 0 then Quad=1; 
		    else if tbeginbl gt 0 then Quad=2;
		    else Quad=6;
			if afteradd       ne . then result=1;
			else if aftercls  ne . then result=2;
			else if afterclr  ne . then result=3;
			else if afterexe  ne . then result=4;
			else if afternorm ne . then result=5;
			else result=-999;
			if Quad=6 then delete;
		run;

		data after_set;
			set after_set after_set_b;
		run;

		proc sort data=after_set tagsort; by tableside quad result; run;	

		proc means data=after_set noprint;
			var afteradd aftercls afterclr afterexe afternorm reacttime;
			output out=after_stats_&ticker.&date.
			mean= afteradd aftercls afterclr afterexe afternorm reacttime;
			by tableside quad result;
		run;

		Data sumtab.after_stats_&ticker.&date.(drop=_TYPE_ result afteradd aftercls afterexe afternorm afterclr);
			set after_stats_&ticker.&date.;
			format tk $8.;
			rename _FREQ_=N;
			If quad eq 6 then delete;
			If result eq 1 then do; _NAME_="addstat"; resttime=afteradd; end;
			else if result eq 2 then do; _NAME_="clsstat"; resttime=aftercls; end;
			else if result eq 3 then do; _NAME_="clrstat"; resttime=.; end;
			else if result eq 4 then do; _NAME_="exestat"; resttime=.; end;
			else if result eq 5 then do; _NAME_="norstat"; resttime=.; reacttime=&timetonothing.; end;
			else _Name_="FUBAR";
			dt=&date.;
			tk="&ticker.";
			marketopen = &workday.;
			dtm = &dtm2.;
			rank = &rank.;
		run;
/*		proc append base=after_stats_all data=after_stats_&ticker.&date. force; run; quit;*/

		/********* Now we need to do the afterstats by timebucket  **************/

		proc sort data=after_set tagsort; by tbucket tableside quad result; run;	

		proc means data=after_set noprint;
			var afteradd aftercls afterclr afterexe afternorm reacttime;
			output out=after_stats_time_&ticker.&date.
			mean= afteradd aftercls afterclr afterexe afternorm reacttime;
			by tbucket tableside quad result;
		run;

		Data sumtab.after_stats_time_&ticker.&date.(drop=_TYPE_ result afteradd aftercls afterexe afternorm afterclr);
			set after_stats_time_&ticker.&date.;
			format tk $8.;
			rename _FREQ_=N;
			If quad eq 6 then delete;
			If result eq 1 then do; _NAME_="addstat"; resttime=afteradd; end;
			else if result eq 2 then do; _NAME_="clsstat"; resttime=aftercls; end;
			else if result eq 3 then do; _NAME_="clrstat"; resttime=.; end;
			else if result eq 4 then do; _NAME_="exestat"; resttime=.; end;
			else if result eq 5 then do; _NAME_="norstat"; resttime=.; reacttime=&timetonothing.; end;
			else _Name_="FUBAR";
			dt=&date.;
			tk="&ticker.";
		run;



		%if %sysfunc(exist(cmehft.tables1ab_&ticker.&date.)) %then %do;
			proc append base=sumtab.tables1ab_all data=cmehft.tables1ab_&ticker.&date. force; run; quit;
		%end;

		%if %sysfunc(exist(cmehft.tables1ab_time_&ticker.&date.)) %then %do;
			proc append base=sumtab.tables1ab_time_all data=cmehft.tables1ab_time_&ticker.&date. force; run; quit;
		%end;

		%if %sysfunc(exist(cmehft.after_stats_&ticker.&date.)) %then %do;
			proc append base=sumtab.after_stats_all data=cmehft.after_stats_&ticker.&date. force; run; quit;
		%end;

		%if %sysfunc(exist(cmehft.after_stats_time_&ticker.&date.)) %then %do;
			proc append base=sumtab.after_stats_time_all data=cmehft.after_stats_time_&ticker.&date. force; run; quit;
		%end;

		%END;
		%END;
	%END;
%MEND	create_agg_tables;

%create_agg_tables;


/*libname x v9 'F:\Output2\ES2_';*/
/*proc copy in=work out=x memtype=data;*/
/*run;*/

data after_stats_all;
	set sumtab.after_stats_all;
	if dt ^= .;
run;
data after_stats_time_all;
	set sumtab.after_stats_time_all;
	if dt ^= .;
run;
data tables1ab_all;
	set sumtab.tables1ab_all;
	if dt ^= .;
run;
data tables1ab_time_all;
	set sumtab.tables1ab_time_all;
	if dt ^= .;
run;


/* Split data into stock market open vs closed*/

data sumtab.after_stats_time_all_Open after_stats_time_all_Open;
	set sumtab.after_stats_time_all;
	if tbucket > 17 and tbucket < 31 and marketopen = 1;
run;

data sumtab.after_stats_time_all_Close after_stats_time_all_Close;
	set sumtab.after_stats_time_all;
	if tbucket < 18 or tbucket > 30 or marketopen = 0;
run;

data sumtab.tables1ab_time_all_Open tables1ab_time_all_Open;
	set sumtab.tables1ab_time_all;
	if tbucket > 17 and tbucket < 31 and marketopen = 1;
run;

data sumtab.tables1ab_time_all_Close tables1ab_time_all_Close;
	set sumtab.tables1ab_time_all;
	if tbucket < 18 or tbucket > 30 or marketopen = 0;
run;












			/******* Consolidated Table 1 ***********/
proc sort data=tables1ab_all tagsort; by tableside quad; run;

proc means data=tables1ab_all noprint;
	var avetime avecl aveclcs avecles lclrtime pct_day aveexe pct_exe;
	output out=tables1ab_summary
	mean= avetime avecl aveclcs avecles lclrtime pct_day aveexe pct_exe;
	by tableside quad;
	weight _FREQ_;
run;

proc means data=tables1ab_all noprint;
	var _FREQ_;
	output out=tables1ab_summary_b
	sum=total_n;
	by tableside quad;
run;

data tables1ab_summary (drop=_type_ _freq_);
    merge tables1ab_summary tables1ab_summary_b;
	by tableside quad;
run;


		/*********** Table 1 by Commodity ************/

proc sort data=tables1ab_all tagsort; by tk tableside quad; run;

proc means data=tables1ab_all noprint;
	var avetime avecl aveclcs avecles lclrtime pct_day aveexe pct_exe;
	output out=tables1ab_summary_Commodity
	mean= avetime avecl aveclcs avecles lclrtime pct_day aveexe pct_exe;
	by tk tableside quad;
	weight _FREQ_;
run;

proc means data=tables1ab_all noprint;
	var _FREQ_;
	output out=tables1ab_summary_b
	sum=total_n;
	by tk tableside quad;
run;

data tables1ab_summary_Commodity;
	merge tables1ab_summary_Commodity tables1ab_summary_b;
	by tk tableside quad;
run;

		/*********** Table 1 by MarketOpen ************/

proc sort data=tables1ab_all tagsort; by marketopen tableside quad; run;

proc means data=tables1ab_all noprint;
	var avetime avecl aveclcs avecles lclrtime pct_day aveexe pct_exe;
	output out=tables1ab_summary_marketopen
	mean= avetime avecl aveclcs avecles lclrtime pct_day aveexe pct_exe;
	by marketopen tableside quad;
	weight _FREQ_;
run;

proc means data=tables1ab_all noprint;
	var _FREQ_;
	output out=tables1ab_summary_b
	sum=total_n;
	by marketopen tableside quad;
run;

data tables1ab_summary_marketopen;
	merge tables1ab_summary_marketopen tables1ab_summary_b;
	by marketopen tableside quad;
run;


		/*********** Table 1 by Rank of Futures Contract ************/

proc sort data=tables1ab_all tagsort; by rank tableside quad; run;

proc means data=tables1ab_all noprint;
	var avetime avecl aveclcs avecles lclrtime pct_day aveexe pct_exe;
	output out=tables1ab_summary_rank
	mean= avetime avecl aveclcs avecles lclrtime pct_day aveexe pct_exe;
	by rank tableside quad;
	weight _FREQ_;
run;

proc means data=tables1ab_all noprint;
	var _FREQ_;
	output out=tables1ab_summary_b
	sum=total_n;
	by rank tableside quad;
run;

data tables1ab_summary_rank;
	merge tables1ab_summary_rank tables1ab_summary_b;
	by rank tableside quad;
run;


		/*********** Table 1 by Days to Maturity ************/

proc sort data=tables1ab_all tagsort; by dtm tableside quad; run;

proc means data=tables1ab_all noprint;
	var avetime avecl aveclcs avecles lclrtime pct_day aveexe pct_exe;
	output out=tables1ab_summary_dtm
	mean= avetime avecl aveclcs avecles lclrtime pct_day aveexe pct_exe;
	by dtm tableside quad;
	weight _FREQ_;
run;

proc means data=tables1ab_all noprint;
	var _FREQ_;
	output out=tables1ab_summary_b
	sum=total_n;
	by dtm tableside quad;
run;

data tables1ab_summary_dtm;
	merge tables1ab_summary_dtm tables1ab_summary_b;
	by dtm tableside quad;
run;


		/*************Table 1 by Time Bucket **************/

proc sort data=tables1ab_time_all tagsort; by tbucket tableside quad; run;

proc means data=tables1ab_time_all noprint;
	var avetime avecl aveclcs avecles lclrtime pct_day aveexe pct_exe;
	output out=tables1ab_time_summary
	mean= avetime avecl aveclcs avecles lclrtime pct_day aveexe pct_exe;
	by tbucket tableside quad;
	weight _FREQ_;
run;

proc means data=tables1ab_time_all noprint;
	var _FREQ_;
	output out=tables1ab_summary_b
	sum=total_n;
	by tbucket tableside quad;
run;

data tables1ab_time_summary;
	merge tables1ab_time_summary tables1ab_summary_b;
	by tbucket tableside quad;
run;


/*********** Get the aggregated tables for the after clears ***********/

proc sort data=after_stats_all tagsort; by tableside quad _NAME_ tk dt; run;

proc means data=after_stats_all noprint;
	var reacttime resttime;
	output out=after_summary
	mean= avereact averest;
	by tableside quad _NAME_;
	weight N;
run;

proc means data=after_stats_all noprint;
	var N;
	output out=after_summary_b
	sum=total_n;
	by tableside quad _NAME_;
run;

data after_summary (drop=_type_ _freq_);
    merge after_summary after_summary_b;
	by tableside quad _NAME_;
run;


/************* After stats by Commodity ****************/

proc sort data=after_stats_all tagsort; by tk tableside quad _NAME_; run;

proc means data=after_stats_all noprint;
	var reacttime resttime;
	output out=after_summary_Commodity
	mean= avereact averest;
	by tk tableside quad _NAME_;
	weight N;
run;

proc means data=after_stats_all noprint;
	var N;
	output out=after_summary_b
	sum=total_n;
	by tk tableside quad _NAME_;
run;

data after_summary_Commodity (drop=_type_ _freq_);
    merge after_summary_Commodity after_summary_b;
	by tk tableside quad _NAME_;
run;


/************* After stats by MarketOpen ****************/

proc sort data=after_stats_all tagsort; by marketopen tableside quad _NAME_; run;

proc means data=after_stats_all noprint;
	var reacttime resttime;
	output out=after_summary_marketopen
	mean= avereact averest;
	by marketopen tableside quad _NAME_;
	weight N;
run;

proc means data=after_stats_all noprint;
	var N;
	output out=after_summary_b
	sum=total_n;
	by marketopen tableside quad _NAME_;
run;

data after_summary_marketopen (drop=_type_ _freq_);
    merge after_summary_marketopen after_summary_b;
	by marketopen tableside quad _NAME_;
run;


/************* After stats by Rank of Futures Contract ****************/

proc sort data=after_stats_all tagsort; by rank tableside quad _NAME_; run;

proc means data=after_stats_all noprint;
	var reacttime resttime;
	output out=after_summary_rank
	mean= avereact averest;
	by rank tableside quad _NAME_;
	weight N;
run;

proc means data=after_stats_all noprint;
	var N;
	output out=after_summary_b
	sum=total_n;
	by rank tableside quad _NAME_;
run;

data after_summary_rank (drop=_type_ _freq_);
    merge after_summary_rank after_summary_b;
	by rank tableside quad _NAME_;
run;


/************* After stats by Days to Maturity ****************/

proc sort data=after_stats_all tagsort; by dtm tableside quad _NAME_; run;

proc means data=after_stats_all noprint;
	var reacttime resttime;
	output out=after_summary_dtm
	mean= avereact averest;
	by dtm tableside quad _NAME_;
	weight N;
run;

proc means data=after_stats_all noprint;
	var N;
	output out=after_summary_b
	sum=total_n;
	by DTM tableside quad _NAME_;
run;

data after_summary_dtm (drop=_type_ _freq_);
    merge after_summary_dtm after_summary_b;
	by dtm tableside quad _NAME_;
run;


/************* After stats by time ****************/

proc sort data=after_stats_time_all tagsort; by tbucket tableside quad _NAME_; run;


proc means data=after_stats_time_all noprint;
	var reacttime resttime;
	output out=after_summary_time
	mean= avereact averest;
	by tbucket tableside quad _NAME_;
	weight N;
run;

proc means data=after_stats_time_all noprint;
	var N;
	output out=after_summary_b
	sum=total_n;
	by tbucket tableside quad _NAME_;
run;

data after_summary_time (drop=_type_ _freq_);
    merge after_summary_time after_summary_b;
	by tbucket tableside quad _NAME_;
run;


/*Export all tables*/
/*libname x 'D:\Summary Tables';*/
/*proc copy in=work out=x memtype=data;*/
/*run;*/




options noxwait noxsync;
 	x "&template";


			/*** Fill in Cluster stat aggregated information **/

filename result dde "excel|overall!r1c1:r2c2" notab;
data _NULL_;
	file result;
	put "table 1 --Summary";
run;

filename result dde "excel|overall!r3c1:r4c23" notab;
data _NULL_;
	file result;
	put "ASK" '09'x '09'x '09'x '09'x '09'x '09'x '09'x '09'x '09'x '09'x '09'x '09'x "BID"; 
	put "EWMA" '09'x "Clear" '09'x "N" '09'x "pct-day" '09'x "ave-time" '09'x "lclr-time" '09'x "ave-clr" '09'x "ave-clr/C" '09'x "ave-clr/E" '09'x "ave-exe" '09'x "pct-exe" '09'x
		'09'x "EWMA" '09'x "Clear" '09'x "N" '09'x "pct-day" '09'x "ave-time" '09'x "lclr-time" '09'x "ave-clr" '09'x "ave-clr/C" '09'x "ave-clr/E" '09'x "ave-exe" '09'x "pct-exe";
run;
filename result dde "excel|overall!r5c1:r8c11" notab;
data _NULL_;
	file result;
	set Tables1ab_summary;
	if Quad=1 then do; EWMA="Hi"; clear="Yes"; end;
	else if Quad=2 then do; EWMA="Lo"; Clear="Yes"; end;
	else if Quad=3 then do; EWMA="Lo"; Clear="No"; end;
	else do; EWMA="Hi"; Clear="No"; end;
	if Tableside="A" then
		Put EWMA '09'x Clear '09'x total_n '09'x pct_day '09'x avetime '09'x lclrtime '09'x avecl '09'x aveclcs '09'x avecles '09'x aveexe '09'x pct_exe;
run;

filename result dde "excel|overall!r5c13:r8c23" notab;
data _NULL_;
	file result;
	set Tables1ab_summary;
	if Quad=1 then do; EWMA="Hi"; clear="Yes"; end;
	else if Quad=2 then do; EWMA="Lo"; Clear="Yes"; end;
	else if Quad=3 then do; EWMA="Lo"; Clear="No"; end;
	else do; EWMA="Hi"; Clear="No"; end;
	if Tableside="B" then
		Put EWMA '09'x Clear '09'x total_n '09'x pct_day '09'x avetime '09'x lclrtime '09'x avecl '09'x aveclcs '09'x avecles '09'x aveexe '09'x pct_exe;
run;

							/**********  Fill in the general info on dates tickers and parameters **/
							
filename result dde "excel|overall!r3c25:r3c30" notab;
data _NULL_;
	file result;
	put "sheet" '09'x "ticker" '09'x '09'x "dates" '09'x '09'x "parameters";
run;

filename result dde "excel|overall!r4c25:r275c26" notab;
data _NULL_;
	file result;
	set tickers;
	put _N_ '09'x ticker;
run;

filename result dde "excel|overall!r4c28:r275c28" notab;
data _NULL_;
	file result;
	set dates;
	n=_N_+1;
	put date;
run;

filename result dde "excel|overall!r4c30:r275c32" notab;
data _NULL_;
file result;
	Put "Lequiv" '09'x "&Lequiv.";
	Put "Eslack" "09"x "&Eslack.";
	Put "Low" "09"x "&Low.";
	Put "High" "09"x "&High.";
	Put "warmup" "09"x "&warmup.";
	Put "dataplace" "09"x "&dataplace.";
	Put "template" "09"x "&template.";
	Put "Detail" "09"x "&detail.";
	Put "Look after" "09"x "&timetonothing.";
run;

							/***** After clear output-- summary  ***/
filename result dde "excel|overall!r10c1:r10c2" notab;
data _NULL_;
	file result;
	put "Table 2--After Clear Statistics";
run;

filename result dde "excel|overall!r12c1:r13c11" notab;
data _NULL_;
	file result;
	put "ASK" '09'x '09'x '09'x '09'x '09'x '09'x "BID"; 
	put "EWMA" '09'x "Result" '09'x "N" '09'x "Reaction T" '09'x "Resting T" '09'x '09'x 
        "EWMA" '09'x "Result" '09'x "N" '09'x "Reaction T" '09'x "Resting T" '09'x '09'x;
run;

filename result dde "excel|overall!r14c1:r25c5" notab;
data _NULL_;
	file result;
	set after_summary;
	if Quad=1 then EWMA="Hi"; else EWMA="Lo";
	if _NAME_ eq "addstat" then result="Lev Fill";
	else if _NAME_ eq "clsstat" then result="opp Fill";
	else if _NAME_ eq "clrstat" then result="clear";
	else if _NAME_ eq "exestat" then result="execute";
	else result="Nothing";
	if Tableside="A" then Put EWMA '09'x result '09'x total_n '09'x avereact '09'x averest;
run;

filename result dde "excel|overall!r14c7:r25c11" notab;
data _NULL_;
	file result;
	set after_summary;
	if Quad=1 then EWMA="Hi"; else EWMA="Lo";
	if _NAME_ eq "addstat" then result="Lev Fill";
	else if _NAME_ eq "clsstat" then result="opp Fill";
	else if _NAME_ eq "clrstat" then result="clear";
	else if _NAME_ eq "exestat" then result="execute";
	else result="Nothing";
	if Tableside="B" then Put EWMA '09'x result '09'x total_n '09'x avereact '09'x averest;
run;
	

					/***** cluster stats by Commodity **********/

filename result dde "excel|Commodity!r1c1:r1c2" notab;
data _NULL_;
	file result;
	put "table 1 -- By Commodity";
run;

filename result dde "excel|Commodity!r2c1:r2005c12" notab;
data _NULL_;
    file result;
	set tables1ab_summary_Commodity;
	if mod(_N_,8)=1 then do;
		put;
	    put "ASK"; 
		put "EWMA" '09'x "Clear" '09'x "N" '09'x "pct-day" '09'x "ave-time" '09'x "lclr-time" '09'x "ave-clr" '09'x "ave-clr/C" '09'x "ave-clr/E" '09'x "ave-exe" '09'x "pct-exe" '09'x "Commodity";
	end;
	if Quad=1 then do; EWMA="Hi"; clear="Yes"; end;
	else if Quad=2 then do; EWMA="Lo"; Clear="Yes"; end;
	else if Quad=3 then do; EWMA="Lo"; Clear="No"; end;
	else do; EWMA="Hi"; Clear="No"; end;
	If tableside="A" then Put EWMA '09'x Clear '09'x total_n '09'x pct_day '09'x avetime '09'x lclrtime '09'x avecl '09'x aveclcs '09'x avecles '09'x aveexe '09'x pct_exe '09'x tk;
run;


filename result dde "excel|Commodity!r2c13:r2005c23" notab;
data _NULL_;
    file result;
	set tables1ab_summary_Commodity;
	if mod(_N_,8)=1 then do;
		put;
	    put "BID"; 
		put "EWMA" '09'x "Clear" '09'x "N" '09'x "pct-day" '09'x "ave-time" '09'x "lclr-time" '09'x "ave-clr" '09'x "ave-clr/C" '09'x "ave-clr/E" '09'x "ave-exe" '09'x "pct-exe";
	end;
	if Quad=1 then do; EWMA="Hi"; clear="Yes"; end;
	else if Quad=2 then do; EWMA="Lo"; Clear="Yes"; end;
	else if Quad=3 then do; EWMA="Lo"; Clear="No"; end;
	else do; EWMA="Hi"; Clear="No"; end;
	If tableside="B" then Put EWMA '09'x Clear '09'x total_n '09'x pct_day '09'x avetime '09'x lclrtime '09'x avecl '09'x aveclcs '09'x avecles '09'x aveexe '09'x pct_exe;
run;


				/****** after clear output -- Commodity ******/
filename result dde "excel|Commodity!r1c26:r1c30" notab;
data _NULL_;
	file result;
	put "table 2 -- By Commodity";
run;

filename result dde "excel|Commodity!r2c26:r2005c31" notab;
data _NULL_;
    file result;
	set after_summary_Commodity;
	If tableside eq "A" then do;
		ltk=lag1(tk);
		if ltk ne tk then do;
		put;
	    put "ASK";
		put "EWMA" '09'x "Result" '09'x "N" '09'x "Reaction T" '09'x "Resting T" '09'x "Commodity";
		end;
	end;
	if Quad=1 then EWMA="Hi"; else EWMA="Lo";
	if _NAME_ eq "addstat" then result="Lev Fill";
	else if _NAME_ eq "clsstat" then result="opp Fill";
	else if _NAME_ eq "clrstat" then result="clear";
	else if _NAME_ eq "exestat" then result="execute";
	else result="Nothing";
	If tableside="A" then Put EWMA '09'x result '09'x total_n '09'x avereact '09'x averest '09'x tk;
run;

filename result dde "excel|Commodity!r2c32:r2005c36" notab;
data _NULL_;
    file result;
	set after_summary_Commodity;
	If tableside eq "B" then do;
		ltk=lag1(tk);
		if ltk ne tk then do;
		put;
	    put "BID";
		put "EWMA" '09'x "Result" '09'x "N" '09'x "Reaction T" '09'x "Resting T" '09'x;
		end;
	end;
	if Quad=1 then EWMA="Hi"; else EWMA="Lo";
	if _NAME_ eq "addstat" then result="Lev Fill";
	else if _NAME_ eq "clsstat" then result="opp Fill";
	else if _NAME_ eq "clrstat" then result="clear";
	else if _NAME_ eq "exestat" then result="execute";
	else result="Nothing";
	If tableside="B" then Put EWMA '09'x result '09'x total_n '09'x avereact '09'x averest '09'x;
run;
	

					/***** cluster stats by MarketOpen **********/

filename result dde "excel|MarketOpen!r1c1:r1c2" notab;
data _NULL_;
	file result;
	put "table 1 -- By MarketOpen";
run;

filename result dde "excel|MarketOpen!r2c1:r2005c12" notab;
data _NULL_;
    file result;
	set tables1ab_summary_MarketOpen;
	if mod(_N_,8)=1 then do;
		put;
	    put "ASK"; 
		put "EWMA" '09'x "Clear" '09'x "N" '09'x "pct-day" '09'x "ave-time" '09'x "lclr-time" '09'x "ave-clr" '09'x "ave-clr/C" '09'x "ave-clr/E" '09'x "ave-exe" '09'x "pct-exe" '09'x "MarketOpen";
	end;
	if Quad=1 then do; EWMA="Hi"; clear="Yes"; end;
	else if Quad=2 then do; EWMA="Lo"; Clear="Yes"; end;
	else if Quad=3 then do; EWMA="Lo"; Clear="No"; end;
	else do; EWMA="Hi"; Clear="No"; end;
	If tableside="A" then Put EWMA '09'x Clear '09'x total_n '09'x pct_day '09'x avetime '09'x lclrtime '09'x avecl '09'x aveclcs '09'x avecles '09'x aveexe '09'x pct_exe '09'x marketopen;
run;


filename result dde "excel|MarketOpen!r2c13:r2005c23" notab;
data _NULL_;
    file result;
	set tables1ab_summary_MarketOpen;
	if mod(_N_,8)=1 then do;
		put;
	    put "BID"; 
		put "EWMA" '09'x "Clear" '09'x "N" '09'x "pct-day" '09'x "ave-time" '09'x "lclr-time" '09'x "ave-clr" '09'x "ave-clr/C" '09'x "ave-clr/E" '09'x "ave-exe" '09'x "pct-exe";
	end;
	if Quad=1 then do; EWMA="Hi"; clear="Yes"; end;
	else if Quad=2 then do; EWMA="Lo"; Clear="Yes"; end;
	else if Quad=3 then do; EWMA="Lo"; Clear="No"; end;
	else do; EWMA="Hi"; Clear="No"; end;
	If tableside="B" then Put EWMA '09'x Clear '09'x total_n '09'x pct_day '09'x avetime '09'x lclrtime '09'x avecl '09'x aveclcs '09'x avecles '09'x aveexe '09'x pct_exe;
run;


				/****** after clear output -- MarketOpen ******/
filename result dde "excel|MarketOpen!r1c26:r1c30" notab;
data _NULL_;
	file result;
	put "table 2 -- By MarketOpen";
run;

filename result dde "excel|MarketOpen!r2c26:r2005c31" notab;
data _NULL_;
    file result;
	set after_summary_MarketOpen;
	If tableside eq "A" then do;
		lmarketopen=lag1(marketopen);
		if lmarketopen ne marketopen then do;
		put;
	    put "ASK";
		put "EWMA" '09'x "Result" '09'x "N" '09'x "Reaction T" '09'x "Resting T" '09'x "MarketOpen";
		end;
	end;
	if Quad=1 then EWMA="Hi"; else EWMA="Lo";
	if _NAME_ eq "addstat" then result="Lev Fill";
	else if _NAME_ eq "clsstat" then result="opp Fill";
	else if _NAME_ eq "clrstat" then result="clear";
	else if _NAME_ eq "exestat" then result="execute";
	else result="Nothing";
	If tableside="A" then Put EWMA '09'x result '09'x total_n '09'x avereact '09'x averest '09'x marketopen;
run;

filename result dde "excel|MarketOpen!r2c32:r2005c36" notab;
data _NULL_;
    file result;
	set after_summary_MarketOpen;
	If tableside eq "B" then do;
		lmarketopen=lag1(marketopen);
		if lmarketopen ne marketopen then do;
		put;
	    put "BID";
		put "EWMA" '09'x "Result" '09'x "N" '09'x "Reaction T" '09'x "Resting T" '09'x;
		end;
	end;
	if Quad=1 then EWMA="Hi"; else EWMA="Lo";
	if _NAME_ eq "addstat" then result="Lev Fill";
	else if _NAME_ eq "clsstat" then result="opp Fill";
	else if _NAME_ eq "clrstat" then result="clear";
	else if _NAME_ eq "exestat" then result="execute";
	else result="Nothing";
	If tableside="B" then Put EWMA '09'x result '09'x total_n '09'x avereact '09'x averest '09'x;
run;
	

					/***** cluster stats by Rank of Futures Contract **********/

filename result dde "excel|Rank!r1c1:r1c2" notab;
data _NULL_;
	file result;
	put "table 1 -- By rank";
run;

filename result dde "excel|Rank!r2c1:r2005c12" notab;
data _NULL_;
    file result;
	set tables1ab_summary_rank;
	if mod(_N_,8)=1 then do;
		put;
	    put "ASK"; 
		put "EWMA" '09'x "Clear" '09'x "N" '09'x "pct-day" '09'x "ave-time" '09'x "lclr-time" '09'x "ave-clr" '09'x "ave-clr/C" '09'x "ave-clr/E" '09'x "ave-exe" '09'x "pct-exe" '09'x "rank";
	end;
	if Quad=1 then do; EWMA="Hi"; clear="Yes"; end;
	else if Quad=2 then do; EWMA="Lo"; Clear="Yes"; end;
	else if Quad=3 then do; EWMA="Lo"; Clear="No"; end;
	else do; EWMA="Hi"; Clear="No"; end;
	If tableside="A" then Put EWMA '09'x Clear '09'x total_n '09'x pct_day '09'x avetime '09'x lclrtime '09'x avecl '09'x aveclcs '09'x avecles '09'x aveexe '09'x pct_exe '09'x rank;
run;


filename result dde "excel|Rank!r2c13:r2005c23" notab;
data _NULL_;
    file result;
	set tables1ab_summary_rank;
	if mod(_N_,8)=1 then do;
		put;
	    put "BID"; 
		put "EWMA" '09'x "Clear" '09'x "N" '09'x "pct-day" '09'x "ave-time" '09'x "lclr-time" '09'x "ave-clr" '09'x "ave-clr/C" '09'x "ave-clr/E" '09'x "ave-exe" '09'x "pct-exe";
	end;
	if Quad=1 then do; EWMA="Hi"; clear="Yes"; end;
	else if Quad=2 then do; EWMA="Lo"; Clear="Yes"; end;
	else if Quad=3 then do; EWMA="Lo"; Clear="No"; end;
	else do; EWMA="Hi"; Clear="No"; end;
	If tableside="B" then Put EWMA '09'x Clear '09'x total_n '09'x pct_day '09'x avetime '09'x lclrtime '09'x avecl '09'x aveclcs '09'x avecles '09'x aveexe '09'x pct_exe;
run;


				/****** after clear output -- rank ******/
filename result dde "excel|Rank!r1c26:r1c30" notab;
data _NULL_;
	file result;
	put "table 2 -- By rank";
run;

filename result dde "excel|Rank!r2c26:r2005c31" notab;
data _NULL_;
    file result;
	set after_summary_rank;
	If tableside eq "A" then do;
		lrank=lag1(rank);
		if lrank ne rank then do;
		put;
	    put "ASK";
		put "EWMA" '09'x "Result" '09'x "N" '09'x "Reaction T" '09'x "Resting T" '09'x "rank";
		end;
	end;
	if Quad=1 then EWMA="Hi"; else EWMA="Lo";
	if _NAME_ eq "addstat" then result="Lev Fill";
	else if _NAME_ eq "clsstat" then result="opp Fill";
	else if _NAME_ eq "clrstat" then result="clear";
	else if _NAME_ eq "exestat" then result="execute";
	else result="Nothing";
	If tableside="A" then Put EWMA '09'x result '09'x total_n '09'x avereact '09'x averest '09'x rank;
run;

filename result dde "excel|Rank!r2c32:r2005c36" notab;
data _NULL_;
    file result;
	set after_summary_rank;
	If tableside eq "B" then do;
		lrank=lag1(rank);
		if lrank ne rank then do;
		put;
	    put "BID";
		put "EWMA" '09'x "Result" '09'x "N" '09'x "Reaction T" '09'x "Resting T" '09'x;
		end;
	end;
	if Quad=1 then EWMA="Hi"; else EWMA="Lo";
	if _NAME_ eq "addstat" then result="Lev Fill";
	else if _NAME_ eq "clsstat" then result="opp Fill";
	else if _NAME_ eq "clrstat" then result="clear";
	else if _NAME_ eq "exestat" then result="execute";
	else result="Nothing";
	If tableside="B" then Put EWMA '09'x result '09'x total_n '09'x avereact '09'x averest '09'x;
run;

	
					/***** cluster stats by Days to Maturity **********/

filename result dde "excel|DTM!r1c1:r1c2" notab;
data _NULL_;
	file result;
	put "table 1 -- By DTM";
run;

filename result dde "excel|DTM!r2c1:r2005c12" notab;
data _NULL_;
    file result;
	set tables1ab_summary_dtm;
	if mod(_N_,8)=1 then do;
		put;
	    put "ASK"; 
		put "EWMA" '09'x "Clear" '09'x "N" '09'x "pct-day" '09'x "ave-time" '09'x "lclr-time" '09'x "ave-clr" '09'x "ave-clr/C" '09'x "ave-clr/E" '09'x "ave-exe" '09'x "pct-exe" '09'x "DTM";
	end;
	if Quad=1 then do; EWMA="Hi"; clear="Yes"; end;
	else if Quad=2 then do; EWMA="Lo"; Clear="Yes"; end;
	else if Quad=3 then do; EWMA="Lo"; Clear="No"; end;
	else do; EWMA="Hi"; Clear="No"; end;
	If tableside="A" then Put EWMA '09'x Clear '09'x total_n '09'x pct_day '09'x avetime '09'x lclrtime '09'x avecl '09'x aveclcs '09'x avecles '09'x aveexe '09'x pct_exe '09'x DTM;
run;


filename result dde "excel|DTM!r2c13:r2005c23" notab;
data _NULL_;
    file result;
	set tables1ab_summary_dtm;
	if mod(_N_,8)=1 then do;
		put;
	    put "BID"; 
		put "EWMA" '09'x "Clear" '09'x "N" '09'x "pct-day" '09'x "ave-time" '09'x "lclr-time" '09'x "ave-clr" '09'x "ave-clr/C" '09'x "ave-clr/E" '09'x "ave-exe" '09'x "pct-exe";
	end;
	if Quad=1 then do; EWMA="Hi"; clear="Yes"; end;
	else if Quad=2 then do; EWMA="Lo"; Clear="Yes"; end;
	else if Quad=3 then do; EWMA="Lo"; Clear="No"; end;
	else do; EWMA="Hi"; Clear="No"; end;
	If tableside="B" then Put EWMA '09'x Clear '09'x total_n '09'x pct_day '09'x avetime '09'x lclrtime '09'x avecl '09'x aveclcs '09'x avecles '09'x aveexe '09'x pct_exe;
run;


				/****** after clear output -- DTM ******/
filename result dde "excel|DTM!r1c26:r1c30" notab;
data _NULL_;
	file result;
	put "table 2 -- By DTM";
run;

filename result dde "excel|DTM!r2c26:r2005c31" notab;
data _NULL_;
    file result;
	set after_summary_dtm;
	If tableside eq "A" then do;
		ldtm=lag1(dtm);
		if ldtm ne dtm then do;
		put;
	    put "ASK";
		put "EWMA" '09'x "Result" '09'x "N" '09'x "Reaction T" '09'x "Resting T" '09'x "DTM";
		end;
	end;
	if Quad=1 then EWMA="Hi"; else EWMA="Lo";
	if _NAME_ eq "addstat" then result="Lev Fill";
	else if _NAME_ eq "clsstat" then result="opp Fill";
	else if _NAME_ eq "clrstat" then result="clear";
	else if _NAME_ eq "exestat" then result="execute";
	else result="Nothing";
	If tableside="A" then Put EWMA '09'x result '09'x total_n '09'x avereact '09'x averest '09'x DTM;
run;

filename result dde "excel|DTM!r2c32:r2005c36" notab;
data _NULL_;
    file result;
	set after_summary_dtm;
	If tableside eq "B" then do;
		ldtm=lag1(dtm);
		if ldtm ne dtm then do;
		put;
	    put "BID";
		put "EWMA" '09'x "Result" '09'x "N" '09'x "Reaction T" '09'x "Resting T" '09'x;
		end;
	end;
	if Quad=1 then EWMA="Hi"; else EWMA="Lo";
	if _NAME_ eq "addstat" then result="Lev Fill";
	else if _NAME_ eq "clsstat" then result="opp Fill";
	else if _NAME_ eq "clrstat" then result="clear";
	else if _NAME_ eq "exestat" then result="execute";
	else result="Nothing";
	If tableside="B" then Put EWMA '09'x result '09'x total_n '09'x avereact '09'x averest '09'x;
run;


					/***** cluster stats by time **********/

filename result dde "excel|time!r1c1:r1c2" notab;
data _NULL_;
	file result;
	put "table 1 -- By time";
run;

filename result dde "excel|time!r2c1:r2005c12" notab;
data _NULL_;
    file result;
	set tables1ab_time_summary;
	if mod(_N_,8)=1 then do;
		put;
	    put "ASK"; 
		put "EWMA" '09'x "Clear" '09'x "N" '09'x "pct-day" '09'x "ave-time" '09'x "lclr-time" '09'x "ave-clr" '09'x "ave-clr/C" '09'x "ave-clr/E" '09'x "ave-exe" '09'x "pct-exe" '09'x "time bucket";
	end;
	if Quad=1 then do; EWMA="Hi"; clear="Yes"; end;
	else if Quad=2 then do; EWMA="Lo"; Clear="YES"; end;
	else if Quad=3 then do; EWMA="L0"; Clear="NO"; end;
	else do; EWMA="HI"; Clear="NO"; end;
	If tableside="A" then Put EWMA '09'x Clear '09'x total_n '09'x pct_day '09'x avetime '09'x lclrtime '09'x avecl '09'x aveclcs '09'x avecles '09'x aveexe '09'x pct_exe '09'x tbucket;
run;


filename result dde "excel|time!r2c13:r2005c23" notab;
data _NULL_;
    file result;
	set tables1ab_time_summary;
	if mod(_N_,8)=1 then do;
		put;
	    put "BID"; 
		put "EWMA" '09'x "Clear" '09'x "N" '09'x "pct-day" '09'x "ave-time" '09'x "lclr-time" '09'x "ave-clr" '09'x "ave-clr/C" '09'x "ave-clr/E" '09'x "ave-exe" '09'x "pct-exe";
	end;
	if Quad=1 then do; EWMA="Hi"; clear="Yes"; end;
	else if Quad=2 then do; EWMA="Lo"; Clear="Yes"; end;
	else if Quad=3 then do; EWMA="Lo"; Clear="No"; end;
	else do; EWMA="Hi"; Clear="No"; end;
	If tableside="B" then Put EWMA '09'x Clear '09'x total_n '09'x pct_day '09'x avetime '09'x lclrtime '09'x avecl '09'x aveclcs '09'x avecles '09'x aveexe '09'x pct_exe;
run;


				/****** after clear output -- time ******/
filename result dde "excel|time!r1c26:r1c30" notab;
data _NULL_;
	file result;
	put "table 2 -- By time bucket";
run;

filename result dde "excel|time!r2c26:r2005c31" notab;
data _NULL_;
    file result;
	set after_summary_time;
	If tableside eq "A" then do;
		ltime=lag1(tbucket);
		if ltime ne tbucket then do;
		put;
	    put "ASK";
		put "EWMA" '09'x "Result" '09'x "N" '09'x "Reaction T" '09'x "Resting T" '09'x "time bucket";
		end;
	end;
	if Quad=1 then EWMA="Hi"; else EWMA="Lo";
	if _NAME_ eq "addstat" then result="Lev Fill";
	else if _NAME_ eq "clsstat" then result="opp Fill";
	else if _NAME_ eq "clrstat" then result="clear";
	else if _NAME_ eq "exestat" then result="execute";
	else result="Nothing";
	If tableside="A" then Put EWMA '09'x result '09'x total_n '09'x avereact '09'x averest '09'x tbucket;
run;

filename result dde "excel|time!r2c32:r2005c36" notab;
data _NULL_;
    file result;
	set after_summary_time;
	If tableside eq "B" then do;
		ltime=lag1(tbucket);
		if ltime ne tbucket then do;
		put;
	    put "BID";
		put "EWMA" '09'x "Result" '09'x "N" '09'x "Reaction T" '09'x "Resting T" '09'x;
		end;
	end;
	if Quad=1 then EWMA="Hi"; else EWMA="Lo";
	if _NAME_ eq "addstat" then result="Lev Fill";
	else if _NAME_ eq "clsstat" then result="opp Fill";
	else if _NAME_ eq "clrstat" then result="clear";
	else if _NAME_ eq "exestat" then result="execute";
	else result="Nothing";
	If tableside="B" then Put EWMA '09'x result '09'x total_n '09'x avereact '09'x averest '09'x;
run;



				/**********   Detailed Output ***********/  

%macro output_tables1ab;
%IF "&Detail."="YES" %THEN %DO;

/*			%let ticker=GE1_;*/
/*			%let date=20180101;*/
/*			%let k = 1;*/
/*			%let j = 1;*/

	%do j=1 %to &nticks;
		data tickers;
			set tickers;
			if _N_ eq &j then call symput("ticker", ticker);
		run;
		
		%Let shtnum=%eval(&j);
		filename result dde "excel|Sheet&shtnum.!r1c1:r2c2" notab;
		data _NULL_;
			file result;
			put "table 1 --&ticker";
		run; 
		filename result dde "excel|Sheet&shtnum.!r1c22:r1c24" notab;
		data _NULL_;
			file result;
			put "table 2 --&ticker";
		run; 

		
		%do k=1 %to &ndates;
			data dates;
				set dates;
                if _N_ eq &k then call symput("date", date);	
			run;

			%let ticker=%trim(&ticker);
			%let date=%trim(&date);

			%let rwbeg=%eval((&k -1)*7 +3);
			%let rwend=%eval(&rwbeg+1);

			%let aftbeg=%eval((&k-1)*13 +3);
			%let aftend=%eval(&aftbeg+1);

			filename result dde "excel|Sheet&shtnum.!r&rwbeg.c1:r&rwend.c20" notab;
			data _NULL_;
			file result;
			put "ASK" '09'x '09'x '09'x '09'x '09'x '09'x '09'x '09'x "&date." '09'x '09'x "BID"; 
			put "EWMA" '09'x "Clear" '09'x "N" '09'x "pct-day" "09"x "ave-time" '09'x "lclr-time" '09'x "ave-clr" '09'x "ave-clr/C" '09'x "ave-clr/E" '09'x '09'x
				"EWMA" '09'x "Clear" '09'x "N" '09'x "pct-day" "09"x "ave-time" '09'x "lclr-time" '09'x "ave-clr" '09'x "ave-clr/C" '09'x "ave-clr/E" '09'x;
			run;

			filename result dde "excel|Sheet&shtnum.!r&aftbeg.c22:r&aftend.c32" notab;
			data _NULL_;
			file result;
			put "ASK" '09'x '09'x '09'x '09'x "&date." '09'x '09'x "BID"; 
			put "EWMA" '09'x "Result" '09'x "N" '09'x "Reaction T" '09'x "Resting T" '09'x '09'x 
        		"EWMA" '09'x "Result" '09'x "N" '09'x "Reaction T" '09'x "Resting T" '09'x;
			run;
			
			%let rwbeg=%eval(&rwbeg+2);
			%let rwend=%eval(&rwbeg+5);
			filename result dde "excel|Sheet&shtnum.!r&rwbeg.c1:r&rwend.c10" notab;
			data _NULL_;
				file result;
				set cmehft.Tables1ab_&ticker.&date.;
				if Quad=1 then do; EWMA="Hi"; clear="Yes"; end;
				else if Quad=2 then do; EWMA="Lo"; Clear="Yes"; end;
				else if Quad=3 then do; EWMA="Lo"; Clear="No"; end;
				else do; EWMA="Hi"; Clear="No"; end;
				if Tableside="A" then
					Put EWMA '09'x Clear '09'x _FREQ_ '09'x pct_day '09'x avetime '09'x lclrtime '09'x avecl '09'x aveclcs '09'x avecles;
			run;

			filename result dde "excel|Sheet&shtnum.!r&rwbeg.c11:r&rwend.c19" notab;
			data _NULL_;
				file result;
				set cmehft.Tables1ab_&ticker.&date.;
				if Quad=1 then do; EWMA="Hi"; clear="Yes"; end;
				else if Quad=2 then do; EWMA="Lo"; Clear="Yes"; end;
				else if Quad=3 then do; EWMA="Lo"; Clear="No"; end;
				else do; EWMA="Hi"; Clear="No"; end;
				if Tableside="B" then
					Put EWMA '09'x Clear '09'x _FREQ_  '09'x pct_day '09'x avetime '09'x lclrtime '09'x avecl '09'x aveclcs '09'x avecles;
			run;

			%let aftbeg=%eval(&aftbeg+2);
			%let aftend=%eval(&aftbeg+11);
			filename result dde "excel|Sheet&shtnum.!r&aftbeg.c22:r&aftend.c26" notab;
			data _NULL_;
    			file result;
				set cmehft.after_stats_time_&ticker.&date.;
				if Quad=1 then EWMA="Hi"; else EWMA="Lo";
				if _NAME_ eq "addstat" then result="Lev Fill";
				else if _NAME_ eq "clsstat" then result="opp Fill";
				else if _NAME_ eq "clrstat" then result="clear";
				else if _NAME_ eq "exestat" then result="execute";
				else result="Nothing";
				if Tableside="A" then Put EWMA '09'x result '09'x N '09'x reacttime '09'x resttime;
			run;

			filename result dde "excel|Sheet&shtnum.!r&aftbeg.c28:r&aftend.c32" notab;
			data _NULL_;
    			file result;
				set cmehft.after_stats_time_&ticker.&date.;
				if Quad=1 then EWMA="Hi"; else EWMA="Lo";
				if _NAME_ eq "addstat" then result="Lev Fill";
				else if _NAME_ eq "clsstat" then result="opp Fill";
				else if _NAME_ eq "clrstat" then result="clear";
				else if _NAME_ eq "exestat" then result="execute";
				else result="Nothing";
				if Tableside="B" then Put EWMA '09'x result '09'x N '09'x reacttime '09'x resttime;
			run;
		%END;
	%END;
%END;
%MEND	output_tables1ab;

%output_tables1ab;

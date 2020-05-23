option nosource nonotes;
libname reg "C:\Chao\Summary";
/* Get file list of all the dataset */
/**** make sure no previous runs   ******/
proc datasets lib=work kill noprint;
run;
quit;


/* The root direction keywords of all 1st and 2nd nearest futures */
data reg.tickers;
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
	set reg.tickers;
	call symput("nticks",_N_);
run;


/*Input regression data*/
%let n = 15;
/*%let ticker = zs2;*/

%macro regression_tables;
	%do j=1 %to &nticks.;
		data tickers;
			set tickers;
			if _N_ eq &j then do;
			call symput("ticker", ticker);
			call symput("tick", tick);
			end;
		run;
        %let ticker=%trim(&ticker);



	data test;
		set reg.reg_all_&ticker.;
	run;

	proc sort data=test noduprecs;
		by _all_ ;
	Run;

	proc sort data=test noduprecs;
		by date groups1;
	Run;

	/*Add time interval sequence, scale price by tick*/
	data test;
		set test;
		Avemidquote = Avemidquote/100;
		AveVWmidquote = AveVWmidquote/100;
		diffmin = groups1 - lag1(groups1);
		retain sequence 0;
		if diffmin ^= 1 then 
			sequence = 0;
		else 
			sequence + 1;
	run;

	/*Calculate Historical Volatility using EWMA measures of Return and Volatility*/
	data test;
		set test;
		retain return .;
		retain return2 .;
		retain vol .;
		retain vol2 .;
		retain EWMAVol 0;
		retain EWMAVWVol 0;
		if sequence ^= 0 then do;
			return = log(Avemidquote/lag(Avemidquote));
			return2 = log(AveVWmidquote/lag(AveVWmidquote));
		end;
	run;

	data test;
		set test;
		EWMAreturn = return;
		EWMAVWreturn = return2;
		if sequence ^= 0 then do;
			EWMAreturn=lag(EWMAreturn) + 2/(&n. + 1)*(return**2-lag(EWMAreturn));
			EWMAVWreturn=lag(EWMAVWreturn) + 2/(&n. + 1)*(return2**2-lag(EWMAVWreturn));
			vol = (return-EWMAreturn)**2;
			vol2 = (return2-EWMAVWreturn)**2;
			end;
		else if sequence = 0 then do;
			EWMAreturn=lag(EWMAreturn);
			EWMAVWreturn=lag(EWMAVWreturn);
			end;
	run;

	data test;
		set test;
		retain contract1-contract5 dum18-dum30 0;
		if sequence ^= 0 then do;
			EWMAVol=lag(EWMAVol) + 2/(&n. + 1)*(vol-lag(EWMAVol));
			EWMAVWVol=lag(EWMAVWVol) + 2/(&n. + 1)*(vol2-lag(EWMAVWVol));
			end;
		else if sequence = 0 then do;
			EWMAVol=lag(EWMAVol);
			EWMAVWVol=lag(EWMAVWVol);
			end;
		Volatility = sqrt(EWMAVol)*1440*252;
		VWVolatility = sqrt(EWMAVWVol)*1440*252;
	run;

	/*Add conditions on the data*/
	data test;
		set test;
/*		TotalActivity = TradeA+TradeB+CancelVolA+CancelVolB+NewA+NewB;*/
		if sequence < &n. then Volatility = . and VWVolatility = .;
		if workday = 1
		and (groups30 > 17 and groups30 < 31)
	/*	and groups30 < 33*/
	;
	/*	if symbol = "ESH8" then contract1=1;*/
	/*	if symbol = "ESM8" then contract2=1;*/
	/*	if symbol = "ESU8" then contract3=1;*/
	/*	if symbol = "ESZ8" then contract4=1;*/
	/*	if symbol = "ESH9" then contract5=1;*/
	/*	if groups30 = 1 then dum1=1;*/
	/*	if groups30 = 2 then dum2=1;*/
	/*	if groups30 = 3 then dum3=1;*/
	/*	if groups30 = 4 then dum4=1;*/
	/*	if groups30 = 5 then dum5=1;*/
	/*	if groups30 = 6 then dum6=1;*/
	/*	if groups30 = 7 then dum7=1;*/
	/*	if groups30 = 8 then dum8=1;*/
	/*	if groups30 = 9 then dum9=1;*/
	/*	if groups30 = 10 then dum10=1;*/
	/*	if groups30 = 11 then dum11=1;*/
	/*	if groups30 = 12 then dum12=1;*/
	/*	if groups30 = 13 then dum13=1;*/
	/*	if groups30 = 14 then dum14=1;*/
	/*	if groups30 = 15 then dum15=1;*/
	/*	if groups30 = 16 then dum16=1;*/
	/*	if groups30 = 17 then dum17=1;*/
		if groups30 = 18 then dum18=1;
		if groups30 = 19 then dum19=1;
		if groups30 = 20 then dum20=1;
		if groups30 = 21 then dum21=1;
		if groups30 = 22 then dum22=1;
		if groups30 = 23 then dum23=1;
		if groups30 = 24 then dum24=1;
		if groups30 = 25 then dum25=1;
		if groups30 = 26 then dum26=1;
		if groups30 = 27 then dum27=1;
		if groups30 = 28 then dum28=1;
		if groups30 = 29 then dum29=1;
		if groups30 = 30 then dum30=1;
	/*	if groups30 = 31 then dum31=1;*/
	/*	if groups30 = 32 then dum32=1;*/
	/*	if groups30 = 33 then dum33=1;*/
	/*	if groups30 = 34 then dum34=1;*/
	/*	if groups30 = 35 then dum35=1;*/
	/*	if groups30 = 36 then dum36=1;*/
	/*	if groups30 = 37 then dum37=1;*/
	/*	if groups30 = 38 then dum38=1;*/
	/*	if groups30 = 39 then dum39=1;*/
	/*	if groups30 = 40 then dum40=1;*/
	/*	if groups30 = 41 then dum41=1;*/
	/*	if groups30 = 42 then dum42=1;*/
	/*	if groups30 = 43 then dum43=1;*/
	/*	if groups30 = 44 then dum44=1;*/
	/*	if groups30 = 45 then dum45=1;*/
	/*	if groups30 = 46 then dum46=1;*/
	/*	if groups30 = 47 then dum47=1;*/
	/*	if groups30 = 48 then dum48=1;*/
		drop return return2 vol vol2 ewmavol ewmavwvol ewmareturn ewmavwreturn diffmin sequence;
	run;


	ods csv file = "C:\Chao\Regressions &ticker..csv";
	proc means data = test mean;
		var TradeA TradeB 
	/*	DeleteA DeleteB*/
	/*	VolumeDecreseA VolumeDecreseB*/
	/*	QueueChangesA QueueChangesB*/
		CancelVolA CancelVolB
		NewA NewB;
		class groups30;
	run;


	ods graphics off;

	PROC CORR DATA=test;
		VAR TradeA TradeB 
	/*	DeleteA DeleteB*/
	/*	VolumeDecreseA VolumeDecreseB*/
	/*	QueueChangesA QueueChangesB*/
		CancelVolA CancelVolB
		NewA NewB;
	RUN;

	/*proc reg data=test;*/
	/*	model AveDepthA = TradeB VolumeDecreseA NewA AveVWMidQuote VWVolatility contract1-contract5 dum18-dum30  / vif tol;*/
	/*	model AveDepthB = TradeA VolumeDecreseB NewB AveVWMidQuote VWVolatility contract1-contract5 dum18-dum30  / vif tol;*/
	/*	model AveBAS = TradeB VolumeDecreseA NewA AveVWMidQuote VWVolatility contract1-contract5 dum18-dum30  / vif tol;*/
	/*	model AveBAS = TradeA VolumeDecreseB NewB AveVWMidQuote VWVolatility contract1-contract5 dum18-dum30  / vif tol;*/
	/*	model AveSBAS = TradeB VolumeDecreseA NewA AveVWMidQuote VWVolatility contract1-contract5 dum18-dum30  / vif tol;*/
	/*	model AveSBAS = TradeA VolumeDecreseB NewB AveVWMidQuote VWVolatility contract1-contract5 dum18-dum30  / vif tol;*/
	/*run;*/
	/*quit;*/
	/**/
	/*proc reg data=test;*/
	/*	model AveDepthA = TradeB CancelVolA NewA AveVWMidQuote VWVolatility contract1-contract5 dum18-dum30  / vif tol;*/
	/*	model AveDepthB = TradeA CancelVolB NewB AveVWMidQuote VWVolatility contract1-contract5 dum18-dum30  / vif tol;*/
	/*	model AveBAS = TradeB CancelVolA NewA AveVWMidQuote VWVolatility contract1-contract5 dum18-dum30  / vif tol;*/
	/*	model AveBAS = TradeA CancelVolB NewB AveVWMidQuote VWVolatility contract1-contract5 dum18-dum30  / vif tol;*/
	/*	model AveSBAS = TradeB CancelVolA NewA AveVWMidQuote VWVolatility contract1-contract5 dum18-dum30  / vif tol;*/
	/*	model AveSBAS = TradeA CancelVolB NewB AveVWMidQuote VWVolatility contract1-contract5 dum18-dum30  / vif tol;*/
	/*run;*/
	/*quit;*/

	proc reg data=test;
		model AveDepthA = TradeB CancelVolA NewA AveVWMidQuote VWVolatility dum18-dum30/ vif tol ;
		model AveDepthB = TradeA CancelVolB NewB AveVWMidQuote VWVolatility dum18-dum30/ vif tol ;
		model AveBAS = TradeB CancelVolA NewA AveVWMidQuote VWVolatility dum18-dum30/ vif tol ;
		model AveBAS = TradeA CancelVolB NewB AveVWMidQuote VWVolatility dum18-dum30/ vif tol ;
	run;
	quit;

	/*proc reg data=test;*/
	/*	model AveDepthA = TradeB CancelVolA NewA AveVWMidQuote VWVolatility dum1-dum48/ vif tol ;*/
	/*	model AveDepthB = TradeA CancelVolB NewB AveVWMidQuote VWVolatility dum1-dum48/ vif tol ;*/
	/*	model AveBAS = TradeB CancelVolA NewA AveVWMidQuote VWVolatility dum1-dum48/ vif tol ;*/
	/*	model AveBAS = TradeA CancelVolB NewB AveVWMidQuote VWVolatility dum1-dum48/ vif tol ;*/
	/*run;*/
	/*quit;*/

	ods csv close;
	%END;
%MEND	regression_tables;

%regression_tables;


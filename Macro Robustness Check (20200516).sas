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
/*%let ticker = es1_;*/

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
		rename VolumeDecreseA = CancelA;
		rename VolumeDecreseB = CancelB;
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
		retain return .;retain return2 .;retain vol .;retain vol2 .;retain EWMAVol 0;retain EWMAVWVol 0;
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

	data test;
		set test;
		if sequence >= 1 then do;
			AveBAS_1 = lag(AveBAS);
			AvedepthB_1 = lag(AvedepthB);
			AvedepthA_1 = lag(AvedepthA);
			TradeB_1 = lag(TradeB);
			TradeA_1 = lag(TradeA);
			CancelB_1 = lag(CancelB);
			CancelA_1 = lag(CancelA);
			NewB_1 = lag(NewB);
			NewA_1 = lag(NewA);
			AveIB_1 = lag(AveIB); 
			AveVWMidQuote_1 = lag(AveVWMidQuote); 
			VWVolatility_1 = lag(VWVolatility);
		end;
		if sequence >=2 then do;
			AveBAS_2 = lag(AveBAS_1);
			AvedepthB_2 = lag(AvedepthB_1);
			AvedepthA_2 = lag(AvedepthA_1);
			TradeB_2 = lag(TradeB_1);
			TradeA_2 = lag(TradeA_1);
			CancelB_2 = lag(CancelB_1);
			CancelA_2 = lag(CancelA_1);
			NewB_2 = lag(NewB_1);
			NewA_2 = lag(NewA_1);
			AveIB_2 = lag(AveIB_1); 
			AveVWMidQuote_2 = lag(AveVWMidQuote_1); 
			VWVolatility_2 = lag(VWVolatility_1);
		end;
		if sequence >= 3 then do;
			AveBAS_3 = lag(AveBAS_2);
			AvedepthB_3 = lag(AvedepthB_2);
			AvedepthA_3 = lag(AvedepthA_2);
			TradeB_3 = lag(TradeB_2);
			TradeA_3 = lag(TradeA_2);
			CancelB_3 = lag(CancelB_2);
			CancelA_3 = lag(CancelA_2);
			NewB_3 = lag(NewB_2);
			NewA_3 = lag(NewA_2);
			AveIB_3 = lag(AveIB_2); 
			AveVWMidQuote_3 = lag(AveVWMidQuote_2); 
			VWVolatility_3 = lag(VWVolatility_2);
		end;
		if sequence >= 4 then do;
			AveBAS_4 = lag(AveBAS_3);
			AvedepthB_4 = lag(AvedepthB_3);
			AvedepthA_4 = lag(AvedepthA_3);
			TradeB_4 = lag(TradeB_3);
			TradeA_4 = lag(TradeA_3);
			CancelB_4 = lag(CancelB_3);
			CancelA_4 = lag(CancelA_3);
			NewB_4 = lag(NewB_3);
			NewA_4 = lag(NewA_3);
			AveIB_4 = lag(AveIB_3); 
			AveVWMidQuote_4 = lag(AveVWMidQuote_3); 
			VWVolatility_4 = lag(VWVolatility_3);
		end;
		if sequence >= 5 then do;
			AveBAS_5 = lag(AveBAS_4);
			AvedepthB_5 = lag(AvedepthB_4);
			AvedepthA_5 = lag(AvedepthA_4);
			TradeB_5 = lag(TradeB_4);
			TradeA_5 = lag(TradeA_4);
			CancelB_5 = lag(CancelB_4);
			CancelA_5 = lag(CancelA_4);
			NewB_5 = lag(NewB_4);
			NewA_5 = lag(NewA_4);
			AveIB_5 = lag(AveIB_4); 
			AveVWMidQuote_5 = lag(AveVWMidQuote_4); 
			VWVolatility_5 = lag(VWVolatility_4);
		end;
	run;

	data test;
		set test;
		if sequence < &n. then Volatility = . and VWVolatility = .;
		if workday = 1 and (groups30 > 17 and groups30 < 31);
		drop return return2 vol vol2 ewmavol ewmavwvol ewmareturn ewmavwreturn diffmin sequence;
	run;

	/*Linear regression following Frino's paper*/
	ods csv file = "C:\Chao\Regression of Each Contract\Causality test 1-5 &ticker..csv";

/*	proc means data = test n mean;*/
/*		var AveBAS;*/
/*		class groups30;*/
/*	run;*/
/**/
/*	ods graphics off;*/


	/*Proc GLM Granger Caulsality test 1-5 liquidity on IVs*/

/*	proc glm data=test;*/
/*	   class symbol groups30;*/
/*	   model AvedepthA = CancelA_1-CancelA_5 TradeB_1-TradeB_5 NewA_1-NewA_5 AveVWMidQuote_1-AveVWMidQuote_5*/
/*		VWVolatility_1-VWVolatility_5 symbol groups30/ solution;*/
/*	   ods select ParameterEstimates;*/
/*	quit;*/
/**/
/*	proc glm data=test;*/
/*	   class symbol groups30;*/
/*	   model AvedepthB = CancelB_1-CancelB_5 TradeA_1-TradeA_5 NewB_1-NewB_5 AveVWMidQuote_1-AveVWMidQuote_5*/
/*		VWVolatility_1-VWVolatility_5 symbol groups30/ solution;*/
/*	   ods select ParameterEstimates;*/
/*	quit;*/
/**/
/*	proc glm data=test;*/
/*	   class symbol groups30;*/
/*	   model AveBAS = CancelA_1-CancelA_5 TradeB_1-TradeB_5 NewA_1-NewA_5  AveVWMidQuote_1-AveVWMidQuote_5*/
/*		VWVolatility_1-VWVolatility_5 symbol groups30/ solution;*/
/*	   ods select ParameterEstimates;*/
/*	quit;*/
/**/
/*	proc glm data=test;*/
/*	   class symbol groups30;*/
/*	   model AveBAS = CancelB_1-CancelB_5 TradeA_1-TradeA_5 NewB_1-NewB_5  AveVWMidQuote_1-AveVWMidQuote_5*/
/*		VWVolatility_1-VWVolatility_5 symbol groups30/ solution;*/
/*	   ods select ParameterEstimates;*/
/*	quit;*/



	/*VARMAX Causality Testing*/

	proc varmax data=test;
		where AveBAS ne . and AvedepthA ne . and AvedepthB ne . and AveBAS ne . and CancelA ne . and CancelB ne .
		and TradeA ne . and TradeB ne . and NewA ne . and NewB ne . and AveVWMidQuote ne . and VWVolatility ne .;
		model AveBAS = TradeB CancelA NewA AveVWMidQuote VWVolatility / p=5 noprint;
		causal group1=(AveBAS) group2=(TradeB CancelA NewA AveVWMidQuote VWVolatility);
		causal group1=(AveBAS) group2=(CancelA AveVWMidQuote VWVolatility);
		causal group1=(AveBAS) group2=(TradeB);
		causal group1=(AveBAS) group2=(CancelA);
		causal group1=(AveBAS) group2=(NewA);
	run;

	proc varmax data=test;
		where AveBAS ne . and AvedepthA ne . and AvedepthB ne . and AveBAS ne . and CancelA ne . and CancelB ne .
		and TradeA ne . and TradeB ne . and NewA ne . and NewB ne . and AveVWMidQuote ne . and VWVolatility ne .;
		model AveBAS = TradeA CancelB NewB AveVWMidQuote VWVolatility/ p=5 noprint;
		causal group1=(AveBAS) group2=(TradeA CancelB NewB AveVWMidQuote VWVolatility);
		causal group1=(AveBAS) group2=(CancelB AveVWMidQuote VWVolatility);
		causal group1=(AveBAS) group2=(TradeA);
		causal group1=(AveBAS) group2=(CancelB);
		causal group1=(AveBAS) group2=(NewB);
	run;

	proc varmax data=test;
		where AveBAS ne . and AvedepthA ne . and AvedepthB ne . and AveBAS ne . and CancelA ne . and CancelB ne .
		and TradeA ne . and TradeB ne . and NewA ne . and NewB ne . and AveVWMidQuote ne . and VWVolatility ne .;
		model AvedepthA = TradeB CancelA NewA AveVWMidQuote VWVolatility/ p=5 noprint;
		causal group1=(AvedepthA) group2=(TradeB CancelA NewA AveVWMidQuote VWVolatility);
		causal group1=(AvedepthA) group2=(CancelA AveVWMidQuote VWVolatility);
		causal group1=(AvedepthA) group2=(TradeB);
		causal group1=(AvedepthA) group2=(CancelA);
		causal group1=(AvedepthA) group2=(NewA);
	run;

	proc varmax data=test;
		where AveBAS ne . and AvedepthA ne . and AvedepthB ne . and AveBAS ne . and CancelA ne . and CancelB ne .
		and TradeA ne . and TradeB ne . and NewA ne . and NewB ne . and AveVWMidQuote ne . and VWVolatility ne .;
		model AvedepthB = TradeA CancelB NewB AveVWMidQuote VWVolatility/ p=5 noprint;
		causal group1=(AvedepthB) group2=(TradeA CancelB NewB AveVWMidQuote VWVolatility);
		causal group1=(AvedepthB) group2=(CancelB AveVWMidQuote VWVolatility);
		causal group1=(AvedepthB) group2=(TradeA);
		causal group1=(AvedepthB) group2=(CancelB);
		causal group1=(AvedepthB) group2=(NewB);
	run;


/*proc model data=test out=test2;*/
/*   endogenous TradeA CancelB;*/
/*   TradeA = CancelB + NewB + AveVWMidQuote + VWVolatility;*/
/*   CancelB = TradeA + NewB + AveVWMidQuote + VWVolatility;*/
/*   fit TradeA CancelB / ols 2sls hausman;*/
/*   instruments NewB;*/
/*run;*/

/*	proc varmax data=test;*/
/*		where AveBAS ne . and AvedepthA ne . and AvedepthB ne . and AveBAS ne . and CancelA ne . and CancelB ne .*/
/*		and TradeA ne . and TradeB ne . and NewA ne . and NewB ne . ;*/
/*		model AveBAS = CancelA TradeB NewA/ p=5 noprint;*/
/*		causal group1=(CancelA) group2=(AveBAS);*/
/*		causal group1=(CancelA) group2=(TradeB NewA);*/
/*	run;*/
/**/
/*	proc varmax data=test;*/
/*		where AveBAS ne . and AvedepthA ne . and AvedepthB ne . and AveBAS ne . and CancelA ne . and CancelB ne .*/
/*		and TradeA ne . and TradeB ne . and NewA ne . and NewB ne . ;*/
/*		model AveBAS = CancelB TradeA NewB/ p=5 noprint;*/
/*		causal group1=(CancelB) group2=(AveBAS);*/
/*		causal group1=(CancelB) group2=(TradeA NewB);*/
/*	run;*/
/**/
/*	proc varmax data=test;*/
/*		where AveBAS ne . and AvedepthA ne . and AvedepthB ne . and AveBAS ne . and CancelA ne . and CancelB ne .*/
/*		and TradeA ne . and TradeB ne . and NewA ne . and NewB ne . ;*/
/*		model AvedepthA = CancelA TradeB NewA/ p=5 noprint;*/
/*		causal group1=(CancelA) group2=(AvedepthA);*/
/*		causal group1=(CancelA) group2=(TradeB NewA);*/
/*	run;*/
/**/
/*	proc varmax data=test;*/
/*		where AveBAS ne . and AvedepthA ne . and AvedepthB ne . and AveBAS ne . and CancelA ne . and CancelB ne .*/
/*		and TradeA ne . and TradeB ne . and NewA ne . and NewB ne . ;*/
/*		model AvedepthB = CancelB TradeA NewB/ p=5 noprint;*/
/*		causal group1=(CancelB) group2=(AvedepthB);*/
/*		causal group1=(CancelB) group2=(TradeA NewB);*/
/*	run;*/





	ods csv close;

	%END;
%MEND	regression_tables;

%regression_tables;


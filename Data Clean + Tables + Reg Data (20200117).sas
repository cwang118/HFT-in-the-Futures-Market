options nosource nonotes;
libname cmehft "D:\ES";
libname tables "D:\ES\Tables";
%Let path = D:\ES\;
%Let datadict=D:\DatesAndFutures.csv;
%Let template=E:\cmehft\tables1ab.xlsx;
/* Get file list of all the dataset */
/**** make sure no previous runs   ******/
proc datasets lib=work kill noprint;
run;
quit;

/* The root direction keywords of all 1st and 2nd nearest futures */
data cmehft.tickers;
	input ticker $6. tick 8.;
	cards;
ES1_	25
;
run;

%macro get_filenames(location,dataname);
filename _dir_ "%bquote(&location.)";
data &dataname. (keep=filename);
	handle=dopen( '_dir_' );
	if handle > 0 then do;
	count=dnum(handle);
	do i=1 to count;
		filename=dread(handle,i);
		output &dataname.;
	end;
	end;
	rc=dclose(handle);
run;
filename _dir_ clear;
%mend;

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

/* Import the information of all dates + contracts 2018 */

proc import datafile="&datadict."
	out=dates
	dbms=csv
	replace;
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

/*******************rescale dtm*************************/

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
	    else quest(i) = 0;
	end;
	drop holiday weekday i;
run;

%let ndates=365;

%LET Lequiv=10;
%LET Low=_p10_;
%LET High=_p90_;
%LET warmup=100;
%Let Detail=YES;
%Let Eslack=.10;
%Let TimeToNothing=5;
%let gap=3600;
%let endtime = 54000000000000;

%macro create_agg_tables;
	%do j=1 %to &nticks.;
/*	%let j=1;*/
/*	%let i=1;*/
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

		%get_filenames("&path.&ticker.\depth", &ticker.depth);
		%get_filenames("&path.&ticker.\event", &ticker.event);

		data cmehft.&ticker.depth;
			set &ticker.depth;
			Date = substr(filename,6,8);
			commodity = substr(filename,1,4);
			rename filename = depth;
		run;

		proc sort data = cmehft.&ticker.depth;
			by Date;
		run;

		data cmehft.&ticker.event;
			set &ticker.event;
			Date = substr(filename,6,8);
			commodity = substr(filename,1,4);
			rename filename = event;
		run;

		proc sort data = cmehft.&ticker.event;
			by Date;
		run;

		data cmehft.&ticker.InputInfo;
			merge cmehft.&ticker.depth cmehft.&ticker.event;
			by Date;
			ticker = "&ticker";
		run;
		
		data cmehft.&ticker.InputInfo;
			set cmehft.&ticker.InputInfo;
			if depth = '' or event = '' then delete;
		run;

		data cmehft.&ticker.InputInfo;
			set cmehft.&ticker.InputInfo;
			call symput ("ndates",_N_);/*npairs means how many days we have*/
		run;

		%put &ndates;

	%do i=1 %to &ndates.;
/*	%let i = 37;*/
		data cmehft.&ticker.InputInfo;
			set cmehft.&ticker.InputInfo;
			if _N_ eq &i then do;
				call symput("depth", trim(depth));
				call symput("event", trim(event));
				call symput("Date", trim(Date));
				call symput("commodity", trim(commodity));
			end;
		run;
		
		data _null_;
			set dates2;
			if Date = &Date. then do;
				call symput("KeyVar", symbol_&ticker.);
				call symput("workday", workday);
				call symput("dtm", dtm_&ticker.);
/*				%put &KeyVar. &workday. &dtm.;*/
			end;
		run;
		%put &commodity. &KeyVar. &date &ticker &rank &workday. &dtm.;

/*		%let event = GCG8_20180206_10_OptRec_Events_20180206.csv;*/
/*		%let depth = GCG8_20180206_10_Depth_Trades_Events_20180206.csv;*/
/*		%let date = 20180206;*/
/*		%let ticker = gc1_;*/
/*		%let tick = 1;*/
/*		%let workday = 1;*/
/*		%let dtm = 1;*/
/*		%let rank = 1;*/

/*		Read File*/
		filename events "&path.&ticker.\event\&event";
		filename depths "&path.&ticker.\depth\&depth";

		/*Read All Event Data*/
		data AllEvent (drop=s3-s13 lgseq);
		infile events dsd LRECL=1024 missover DLM=",";
		informat key $8. Time Time18.9 s3-s13 $30.;
		format Time Time18.9;
		input Key Time s3 s4 s5 S6 s7 s8 s9 s10 s11 s12 s13;
		retain lgseq 0;
		IF Key="MBO" then do;
/*			GSEQ=input(s3,best30.);*/
/*			Template=input(s4, best30.);*/
/*			SecurityID=input(s5, best30.);*/
			UAction=s6;
			Side=input(s7,best30.);
			Price=input(s8,best30.);
			Volume=input(s9, best30.);
			OrderID=input(s10, best30.);
			Priority=input(s11, best30.);
			end; 
		ELSE IF KEY="Q" then do;
/*			GSEQ=input(s3,best30.);*/
			Seq=input(s4,best30.);
			bitstr=put(s5,$8.);
/*			Template=input(s6,best30.);*/
/*			SecurityID=input(s7,best30.);*/
/*			TxtSide=s8;*/
			UAction=s9;
/*			Plevel=input(s10, best30.);*/
			Price=input(s11, best30.);
			Volume=input(s12, best30.);
/*			orders=input(s13, best30.);*/
			end;
		Else IF Key="m" then do;
/*			SecurityID=input(s3,best30.);*/
/*			matchNum=input(s4,best30.);*/
			OrderID=input(s6, best30.);
			Volume=input(s7, best30.);
/*			GSEQ=lgseq;*/
			end;
		Else IF Key="T" then do;
/*		   	GSEQ=input(s3,best30.);*/
			bitstr=put(s5,$8.);
/*			MOSide=input(s10,best30.);*/
			Price=input(s11,best30.);
			Volume=input(s12, best30.);
/*			Quantity=input(s13, best30.);*/
			end;
/*		else ERROR =2;*/
/*		lgseq=gseq;*/
		run;

		data TMBOQandM;
			set AllEvent;
			format orderid best13.;
			if (key='T' and substr(bitstr,6,1) ^= "1") or key='MBO' or key='m' or (key='Q' and substr(bitstr,3,1) = "1");
			if key = "Q" then sorti = 1;
			else if key = "MBO" then sorti = 2;
			else if key = "m" then sorti = 3;
			else if key = "T" then sorti = 4;
			id_main=_N_;
		run;

		/*Edit Dates*/
		data TMBOQandM;
			set TMBOQandM;
			if _N_ = 1 then call symput ("timefirst", NS);
			format NS 32.;
			NS = time * 1000000000;
			diff=lag(time)-time;
		run;

		data TMBOQandM;
			set TMBOQandM;
			/*If there is a big time diff, it's another day*/
			if diff>&gap. then call symput ("gg", _N_);
			else do; %let gg = -1; end;
		run;
		%put &gg;
		
		data TMBOQandM;
			set TMBOQandM;
			if &gg = -1 and &timefirst < &endtime then Date = "&Date.";
			else if &gg = -1 and &timefirst >= &endtime then 
				Date=put(intnx('day',input("&Date.",yymmdd8.),-1),yymmddn8.);
			if _N_>= &gg then Date="&Date.";
			else Date = put(intnx('day',input("&Date.",yymmdd8.),-1),yymmddn8.);
			if Date = &Date. then NS = NS + 86400000000000;
			drop diff;
		run;

		proc sort data=TMBOQandM tagsort;
			by descending ns sorti seq;
		run;

		data TMBOQandM;
			set TMBOQandM;
			retain _seq;
			if not missing(seq) then _seq=seq;
			else seq=_seq;
			drop _seq;
		run;

		data TMBOQandM;
			set TMBOQandM;
			retain _orderid;
			if not missing(orderid) then _orderid=orderid;
			else orderid=_orderid;
			drop _orderid;
		run;

		proc sort data=TMBOQandM tagsort;
			by id_main;
		run;

		/*Split file*/

		data TMBOandM;
			set TMBOQandM;
			if key ^= 'Q';
			if key = 'MBO' then num_key = 1;
			else if key ^= 'MBO' then num_key = 0;
		run;

		/*Order Historical Records*/
		proc sql;
			create table mbo as select *, sum(num_key) as keysum from TMBOandM
			group by orderid;
		quit;

		/*keysum=0 means market order, the m that shows only in trading section without mbo orderid correspondence*/
		data mbo;
			set mbo;
			if keysum = 0 then delete;
			drop num_key keysum bitstr sorti;
		run;

		/*Order By Original Index And OrderID*/
		proc sort data = mbo tagsort;
			by orderid id_main;
		run;

		data mbo;
			set mbo;
			retain tradevolume .;
			by orderid;
			lagvolume = lag(volume);
			lagtime = lag(NS);
			if first.orderid then timediff = . ;
			else timediff = NS - lagtime;
			if first.orderid then tradevolume = .;
			if key = "m" then tradevolume = 0;
			if timediff = 0 then tradevolume + lagvolume;
			else if timediff ^= 0 then tradevolume = 0;
			drop lagtime;
		run;

		data mbo;
			set mbo;
			by orderid;
			retain Execute 0;
			difftime2 = NS - lag(NS);
			retain MO 0;
			if key = 'T' then MO = 1;
			else if difftime2 ^= 0 then MO = 0;
			if key = 'm' and timediff ^= 0 then Execute + 1;
			else if timediff ^= 0 then Execute = 0;
		run;

		/*Reformat data, Ovrlay to New with Initialize = 1, New to New (maybe MO if T), Delete to Trade or Delete*/
		/*Change to 1. Delete; 2.Delete New; 3. Execute Delete New;*/

		data mbo;
			set mbo;
			if key = 'MBO';
		run;

		data mbo;
			set mbo;
			by orderid;
			lprice = lag(price);
			lvolume = lag(volume);
			lpriority = lag(priority);
			if first.orderid then Cprice = .;
			else Cprice=round((price-lprice)*100)/100;
			if first.orderid then Cvolume = .;
			else Cvolume=round(volume-lvolume);
			if first.orderid then cpriority = .;
			else if round(priority-lpriority) = 0 then cpriority = 0;
			else cpriority = 1;
		run;

		proc datasets library=work;
		   delete allevent tmboandm tmboqandm;
		run;

		data mbo;
			set mbo;
			retain New_OrderID 0;
			length Naction $18;
			by orderid;
			if first.orderid then New_OrderID = 0;
			/*1.Overlay - Initialize market*/
		    if uaction = "OVRLAY" then do;
				Naction = "NEW"; Nvolume = volume; Nprice = price; Comment = "I"; Case = 1;
				output;
			end;
			/*2.New - New, MO if MO=1*/
			else if uaction = "NEW" then do;
				Naction = "NEW"; Nvolume = volume; Nprice = price; Case = 2;
				output;
			end;
			/*Change - without execution*/
			else if uaction = "CHANGE" and Execute = 0 and MO = 0 then do;
				/*3.Only volume down without execution*/
				if Cprice = 0 and Cvolume < 0 then do;
					Naction = "DELETE"; Case = 3;
					output;
				end;
				/*4.All other cases without execution*/
				else if Cprice ^= 0 or Cvolume > 0 then do;
					Naction = "DELETE"; Nvolume = lvolume; Nprice = lprice; Case = 4; Rank = 1;
					output;
					New_OrderID + 1;
					Naction = "NEW"; Nvolume = volume; Nprice = price; Case = 4; Rank = 2;
					output;
				end;
			end;
			/*Change - be executed by MO, seems lprice = price*/
			else if uaction = "CHANGE" and MO = 0 and Execute > 0 then do;
				/*5.When priority changed means more LO add when execute, so generate new limit order first*/
				if cpriority ^= 0 and cpriority ^= . and Cprice = 0 then do;
					Naction = "DELETE"; Nvolume = lvolume; Nprice = lprice; Case = 5; Rank = 1;
					output;
					New_OrderID + 1;
					Naction = "NEW"; Nvolume = round(volume + tradevolume); Nprice = price; Comment = "A";
					Case = 5; Rank = 2;
					output;
					Naction = "TRADE"; Nvolume = tradevolume; Nprice = lprice; Case = 5; Rank = 3;
					output;
				end;
				/*999.LO May excape from MO when it's executing*/
				else if cpriority ^= 0 and cpriority ^= . and Cprice ^= 0 then do;
					Naction = "TRADE"; Nvolume = tradevolume; Nprice = lprice; Case = 999; Rank = 1;
					output;
					Naction = "DELETE"; Nvolume = round(lvolume - tradevolume); Nprice = lprice; Case = 999; Rank = 2;
					output;
					New_OrderID + 1;
					Naction = "NEW"; Nvolume = volume; Nprice = price; Case = 999; Rank = 3; Comment = "E";
					output;
				end;
				/*6.Partial Trade*/
				else if cpriority = 0 then do;
					Naction = "TRADE"; Nvolume = tradevolume; Nprice = lprice; Case = 6;
					output;
				end;
			end;
			/*7.Change - itself is a MO, i.e. price changed across spread and stayed*/
			else if uaction = "CHANGE" and MO = 1 then do;
				Naction = "DELETE"; Nvolume = lvolume; Nprice = lprice; Case = 7; Rank = 1;
				output;
				New_OrderID + 1;
				Naction = "NEW"; Nvolume = volume; Nprice = price; Case = 7; Rank = 2;
				output;
			end;
			/*8.DELETE - without execution*/
			/*But itself may be a MO, i.e. price changed across spread and all traded*/
			/*And it could be due to market clears*/
			else if uaction = "DELETE" and Execute = 0 then do;
				Naction = "DELETE"; Nvolume = volume; Nprice = price; Case = 8;
				output;
			end;
			/*9.DELETE - by execution*/
			else if uaction = "DELETE" and Execute > 0 then do;
				Naction = "TRADE"; Nvolume = volume; Nprice = price; Case = 9;
				output;
			end;
			/*Else, we missed anomaly patern of data, we should see Case = .*/
			else Case = .;
		run;
		
		data mbo;
			set mbo;
			by orderid;
			lagNew_OrderID = lag(New_OrderID);
			if first.orderid then lagNew_OrderID=0;
		run;

		proc sort data = mbo tagsort;
			by id_main rank;
		run;

		data mbo;
			set mbo;
			diff = NS-lag(NS);
			if diff ^= 0 then call symput("diff",_N_-1);
			%put &diff.;
		run;

		data events (drop = key diff priority id_main tradevolume lagvolume timediff execute difftime2 lprice lvolume 
		lpriority cpriority Price Volume MO);
			set mbo;
			format orderid best13.;
			if Nprice = . then Nprice = Price;
			if Nvolume = . then Nvolume = Volume;
			rename Nprice = Price Nvolume = Volume;
			if _N_ > &diff. and uaction = 'DELETE' then Comment = "C";
			if MO = 1 then Comment = "M";
			if Naction = "TRADE" then do; Cvolume = .; Cprice = .; end; run;
		run;

		proc sort data = events tagsort;
			by seq;
		run;


		/*Read All Book Data*/
		data AllBook (drop=s2-s10 lseq ltime);
		infile depths dsd LRECL=1024 missover DLM=",";
		informat key $8. s2-s10 $32.;
		format Time Time18.9;
		input Key s2 s3 s4 s5  S6 s7 s8  s9 s10;
		retain lseq 0;
		retain ltime 0;
		IF Key="DH" then do;
			Time=input(s2,Time18.9);
			Side=s3;
			LevelChange=input(s4,best30.);
			Seq=input(s5,best30.);
/*			BookType=s6;*/
			end;
		ELSE IF KEY="Q" then do;
			Time=ltime;
			Seq=lseq;
/*			Side1=s2;*/
			BidPrice=input(s3,best30.);
			BidVolume=input(s4,best30.);
			BidQuantity=input(s5,best30.);
			LevelNumber=input(s6,best30.);
/*			Side2=s7;*/
			AskPrice=input(s8,best30.);
			AskVolume=input(s9,best30.);
			AskQuantity=input(s10,best30.);
			end;
/*		else ERROR =2;*/
/*		lseq=seq;*/
		ltime=time;
		run;

		data alltimebooklevel;
			set AllBook;
			if key="DH" or key="Q";
			if bidprice>1000000000 then bidprice=.;
			if askprice>1000000000 then askprice=.;
		run;

		proc datasets library=work;
		   delete AllBook;
		run;

		data alltimebooklevel;
			set alltimebooklevel;
			retain dhgroup 0;
			if key="DH" then dhgroup+1;
			else dhgroup+0;
		run;

		proc sort data=alltimebooklevel tagsort;
			by dhgroup;
		run;
	
		data level0 (keep = bid1 bidv1 bidq1 ask1 askv1 askq1 dhgroup);
			set alltimebooklevel;
			if LevelNumber = 0;
			rename bidprice = bid1 bidvolume = bidv1 bidquantity = bidq1 askprice = ask1 askvolume = askv1 askquantity = askq1;
		run;

		data level1 (keep = bid2 bidv2 bidq2 ask2 askv2 askq2 dhgroup);
			set alltimebooklevel;
			if LevelNumber = 1;
			rename bidprice = bid2 bidvolume = bidv2 bidquantity = bidq2 askprice = ask2 askvolume = askv2 askquantity = askq2;
		run;

		data level2 (keep = bid3 bidv3 bidq3 ask3 askv3 askq3 dhgroup);
			set alltimebooklevel;
			if LevelNumber = 2;
			rename bidprice = bid3 bidvolume = bidv3 bidquantity = bidq3 askprice = ask3 askvolume = askv3 askquantity = askq3;
		run;

		data level3 (keep = bid4 bidv4 bidq4 ask4 askv4 askq4 dhgroup);
			set alltimebooklevel;
			if LevelNumber = 3;
			rename bidprice = bid4 bidvolume = bidv4 bidquantity = bidq4 askprice = ask4 askvolume = askv4 askquantity = askq4;
		run;

		data level4 (keep = bid5 bidv5 bidq5 ask5 askv5 askq5 dhgroup);
			set alltimebooklevel;
			if LevelNumber = 4;
			rename bidprice = bid5 bidvolume = bidv5 bidquantity = bidq5 askprice = ask5 askvolume = askv5 askquantity = askq5;
		run;

		data level5 (keep = bid6 bidv6 bidq6 ask6 askv6 askq6 dhgroup);
			set alltimebooklevel;
			if LevelNumber = 5;
			rename bidprice = bid6 bidvolume = bidv6 bidquantity = bidq6 askprice = ask6 askvolume = askv6 askquantity = askq6;
		run;

		data level6 (keep = bid7 bidv7 bidq7 ask7 askv7 askq7 dhgroup);
			set alltimebooklevel;
			if LevelNumber = 6;
			rename bidprice = bid7 bidvolume = bidv7 bidquantity = bidq7 askprice = ask7 askvolume = askv7 askquantity = askq7;
		run;

		data level7 (keep = bid8 bidv8 bidq8 ask8 askv8 askq8 dhgroup);
			set alltimebooklevel;
			if LevelNumber = 7;
			rename bidprice = bid8 bidvolume = bidv8 bidquantity = bidq8 askprice = ask8 askvolume = askv8 askquantity = askq8;
		run;

		data level8 (keep = bid9 bidv9 bidq9 ask9 askv9 askq9 dhgroup);
			set alltimebooklevel;
			if LevelNumber = 8;
			rename bidprice = bid9 bidvolume = bidv9 bidquantity = bidq9 askprice = ask9 askvolume = askv9 askquantity = askq9;
		run;

		data level9 (keep = bid10 bidv10 bidq10 ask10 askv10 askq10 dhgroup);
			set alltimebooklevel;
			if LevelNumber = 9;
			rename bidprice = bid10 bidvolume = bidv10 bidquantity = bidq10 askprice = ask10 askvolume = askv10 askquantity = askq10;
		run;

		data dh (keep = dhgroup seq time);
			set alltimebooklevel;
			if key = "DH";
		run;

		data book_final;
			merge dh level0 level1 level2 level3 level4 level5 level6 level7 level8 level9;
			by dhgroup;
			drop dhgroup time;
		run;

		proc sort data = book_final tagsort;
			by seq;
		run;

		/*Matching The All Event Data With Only Limit Order And Quote Info*/

		data final;
			merge events book_final;
			if NS = . then delete;
			by seq;
			drop seq;
		run;

		data final;
			set final;
			New_OrderID = round(New_OrderID);
			lagNew_OrderID = round(lagNew_OrderID);
			if lagNew_OrderID = . then lagNew_OrderID = '000000';
			char_id = STRIP(PUT(New_OrderID, z6.));
			char_id2 = STRIP(PUT(lagNew_OrderID, z6.));
			char_id3 = STRIP(PUT(OrderID, z13.));
			format char_id char_id2 $6. char_id3 $13.;
			drop New_OrderID lagNew_OrderID;
			rename char_id=New_OrderID char_id2=lagNew_OrderID char_id3 = OrderID OrderID=OriginalID;
		run;

		proc datasets library=work;
			delete alltimebooklevel dh level0 level1 level2 level3 level4 level5 level6 level7 level8 level9
			mbo events book_final;
		run;

		data final;
			set final;
			format ID1 ID2 $20.;
			ID1 = catt(OrderID,lagNew_OrderID);
			if New_OrderID ^= '000000' and New_OrderID ^= lagNew_OrderID then ID2 = catt(OrderID,New_OrderID);
			else ID2 = .;
			drop UAction OrderID New_OrderID lagNew_OrderID;
			rename ID1=OrderID ID2=New_OrderID;
		run;

		proc sort data = final tagsort;
			by ns OriginalID rank;
		run;

		data cmehft.&ticker.&Date.;
			retain Date Time NS OriginalID OrderID New_OrderID Naction Sidenew Volume Cvolume Price Cprice ask1-ask10
			askq1-askq10 askv1-askv10 bid1-bid10 bidq1-bidq10 bidv1-bidv10 Case Rank MO Comment;
			set final;
			informat Sidenew $4.;
			format Sidenew $4. OriginalID best13.;
			if Side = 0 then Sidenew = 'B';
			else Sidenew = 'S';
			if NS = . then delete;
			drop Side;
			rename Sidenew = Side Time = Mtime Naction = Xcomment OrderID = Token New_OrderID = Token_New;
		run;

		/*Delete Raw Data if needed*/
		data _null_;
			rc1=fdelete('events');
			rc2=fdelete('depths');
			put rc1= rc2=;
		run;


		/* Data Processing for Cancle Clusters, follows Dr. Cooper's code*/

		/* First Step is to remove ! items from consderation; They were not part of the original itch feed
		   Also, remove observations at 9:00:00.000000000 or before or after 16:00:00.000000000    */
		/* Also, there are duplicate rows, which we need to remove, these were put in by the data provider because of an coding error. It has been verified that they 
		   do not affect data integrity */

		data dset (drop=lns ltoken ltoken_new);
		  	set cmehft.&ticker.&date.;
			retain obnum 0;
			Lns=lag1(ns);
			Ltoken=lag1(token);
			Ltoken_new=lag1(token_new);
		    if ns ne lns or
				token ne ltoken or
				token_new ne ltoken_new;
			obnum +1;
		run;

		data dset;
			set dset;
			spread=ask1-bid1;
			spread_scaled=(2*spread)/(ask1+bid1);
			vol1=bidv1+askv1;
			quantity1=bidq1+askq1;
			if vol1 ^= 0 then IB=(bidv1-askv1)/vol1;
			else IB=.;
			ns2 = mtime*1000000000;
			groups=ns2/1000000000;
			format groups timefmt1m.;
			groups2=ns2/1000000000;
			format groups2 timefmt30m.;
			drop ns2;
		run;

		data dset (drop = groups groups2);
			set dset;
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

		data tokset (keep=token ns xcomment case comment);
		     set dset;
		run;

		data tokset_new (keep=token ns xcomment case comment);
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
			rename ns=begns xcomment=begaction case=begcase comment=begcomment;
		run;

		data tokset_end;
			set tokset;
			by token;
			if last.token;
			rename ns=endns xcomment=endaction case=endcase comment=endcomment;
		run;

		data tokset;
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

		data tokset_&ticker.&date.;
			set tokset;
			date = "&Date.";
		run;

		proc append base=cmehft.summaryorderid_&ticker. data=tokset_&ticker.&date. force; run; quit;

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
				clrswtchb=mtime; 
				obtrkb=obnum; 
			end;
			/*** if it is a dblclear then we have some business to take care of ************/
			/**** rack up results and reset the counters                        ************/
		 	if dblclrb eq 1 then do;
				reacttimeb=mtime-clrswtchb; /* the last event time l-ess the time of the clear before that, reactiontime */
				obclrb=obtrkb; /* the observation number of the clear */
				if clearbc eq 1 then afterclrb=1; /* flag that tells us the clear followed by another cancel clear */
				else if clearbe eq 1 then afterexeb=1; /* flag that tells us the clear is followed by an execute, it happened to clear also*/
				clrswtchb=mtime; /* now we update the clrswtch variable */
				obtrkb=obnum; /* now we update the observation the clear occurred variable */
			end;	

			/* Now we are into the after a clear event logic */
			/* If we are in the period of dealing with an after clear event */
			if clrswtchb gt 0 and obnum gt obtrkb then do;
		       	/* First we make sure that not too much time has passed since the clear */
			   	if mtime-clrswtchb ge &timetonothing. then do; /*if nothing has happened for awhile*/
					afternormb=1; /*record statistics reset switch to zero*/
					clrswtchb=0;												
					obclrb=obtrkb; /*this is the observation the after came from */
			    end;					
				/*if it is a level fill, add record statistics */
				else if bid1 gt lastbidp then do;										
					if rtime ne . then afteraddb=rtime; 						
					else afteraddb=rtime_new;
					reacttimeb=mtime-clrswtchb;
					obclrb=obtrkb; 										
					clrswtchb=0;
					end;
				/* it it is a gap close from other side, record statistics */
				else if  ask1 lt lastaskp then do;								
					if rtime ne . then afterclsb=rtime; /* reset switch to zero */
					else afterclsb=rtime_new;
					reacttimeb=mtime-clrswtchb;
					obclrb=obtrkb;
					clrswtchb=0;
				end;
				/*if it is an execute that did not result in a double clear, record statistics*/
			    else if side eq "B" and xcomment="TRADE" then do;   
					afterexeb=1; /*same report as an execute that did clear the level*/								 
					reacttimeb=mtime-clrswtchb;
					obclrb=obtrkb; 
					clrswtchb=0;
			    end;
			end;
			/******** repeat for ask *****/
			if cleara eq 1 and clrswtcha gt 0 then dblclra=1;
			else if cleara eq 1 then do; 
				clrswtcha=mtime; 
				obtrka=obnum; 
			end;
		 	if dblclra eq 1 then do;
				reacttimea=mtime-clrswtcha;
				obclra=obtrka;
				if clearac eq 1 then afterclra=1;
				else if clearae eq 1 then afterexea=1;
				clrswtcha=mtime;
				obtrka=obnum;
			end;
			if clrswtcha gt 0 and obnum gt obtrka then do;
			   if mtime-clrswtcha ge &timetonothing. then do;                   
					afternorma=1;
					obclra=obtrka; 
					clrswtcha=0;												
			    end;									
				else if ask1 lt lastaskp then do;					 			
					if rtime ne . then afteradda=rtime; 						
					else afteradda=rtime_new;
					reacttimea=mtime-clrswtcha;
					obclra=obtrka;
					clrswtcha=0;
				end;
				else if bid1 gt lastbidp then do;								
				 	if rtime ne . then afterclsa=rtime;							
				 	else afterclsa=rtime_new;
					reacttimea=mtime-clrswtcha;
					obclra=obtrka;
					clrswtcha=0;
				end;
			    else if side eq "S" and xcomment="TRADE" then do;   
				 	afterexea=1;																			 
				 	reacttimea=mtime-clrswtcha;
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

/*Add variables for regressions*/
		data dset;
			set dset;
			midquote = (ask1+bid1)/2;
			vwmidquote = (ask1*askv1+bid1*bidv1)/(askv1+bidv1);
			/*Trade Volume*/
			if xcomment = 'TRADE' and Side = "S" then TVA = Volume;
			else if xcomment = 'TRADE' and Side = "B" then TVB = Volume;
			/*Delete Volume*/
			else if xcomment = 'DELETE' and (Cvolume = 0 and Cprice = 0) and Side = "S" then DVA = Volume;
			else if xcomment = 'DELETE' and (Cvolume = 0 and Cprice = 0) and Side = "B" then DVB = Volume;
			/*Volume Decrese Only Volume*/
			else if xcomment = 'DELETE' and (Cvolume < 0 and Cprice = 0) and Side = "S" then VDVA = Volume;
			else if xcomment = 'DELETE' and (Cvolume < 0 and Cprice = 0) and Side = "B" then VDVB = Volume;
			/*Other Changes (Queue Changes) Volume*/
			else if xcomment = 'DELETE' and (Cvolume > 0 or Cprice ^= 0) and Side = "S" then QCVA = Volume;
			else if xcomment = 'DELETE' and (Cvolume > 0 or Cprice ^= 0) and Side = "B" then QCVB = Volume;
			/*New Volume*/
			else if xcomment = 'NEW' and Side = "S" then NVA = Volume;
			else if xcomment = 'NEW' and Side = "B" then NVB = Volume;
		run;

		proc sort data = dset tagsort; by date groups1;run;

		data reg1;
			set dset;
			symbol = "&KeyVar.";
			dtm = &dtm.;
			workday = &workday.;
			by date groups1;
			if first.groups1 then TradeA=0;
				TradeA+TVA;
			if first.groups1 then DeleteA=0;
				DeleteA+DVA;
			if first.groups1 then VolumeDecreseA=0;
				VolumeDecreseA+VDVA;
			if first.groups1 then QueueChangesA=0;
				QueueChangesA+QCVA;
			if first.groups1 then NewA=0;
				NewA+NVA;
			if first.groups1 then TradeB=0;
				TradeB+TVB;
			if first.groups1 then DeleteB=0;
				DeleteB+DVB;
			if first.groups1 then VolumeDecreseB=0;
				VolumeDecreseB+VDVB;
			if first.groups1 then QueueChangesB=0;
				QueueChangesB+QCVB;
			if first.groups1 then NewB=0;
				NewB+NVB;
			if last.groups1;
				CancelVolA = DeleteA+VolumeDecreseA+QueueChangesA;
				CancelVolB = DeleteB+VolumeDecreseB+QueueChangesB;
			keep symbol dtm workday mtime date groups1 groups30
			TradeA DeleteA VolumeDecreseA QueueChangesA NewA CancelVolA
			TradeB DeleteB VolumeDecreseB QueueChangesB NewB CancelVolB
			;
		run;

		proc means data=dset noprint nway;
			class date groups1;
			var midquote vwmidquote askv1 bidv1 spread spread_scaled IB;
			output out=reg2 mean=Avemidquote AveVWmidquote AvedepthA AvedepthB AveBAS AveSBAS AveIB;
		run;

		data reg_&ticker.&date.;
			merge reg1 reg2;
			by date groups1;
			drop _TYPE_ _FREQ_;
			AveBAS = AveBAS / &tick.;
		run;

		proc append base=cmehft.reg_all_&ticker. data=reg_&ticker.&date. force; run; quit;
/*		proc append base=cmehft.pairedttest_all data=pairedttest_&ticker.&date. force; run; quit;*/

		proc sql;
			create table dset as select
			/*mb_inf is the 1-min average of EWMA of bid side, vice versa for ma_inf*/
			avg(b_cancel) as mb_inf, avg(a_cancel) as ma_inf, 
			* from dset group by groups1;
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
		    set dset;
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
		proc sort data=cluster_set;by obnum;run;
		proc sort data=cluster_set tagsort; by tableside quad; run;

		proc means data=cluster_set noprint;
  			var elapsedtime clears clearcs cleares mlastclr execute;
		  	output out=tables1ab_&ticker.&date.
		    N=count
			mean=avetime avecl aveclcs avecles lclrtime aveexe;
		  	by tableside quad;
		run;

		data tables.tables1ab_&ticker.&date.;
			format tk $8.;
			set tables1ab_&ticker.&date.;
			dt=&date.;
			tk="&ticker.";
			pct_day=(count*avetime)/24/3600;
			if tableside="" then delete;
			if tableside="A" then pct_exe=aveexe/&executea.;
			if tableside="B" then pct_exe=aveexe/&executeb.;
			marketopen = &workday.;
			dtm = &dtm.;
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

		data tables.tables1ab_time_&ticker.&date.;
			format tk $8.;
			set tables1ab_time_&ticker.&date;
			dt=&date.;
			tk="&ticker.";
			pct_day=(count*avetime)/24/3600;
			if tableside="" then delete;
			if tableside="A" then pct_exe=aveexe/&executea.;
			if tableside="B" then pct_exe=aveexe/&executeb.;
			marketopen = &workday.;
			dtm = &dtm.;
			rank = &rank.;
		run;

/*		proc append base=tables1ab_time_all data=tables1ab_time_&ticker.&date. force; run; quit;*/

		/*  make a data set of what happens after the clear -- by cluster where the clear occured */

		data after_set (keep=tableside Quad result afteradd afterexe aftercls afterclr afternorm reacttime tbucket);
		    set dset;
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
		    set dset;
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

		Data tables.after_stats_&ticker.&date.(drop=_TYPE_ result afteradd aftercls afterexe afternorm afterclr);
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
			dtm = &dtm.;
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

		Data tables.after_stats_time_&ticker.&date.(drop=_TYPE_ result afteradd aftercls afterexe afternorm afterclr);
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
			marketopen = &workday.;
			dtm = &dtm.;
			rank = &rank.;
		run;
/*		proc append base=after_stats_time_all data=after_stats_time_&ticker.&date. force; run; quit;*/

/*		dm log 'clear';*/
	%END;

	%END;
%MEND	create_agg_tables;

%create_agg_tables;






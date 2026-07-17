/* autoexec: cap input rows for the captured run, and stand up the mock
   sdtm.dm dataset that ADSL.sas reads from in place of the Domino-managed
   sdtm libname (see domino.sas for the real libname resolution). */
options obs=100;

libname adam "%sysfunc(pathname(work))";

/* Mock SDTM.DM (Demographics) -- shape matches what ADSL.sas expects:
   one record per subject, carrying USUBJID plus the ADSL-relevant
   demographic/treatment fields used downstream by the TFL programs. */
data work.dm;
  length studyid $8 usubjid $20 subjid $8 actarm $30 sex $1 race $30 country $3;
  infile datalines dlm='|';
  input studyid $ usubjid $ subjid $ actarm $ age sex $ race $ country $ dmdtc :yymmdd10.;
  format dmdtc yymmdd10.;
  datalines;
CDISC01|CDISC01-701-1015|1015|Placebo|63|F|WHITE|USA|2023-01-05
CDISC01|CDISC01-701-1023|1023|Xanomeline Low Dose|71|M|WHITE|USA|2023-01-06
CDISC01|CDISC01-701-1028|1028|Xanomeline High Dose|68|F|BLACK|USA|2023-01-07
CDISC01|CDISC01-701-1033|1033|Placebo|55|M|WHITE|USA|2023-01-08
CDISC01|CDISC01-701-1040|1040|Xanomeline Low Dose|74|F|ASIAN|USA|2023-01-09
CDISC01|CDISC01-701-1047|1047|Xanomeline High Dose|60|M|WHITE|USA|2023-01-10
CDISC01|CDISC01-701-1052|1052|Placebo|66|F|WHITE|USA|2023-01-11
CDISC01|CDISC01-701-1058|1058|Xanomeline Low Dose|58|M|BLACK|USA|2023-01-12
CDISC01|CDISC01-701-1064|1064|Xanomeline High Dose|77|F|WHITE|USA|2023-01-13
CDISC01|CDISC01-701-1071|1071|Placebo|62|M|ASIAN|USA|2023-01-14
;
run;

/* ADSL.sas resolves the pre-flow branch via sdtm.dm, so alias sdtm -> work
   where the mock dm dataset lives. */
libname sdtm "%sysfunc(pathname(work))";

/* Force the pre-flow (interactive/batch) branch in ADSL.sas's
   run_domino_code macro -- we're not inside a Domino Flow. */
%let DOMINO_IS_WORKFLOW_JOB=false;

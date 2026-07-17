/* autoexec: cap input rows for the captured run, and stand up the mock
   adam.adsl / sdtm.vs datasets that ADVS.sas merges (see domino.sas for
   the real libname resolution on Domino). */
options obs=100;

libname adam "%sysfunc(pathname(work))";
libname sdtm "%sysfunc(pathname(work))";

/* Mock ADaM.ADSL -- subject-level, one row per USUBJID with treatment arm. */
data adam.adsl;
  length usubjid $20 actarm $30;
  infile datalines dlm='|';
  input usubjid $ actarm $;
  datalines;
CDISC01-701-1015|Placebo
CDISC01-701-1023|Xanomeline Low Dose
CDISC01-701-1028|Xanomeline High Dose
CDISC01-701-1033|Placebo
CDISC01-701-1040|Xanomeline Low Dose
;
run;

/* Mock SDTM.VS (Vital Signs) -- multiple visits/params per subject, the
   shape t_vscat.sas later reads via adam.advs (SYSBP/DIABP/PULSE). */
data sdtm.vs;
  length usubjid $20 vstestcd $8 vstest $40;
  infile datalines dlm='|';
  input usubjid $ visitnum vstestcd $ vstest $ vsstresn;
  datalines;
CDISC01-701-1015|3|SYSBP|Systolic Blood Pressure|118
CDISC01-701-1015|3|DIABP|Diastolic Blood Pressure|76
CDISC01-701-1015|3|PULSE|Pulse Rate|68
CDISC01-701-1023|4|SYSBP|Systolic Blood Pressure|142
CDISC01-701-1023|4|DIABP|Diastolic Blood Pressure|92
CDISC01-701-1023|4|PULSE|Pulse Rate|58
CDISC01-701-1028|12|SYSBP|Systolic Blood Pressure|85
CDISC01-701-1028|12|DIABP|Diastolic Blood Pressure|55
CDISC01-701-1028|12|PULSE|Pulse Rate|105
CDISC01-701-1033|3|SYSBP|Systolic Blood Pressure|121
CDISC01-701-1033|3|DIABP|Diastolic Blood Pressure|79
CDISC01-701-1033|3|PULSE|Pulse Rate|72
CDISC01-701-1040|4|SYSBP|Systolic Blood Pressure|130
CDISC01-701-1040|4|DIABP|Diastolic Blood Pressure|84
CDISC01-701-1040|4|PULSE|Pulse Rate|64
;
run;

%let DOMINO_IS_WORKFLOW_JOB=false;

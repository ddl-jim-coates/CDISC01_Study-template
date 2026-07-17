/* autoexec: cap input rows for the captured run, and stand up the mock
   adam.advs / metadata.t_vscat datasets that t_vscat.sas reads (see
   domino.sas and share/macros/tfl_metadata.sas for the real setup on
   Domino). */
options obs=100;

libname adam "%sysfunc(pathname(work))";
libname metadata "%sysfunc(pathname(work))";

%let __prog_name = t_vscat;

/* Mock ADaM.ADVS -- vital signs, post-baseline (visitnum > 2) so
   t_vscat.sas's post_base filter picks these rows up, spanning normal
   and out-of-range SYSBP/DIABP/PULSE readings across all 3 arms. */
data adam.advs;
  length usubjid $20 actarm $30 vstestcd $8 vstest $40;
  infile datalines dlm='|';
  input usubjid $ actarm $ visitnum vstestcd $ vstest $ vsstresn;
  datalines;
CDISC01-701-1015|Placebo|3|SYSBP|Systolic Blood Pressure|118
CDISC01-701-1015|Placebo|3|DIABP|Diastolic Blood Pressure|76
CDISC01-701-1015|Placebo|3|PULSE|Pulse Rate|68
CDISC01-701-1023|Xanomeline Low Dose|4|SYSBP|Systolic Blood Pressure|145
CDISC01-701-1023|Xanomeline Low Dose|4|DIABP|Diastolic Blood Pressure|94
CDISC01-701-1023|Xanomeline Low Dose|4|PULSE|Pulse Rate|58
CDISC01-701-1028|Xanomeline High Dose|12|SYSBP|Systolic Blood Pressure|85
CDISC01-701-1028|Xanomeline High Dose|12|DIABP|Diastolic Blood Pressure|55
CDISC01-701-1028|Xanomeline High Dose|12|PULSE|Pulse Rate|108
CDISC01-701-1033|Placebo|3|SYSBP|Systolic Blood Pressure|121
CDISC01-701-1033|Placebo|3|DIABP|Diastolic Blood Pressure|79
CDISC01-701-1033|Placebo|3|PULSE|Pulse Rate|72
CDISC01-701-1040|Xanomeline Low Dose|4|SYSBP|Systolic Blood Pressure|130
CDISC01-701-1040|Xanomeline Low Dose|4|DIABP|Diastolic Blood Pressure|84
CDISC01-701-1040|Xanomeline Low Dose|4|PULSE|Pulse Rate|64
;
run;

/* Mock metadata.t_vscat -- tfl_metadata.sas turns every column of this
   one-row dataset into a macro variable via CALL SYMPUT. */
data metadata.t_vscat;
  length DisplayName $40 DisplayTitle $60 Title1 $60 Footer1 $80 Footer2 $80 Footer3 $80;
  DisplayName  = "CDISC01";
  DisplayTitle = "Table 14.4.1";
  Title1       = "Categorical Summary of Vital Signs";
  Footer1      = "SBP: Systolic Blood Pressure. DBP: Diastolic Blood Pressure.";
  Footer2      = "Criteria: SBP <90/>140 mmHg, DBP <60/>90 mmHg, HR <60/>100 beats/min.";
  Footer3      = "Generated via a Jenner compatibility bundle.";
run;

%let DOMINO_IS_WORKFLOW_JOB=false;

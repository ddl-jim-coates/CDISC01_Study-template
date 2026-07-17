/* autoexec: cap input rows for the captured run, and stand up the mock
   adam.adsl + metadata.t_pop datasets that t_pop.sas reads (see domino.sas
   and share/macros/tfl_metadata.sas for the real setup on Domino). */
options obs=100;

libname adam "%sysfunc(pathname(work))";
libname metadata "%sysfunc(pathname(work))";

/* __prog_name is normally derived by domino.sas from the running program's
   filename; t_pop.sas's %tfl_metadata call reads metadata.&__prog_name. */
%let __prog_name = t_pop;

/* Mock ADaM.ADSL -- subject-level, with the age/sex/treatment fields
   t_pop.sas buckets into age groups and treatment arms. */
data adam.adsl;
  length usubjid $20 actarm $30 sex $1;
  infile datalines dlm='|';
  input usubjid $ actarm $ age sex $;
  datalines;
CDISC01-701-1015|Placebo|58|F
CDISC01-701-1023|Xanomeline Low Dose|71|M
CDISC01-701-1028|Xanomeline High Dose|68|F
CDISC01-701-1033|Placebo|63|M
CDISC01-701-1040|Xanomeline Low Dose|77|F
CDISC01-701-1047|Xanomeline High Dose|59|M
CDISC01-701-1052|Placebo|82|F
CDISC01-701-1058|Xanomeline Low Dose|66|M
CDISC01-701-1064|Xanomeline High Dose|73|F
CDISC01-701-1071|Placebo|61|M
;
run;

/* Mock metadata.t_pop -- tfl_metadata.sas turns every column of this
   one-row dataset into a macro variable via CALL SYMPUT (numeric and
   character arrays), which t_pop.sas then uses for titles/footnotes. */
data metadata.t_pop;
  length DisplayName $40 DisplayTitle $60 Title1 $60 Footer1 $80 Footer2 $80 Footer3 $80;
  DisplayName  = "CDISC01";
  DisplayTitle = "Table 14.1.1";
  Title1       = "Summary of Population";
  Footer1      = "Xanomeline: study drug.";
  Footer2      = "Age groups per protocol-specified bands.";
  Footer3      = "Generated via a Jenner compatibility bundle.";
run;

%let DOMINO_IS_WORKFLOW_JOB=false;

/* autoexec: cap input rows for the captured run, and stand up the mock
   adam.adsl / sdtm.ae / sdtm.ex datasets that ADAE.sas merges (see
   domino.sas for the real libname resolution on Domino). */
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

/* Mock SDTM.AE (Adverse Events) -- AESTDY drives ADAE.sas's visitnum
   bucketing (1-12 => 3, 13-161 => 4, 162+ => 12). */
data sdtm.ae;
  length usubjid $20 aeterm $60 aedecod $60 aesoc $60 aerel $10;
  infile datalines dlm='|';
  input usubjid $ aeterm $ aedecod $ aesoc $ aerel $ aestdy;
  datalines;
CDISC01-701-1015|HEADACHE|Headache|Nervous system disorders|PROBABLE|5
CDISC01-701-1023|NAUSEA|Nausea|Gastrointestinal disorders|POSSIBLE|20
CDISC01-701-1028|DIZZINESS|Dizziness|Nervous system disorders|NONE|170
CDISC01-701-1033|RASH|Rash|Skin disorders|DEFINITE|8
CDISC01-701-1040|FATIGUE|Fatigue|General disorders|NONE|45
;
run;

/* Mock SDTM.EX (Exposure) -- ADAE.sas's second merge joins on
   USUBJID + VISITNUM after the AE bucketing step, so this needs a row
   per subject/visitnum combination that appears in AE after bucketing. */
data sdtm.ex;
  length usubjid $20 extrt $20;
  infile datalines dlm='|';
  input usubjid $ visitnum extrt $ exdose;
  datalines;
CDISC01-701-1015|3|Xanomeline|0
CDISC01-701-1023|4|Xanomeline|54
CDISC01-701-1028|12|Xanomeline|81
CDISC01-701-1033|3|Xanomeline|0
CDISC01-701-1040|4|Xanomeline|54
;
run;

%let DOMINO_IS_WORKFLOW_JOB=false;

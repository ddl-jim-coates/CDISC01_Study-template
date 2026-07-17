/* autoexec: cap input rows for the captured run, and stand up the mock
   adam.adae / adam.adsl / metadata.t_ae_rel datasets that t_ae_rel.sas
   reads (see domino.sas and share/macros/tfl_metadata.sas for the real
   setup on Domino). */
options obs=100;

libname adam "%sysfunc(pathname(work))";
libname metadata "%sysfunc(pathname(work))";

%let __prog_name = t_ae_rel;

/* Mock ADaM.ADAE -- one row per AE, spanning all 3 treatment arms and
   both relatedness categories (POSSIBLE/PROBABLE/DEFINITE => Related). */
data adam.adae;
  length usubjid $20 actarm $30 aerel $10 aesoc $60 aedecod $60;
  infile datalines dlm='|';
  input usubjid $ actarm $ aerel $ aesoc $ aedecod $;
  datalines;
CDISC01-701-1015|Placebo|PROBABLE|Nervous system disorders|Headache
CDISC01-701-1023|Xanomeline Low Dose|POSSIBLE|Gastrointestinal disorders|Nausea
CDISC01-701-1028|Xanomeline High Dose|NONE|Nervous system disorders|Dizziness
CDISC01-701-1033|Placebo|DEFINITE|Skin disorders|Rash
CDISC01-701-1040|Xanomeline Low Dose|NONE|General disorders|Fatigue
CDISC01-701-1047|Xanomeline High Dose|PROBABLE|Nervous system disorders|Headache
;
run;

/* Mock ADaM.ADSL -- subject-level, treated subjects only. */
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
CDISC01-701-1047|Xanomeline High Dose
;
run;

/* Mock metadata.t_ae_rel -- tfl_metadata.sas turns every column of this
   one-row dataset into a macro variable via CALL SYMPUT. */
data metadata.t_ae_rel;
  length DisplayName $40 DisplayTitle $60 Title1 $60
         Footer1 $80 Footer2 $80 Footer3 $80 Footer4 $80 Footer5 $80;
  DisplayName  = "CDISC01";
  DisplayTitle = "Table 14.3.1";
  Title1       = "Treatment Emergent Adverse Events by Relationship";
  Footer1      = "TEAE: Treatment Emergent Adverse Event.";
  Footer2      = "SOC/PT per MedDRA coding.";
  Footer3      = "Related includes POSSIBLE, PROBABLE, DEFINITE.";
  Footer4      = "Percentages based on treated subjects (N).";
  Footer5      = "Generated via a Jenner compatibility bundle.";
run;

%let DOMINO_IS_WORKFLOW_JOB=false;

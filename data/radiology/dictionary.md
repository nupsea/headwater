# Radiology Workflow Dataset — Data Dictionary

Source: Synthetic simulation dataset (CC BY 4.0, Mendeley DOI 10.17632/jsg8c4yhxy.1).
See ATTRIBUTION.md for full provenance.

## cases.csv

| Column | Description |
|---|---|
| case_id | Unique identifier for each patient imaging case |
| arrival_time | Timestamp when the patient arrived at the imaging department |
| end_time | Timestamp when the case was completed and the patient left |
| patient_type | Admission route code for the patient |
| exam_types | List of imaging modality types performed for the case |
| exam_count | Number of imaging exams performed for this case |
| total_wait_time | Cumulative waiting time before imaging started (HH:MM format) |
| total_scan_time | Cumulative time spent in the scanner across all exams (HH:MM format) |
| throughput_time | Total elapsed time from arrival to discharge |
| week_day_of_arrival | Day of week when the patient arrived |
| hour_day_of_arrival | Hour of day (0–23) when the patient arrived |

## exams.csv

| Column | Description |
|---|---|
| case_id | Foreign key linking to cases.csv |
| exam_id | Unique identifier for each imaging exam |
| modality | Imaging technology used for the exam |
| scan_time_duration | Duration of the scan in minutes |
| wait_before_exam | Waiting time before this specific exam began |

## events.csv

| Column | Description |
|---|---|
| case_id | Foreign key linking to cases.csv |
| exam_id | Foreign key linking to exams.csv |
| scenario_id | Simulation scenario identifier |
| activity | Workflow activity or step name |
| timestamp_start | Start time of the activity |
| timestamp_end | End time of the activity |
| total_duration | Duration of the activity |
| modality | Imaging modality associated with the activity |
| patient_type | Admission route code (same domain as cases.patient_type) |

## Patient Type Codes

| Code | Meaning |
|---|---|
| H | Hospitalized inpatient routed from a ward |
| S | Scheduled outpatient with a prior appointment |
| A | Ambulance or emergency walk-in |
| D | Direct referral from a GP or external clinic |

## Imaging Modality Codes

| Code | Meaning |
|---|---|
| X-ray | Conventional radiography |
| CT | Computed tomography |
| MRI | Magnetic resonance imaging |
| US | Ultrasound |
| Mammo | Mammography |
| CBCT | Cone-beam computed tomography |
| Other | Other or unspecified modality |

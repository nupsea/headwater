# Data Dictionary: Inspection Source

## Column Definitions

| Column | Description |
|---|---|
| inspection_score | Numeric score assigned by the inspector, ranging 0 to 100 |
| violation_count | Total number of code violations found during the inspection |
| inspection_type | Category of inspection conducted |
| facility_id | Unique identifier for the inspected facility |

## Inspection Type Codes

| Code | Meaning |
|---|---|
| R | Routine scheduled inspection |
| C | Complaint-driven inspection |
| F | Follow-up re-inspection |

## Additional Notes

- **risk_level**: Overall risk classification derived from score and violation_count
- `inspector_id`: Identifier for the inspector who conducted the visit

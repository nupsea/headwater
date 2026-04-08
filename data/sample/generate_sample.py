#!/usr/bin/env python3
"""
Synthetic Data Generator for ADM Demo
Domain: Community Environmental Health - City of Riverton

Generates realistic NDJSON files for a fictional mid-size city's environmental
health department. Data covers air/water quality monitoring, public health
incidents, facility inspections, community complaints, and intervention programs.

Designed to surface real-world data challenges:
- Sensor gaps and calibration drift
- Referential integrity failures (orphaned complaints)
- Schema heterogeneity (JSON attributes, varying inspection checklists)
- Seasonal/diurnal patterns in environmental data
- Geographic correlation between pollution and health outcomes
- PII in free-text fields (tests PII detection)
- Mixed temporal granularity (per-minute readings vs daily incidents)

No external dependencies beyond Python 3.10+ standard library.
Output: NDJSON files in the same directory as this script.
License: CC0 (public domain)
"""

import json
import math
import os
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

# ---------------------------------------------------------------------------
# Deterministic seed for reproducibility
# ---------------------------------------------------------------------------
SEED = 42
random.seed(SEED)

OUTPUT_DIR = Path(__file__).parent

# ---------------------------------------------------------------------------
# Time range: 2024-01-01 to 2024-12-31
# ---------------------------------------------------------------------------
YEAR = 2024
DATE_START = datetime(YEAR, 1, 1)
DATE_END = datetime(YEAR, 12, 31, 23, 59, 59)

def rand_date(start: datetime = DATE_START, end: datetime = DATE_END) -> datetime:
    delta = end - start
    seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=seconds)

def iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S")

def date_only(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")

def uid() -> str:
    return uuid.uuid4().hex[:12]

# ---------------------------------------------------------------------------
# City of Riverton -- Geography & Zones
# ---------------------------------------------------------------------------
# Fictional mid-size city (~180K population) in a river valley.
# Based loosely on real US cities like Chattanooga, TN / Boise, ID / Richmond, VA.
# 25 zones with distinct characteristics affecting environmental & health data.

CITY_NAME = "Riverton"
STATE = "Columbia"  # fictional state
BASE_LAT = 35.42   # approximate latitude (southern mid-US climate)
BASE_LON = -86.78

ZONES = [
    # (zone_id, name, type, pop, median_income, pct_under18, pct_over65, lat_offset, lon_offset, risk_profile)
    ("Z01", "Downtown Core",         "urban_commercial", 8200, 52000, 0.12, 0.08, 0.000, 0.000, "moderate"),
    ("Z02", "Riverside District",    "mixed_use",        6800, 61000, 0.18, 0.11, 0.008, -0.012, "low"),
    ("Z03", "Old Mill Quarter",      "industrial",       4100, 38000, 0.22, 0.14, -0.010, 0.008, "high"),
    ("Z04", "Harbor Point",          "industrial",       3200, 35000, 0.20, 0.16, -0.015, -0.005, "high"),
    ("Z05", "Westlake",              "residential_affluent", 9500, 95000, 0.24, 0.12, 0.012, -0.025, "low"),
    ("Z06", "Cedar Hills",           "residential_middle", 11200, 67000, 0.28, 0.10, 0.020, -0.015, "low"),
    ("Z07", "Northgate",             "residential_middle", 8900, 58000, 0.26, 0.13, 0.025, 0.005, "moderate"),
    ("Z08", "Eastside",              "residential_low_income", 7600, 32000, 0.32, 0.09, 0.005, 0.020, "high"),
    ("Z09", "Southview",             "residential_low_income", 6900, 29000, 0.34, 0.11, -0.020, 0.015, "high"),
    ("Z10", "Parkland Heights",      "residential_affluent", 7200, 102000, 0.22, 0.15, 0.018, -0.030, "low"),
    ("Z11", "University District",   "institutional",    12500, 28000, 0.08, 0.03, 0.015, 0.010, "low"),
    ("Z12", "Greenfield",            "suburban",          14000, 72000, 0.30, 0.08, 0.030, -0.020, "low"),
    ("Z13", "Ironworks",             "industrial",       2800, 41000, 0.15, 0.18, -0.008, 0.022, "high"),
    ("Z14", "Millbrook",             "residential_middle", 9800, 55000, 0.27, 0.14, -0.025, -0.010, "moderate"),
    ("Z15", "Clearwater",            "mixed_use",        5500, 48000, 0.19, 0.10, 0.003, -0.008, "moderate"),
    ("Z16", "Ridgewood",             "suburban",          10800, 78000, 0.31, 0.09, 0.035, 0.008, "low"),
    ("Z17", "Lakeshore",             "residential_affluent", 6100, 88000, 0.20, 0.17, -0.005, -0.035, "low"),
    ("Z18", "Fairview",              "residential_low_income", 5800, 31000, 0.30, 0.13, -0.018, 0.025, "high"),
    ("Z19", "Maple Grove",           "suburban",          11500, 69000, 0.29, 0.11, 0.028, -0.028, "low"),
    ("Z20", "Warehouse Row",         "industrial",       1900, 36000, 0.10, 0.05, -0.003, 0.018, "high"),
    ("Z21", "Hilltop",               "residential_middle", 7400, 53000, 0.25, 0.15, 0.022, 0.018, "moderate"),
    ("Z22", "River Bend",            "mixed_use",        4800, 57000, 0.17, 0.09, -0.012, -0.018, "moderate"),
    ("Z23", "St. Catherine's",       "residential_middle", 8300, 60000, 0.26, 0.16, -0.028, -0.022, "moderate"),
    ("Z24", "North Industrial Park", "industrial",       1500, 40000, 0.08, 0.04, 0.032, 0.025, "high"),
    ("Z25", "Brookside",             "residential_low_income", 5200, 33000, 0.33, 0.12, -0.022, 0.005, "high"),
]

HIGH_RISK_ZONES = [z[0] for z in ZONES if z[9] == "high"]
LOW_RISK_ZONES = [z[0] for z in ZONES if z[9] == "low"]

# ---------------------------------------------------------------------------
# Site types and realistic names
# ---------------------------------------------------------------------------
SITE_TYPES = {
    "air_monitoring_station": {
        "names": [
            "AQM Station {n}", "Air Quality Monitor {n}", "AQMS-{zone}",
            "Ambient Air Station {n}", "Continuous Air Monitor {n}",
        ],
        "count_range": (2, 4),  # per zone that has them
    },
    "water_monitoring_station": {
        "names": [
            "Water Quality Station {n}", "WQM-{zone}", "Stream Monitor {n}",
            "River Gauge Station {n}", "Intake Monitor {n}",
        ],
        "count_range": (1, 2),
    },
    "school": {
        "names": [
            "{zone} Elementary", "{zone} Middle School", "{zone} High School",
            "Lincoln Elementary", "Washington Middle", "Jefferson Academy",
            "Martin Luther King Jr. Elementary", "Roosevelt High",
            "Riverton Preparatory", "Clearwater Montessori",
            "St. Mary's Catholic School", "Brookside Charter Academy",
        ],
        "count_range": (1, 3),
    },
    "clinic": {
        "names": [
            "{zone} Health Center", "Community Clinic - {zone}",
            "Riverton Family Medicine", "Valley Urgent Care",
            "Eastside Free Clinic", "Southview Community Health",
            "University Health Services", "Pediatric Wellness Center",
            "Northgate Medical Group", "Downtown Walk-In Clinic",
        ],
        "count_range": (0, 2),
    },
    "industrial_facility": {
        "names": [
            "Riverton Steel Works", "Columbia Chemical Corp",
            "Valley Concrete & Asphalt", "Tri-State Plastics",
            "Ironworks Foundry", "Harbor Logistics Terminal",
            "Old Mill Lumber Processing", "Northgate Auto Salvage",
            "Eastside Recycling Center", "Warehouse Row Storage Depot",
            "Columbia Power Plant Unit {n}", "Riverton Water Treatment",
            "Millbrook Food Processing", "Fairview Waste Transfer",
            "Industrial Park Metals LLC", "River Bend Petroleum Storage",
        ],
        "count_range": (1, 4),
    },
    "park": {
        "names": [
            "{zone} Park", "Riverside Park", "Cedar Hills Nature Reserve",
            "Lakeshore Greenway", "Veterans Memorial Park",
            "Parkland Community Garden", "Brookside Trail",
            "University Arboretum", "Hilltop Overlook Park",
        ],
        "count_range": (0, 2),
    },
    "food_establishment": {
        "names": [
            "Golden Dragon Restaurant", "Mama Rosa's Pizzeria",
            "Taqueria El Sol", "Riverton Diner", "The Olive Branch Cafe",
            "Pho Saigon", "Burger Barn", "Seoul Kitchen",
            "Downtown Bakery & Deli", "Eastside BBQ Pit",
            "Harbor Fish Market", "The Green Plate",
            "Millbrook Grocery & Deli", "Campus Pizza",
            "Fairview Corner Store", "Quick Mart - {zone}",
        ],
        "count_range": (1, 5),
    },
}

# ---------------------------------------------------------------------------
# Sensor specifications
# ---------------------------------------------------------------------------
SENSOR_SPECS = {
    "pm25": {"unit": "ug/m3", "measurement": "PM2.5", "min": 0.5, "max": 300, "normal_range": (3, 18), "precision": 1},
    "pm10": {"unit": "ug/m3", "measurement": "PM10", "min": 1, "max": 500, "normal_range": (8, 40), "precision": 1},
    "ozone": {"unit": "ppm", "measurement": "Ozone (O3)", "min": 0.000, "max": 0.200, "normal_range": (0.015, 0.065), "precision": 3},
    "no2": {"unit": "ppb", "measurement": "Nitrogen Dioxide (NO2)", "min": 0, "max": 150, "normal_range": (5, 35), "precision": 1},
    "co": {"unit": "ppm", "measurement": "Carbon Monoxide (CO)", "min": 0.0, "max": 35.0, "normal_range": (0.2, 2.5), "precision": 1},
    "so2": {"unit": "ppb", "measurement": "Sulfur Dioxide (SO2)", "min": 0, "max": 200, "normal_range": (1, 15), "precision": 1},
    "temperature": {"unit": "celsius", "measurement": "Ambient Temperature", "min": -15, "max": 45, "normal_range": (5, 30), "precision": 1},
    "humidity": {"unit": "percent", "measurement": "Relative Humidity", "min": 5, "max": 100, "normal_range": (30, 80), "precision": 0},
    "ph": {"unit": "pH", "measurement": "Water pH", "min": 4.0, "max": 10.0, "normal_range": (6.5, 8.5), "precision": 2},
    "turbidity": {"unit": "NTU", "measurement": "Turbidity", "min": 0.1, "max": 50.0, "normal_range": (0.3, 4.0), "precision": 2},
    "dissolved_oxygen": {"unit": "mg/L", "measurement": "Dissolved Oxygen", "min": 0.0, "max": 14.0, "normal_range": (5.0, 9.0), "precision": 1},
    "conductivity": {"unit": "uS/cm", "measurement": "Specific Conductance", "min": 50, "max": 2000, "normal_range": (200, 800), "precision": 0},
    "noise": {"unit": "dBA", "measurement": "Ambient Noise Level", "min": 20, "max": 110, "normal_range": (35, 65), "precision": 0},
}

AIR_SENSORS = ["pm25", "pm10", "ozone", "no2", "co", "so2", "temperature", "humidity"]
WATER_SENSORS = ["ph", "turbidity", "dissolved_oxygen", "conductivity", "temperature"]

# ---------------------------------------------------------------------------
# Health incident types and templates
# ---------------------------------------------------------------------------
INCIDENT_TYPES = {
    "respiratory": {
        "subtypes": ["asthma_exacerbation", "copd_flare", "bronchitis", "pneumonia", "respiratory_infection"],
        "severity_weights": {"mild": 0.45, "moderate": 0.35, "severe": 0.15, "critical": 0.05},
        "seasonal_peak": 1,  # January (winter)
        "pollution_correlated": True,
        "narrative_templates": [
            "Patient presented with {symptom}. Onset {onset}. {history}. {treatment}.",
            "{age_group} patient, chief complaint: {symptom}. Duration: {duration}. {exposure}. {treatment}.",
            "Referred from {referral}. {symptom} worsening over {duration}. {history}. Plan: {treatment}.",
        ],
    },
    "waterborne": {
        "subtypes": ["gastroenteritis", "giardiasis", "cryptosporidiosis", "legionellosis", "skin_irritation"],
        "severity_weights": {"mild": 0.50, "moderate": 0.35, "severe": 0.12, "critical": 0.03},
        "seasonal_peak": 7,  # July (summer water issues)
        "pollution_correlated": True,
        "narrative_templates": [
            "Patient reports {symptom} beginning {onset}. Water source: {water_source}. {lab_result}.",
            "Cluster investigation: {symptom}. {count} similar cases in {zone}. Source suspected: {water_source}.",
            "{age_group} with {symptom}. {duration} duration. Stool sample: {lab_result}. {treatment}.",
        ],
    },
    "heat_related": {
        "subtypes": ["heat_exhaustion", "heat_stroke", "dehydration", "heat_cramps"],
        "severity_weights": {"mild": 0.40, "moderate": 0.30, "severe": 0.20, "critical": 0.10},
        "seasonal_peak": 7,  # July
        "pollution_correlated": False,
        "narrative_templates": [
            "Patient found {location}. Core temp: {temp}F. {symptom}. {treatment}.",
            "EMS transport: {age_group}, {symptom} after {activity}. Ambient temp: {ambient}F. {treatment}.",
            "Walk-in: {symptom}. Outdoor worker, {duration} exposure. {vitals}. {treatment}.",
        ],
    },
    "lead_exposure": {
        "subtypes": ["elevated_blood_lead", "lead_poisoning"],
        "severity_weights": {"mild": 0.55, "moderate": 0.30, "severe": 0.12, "critical": 0.03},
        "seasonal_peak": None,  # year-round
        "pollution_correlated": False,
        "narrative_templates": [
            "Screening result: BLL {bll} ug/dL. {age_group}. Residence: {address}. Built: {year_built}. {followup}.",
            "Follow-up visit. Previous BLL: {prev_bll} ug/dL, current: {bll} ug/dL. {address}. {followup}.",
            "Referral from pediatrician. BLL {bll} ug/dL. {age_group}, residence built {year_built}. {inspection_status}.",
        ],
    },
    "vector_borne": {
        "subtypes": ["west_nile_virus", "lyme_disease", "tick_borne_illness"],
        "severity_weights": {"mild": 0.35, "moderate": 0.40, "severe": 0.20, "critical": 0.05},
        "seasonal_peak": 8,  # August
        "pollution_correlated": False,
        "narrative_templates": [
            "Patient presents with {symptom}. Recent outdoor activity: {activity}. {location}. {lab_result}.",
            "{age_group}, {symptom} x {duration}. Tick attachment noted {body_part}. {lab_result}. {treatment}.",
            "Confirmed {subtype}. Symptom onset: {onset}. Likely exposure: {location}. {treatment}.",
        ],
    },
}

RESPIRATORY_SYMPTOMS = [
    "acute shortness of breath", "wheezing and chest tightness",
    "persistent cough with sputum production", "dyspnea on exertion",
    "nocturnal cough worsening", "increased rescue inhaler use",
    "chest pain with deep inspiration", "difficulty breathing at rest",
]
WATERBORNE_SYMPTOMS = [
    "nausea, vomiting, and diarrhea", "abdominal cramping and watery diarrhea",
    "persistent diarrhea x 5 days", "fever and bloody stool",
    "skin rash after swimming", "gastrointestinal distress",
]
HEAT_SYMPTOMS = [
    "dizziness and profuse sweating", "altered mental status",
    "muscle cramps and weakness", "nausea and headache",
    "syncope episode", "confusion and hot dry skin",
]

# ---------------------------------------------------------------------------
# Inspection templates
# ---------------------------------------------------------------------------
VIOLATION_TYPES = {
    "food_safety": [
        "Improper cold holding temperature ({temp}F observed, 41F required)",
        "Handwashing sink not accessible; blocked by storage containers",
        "No certified food manager on duty during inspection",
        "Raw meat stored above ready-to-eat foods in walk-in cooler",
        "Expired food items found: {items}",
        "Pest evidence (droppings) observed near food prep area",
        "Sanitizer concentration below required minimum ({ppm} ppm, 200 ppm required)",
        "Employee handling food without gloves; no handwash observed between tasks",
        "Grease buildup on exhaust hood above fryer station",
        "Thermometer not available in reach-in cooler",
    ],
    "environmental": [
        "Dust particulate accumulation exceeds threshold on HVAC intake",
        "Standing water observed near foundation; potential mosquito breeding site",
        "Lead paint chips observed in windowsills of classroom {room}",
        "Asbestos-containing material damaged in boiler room; fibers potentially released",
        "Mold growth visible on ceiling tiles in {location}",
        "Chemical storage not properly segregated; incompatible materials adjacent",
        "Emergency eyewash station non-functional",
        "Stack emission opacity estimated at {opacity}% (limit: 20%)",
        "Wastewater discharge pH measured at {ph} (permitted range: 6.0-9.0)",
        "Noise level at property boundary: {noise} dBA (limit: 65 dBA)",
    ],
    "safety": [
        "Fire extinguisher expired ({date}); not replaced",
        "Emergency exit blocked by stored pallets",
        "Missing guardrail on elevated platform (>4 ft)",
        "Electrical panel obstructed; 36-inch clearance not maintained",
        "No Safety Data Sheets available for on-site chemicals",
        "Eye protection not worn in designated area",
    ],
}

INSPECTOR_NAMES = [
    "M. Rodriguez", "J. Chen", "A. Thompson", "R. Patel", "S. O'Brien",
    "K. Williams", "D. Nakamura", "L. Okonkwo", "T. Fitzgerald", "B. Gupta",
    "C. Martinez", "P. Andersen", "N. Volkov", "H. Yilmaz", "F. Al-Rashidi",
]

# ---------------------------------------------------------------------------
# Complaint templates with realistic PII patterns (for PII detection testing)
# ---------------------------------------------------------------------------
COMPLAINT_CATEGORIES = {
    "air_quality": {
        "weight": 0.30,
        "templates": [
            "Strong chemical odor coming from the plant on {street}. Started around {time}. My kids have been coughing all evening. Please investigate. - {name}, {phone}",
            "Thick black smoke from the stack at {facility}. Happening every morning between 5-7am for the past week. I have photos. Contact me at {email}.",
            "Dust from the construction site at {address} is covering our cars and getting inside the house. My daughter has asthma and this is making it worse.",
            "Burning smell in the neighborhood. Not sure of the source but it smells like plastic or rubber. Multiple neighbors have complained. We live at {address}.",
            "The air around {facility} smells like rotten eggs. It's been ongoing for months. When will something be done? You can reach me at {phone}.",
            "Excessive idling of diesel trucks at {facility} loading dock. Fumes entering our apartment windows. We have a newborn baby. URGENT.",
        ],
    },
    "water_quality": {
        "weight": 0.20,
        "templates": [
            "Our tap water has been brown/rusty for 3 days. We're at {address}. Is it safe to drink? We have small children. Call {phone}.",
            "Dead fish observed along the riverbank near {location}. Suspicious discharge coming from a pipe upstream. Photos attached.",
            "Swimming area at {location} has green algae bloom. Kids were swimming there yesterday. Should we be concerned? - {name}",
            "Water pressure has dropped and there's a sulfur smell. Address: {address}. My neighbor at {neighbor_address} has the same issue.",
            "Storm drain on {street} is discharging milky white liquid into the creek. Observed today at {time}. Not from rain.",
        ],
    },
    "noise": {
        "weight": 0.20,
        "templates": [
            "Construction noise from {address} starting at 5:30am, before the 7am ordinance. This has been going on for 2 weeks. Please enforce. - {name}",
            "The plant at {facility} runs heavy machinery 24/7. Night shift noise is unbearable. We've measured {noise} dB from our bedroom window.",
            "Loud music and bass from {address} every weekend until 3am. Have called police multiple times. Need environmental health follow-up.",
            "Truck traffic on {street} has increased dramatically. Constant jake braking at all hours. Vibration cracking our foundation. {phone}",
        ],
    },
    "waste": {
        "weight": 0.15,
        "templates": [
            "Illegal dumping behind {address}. Mattresses, tires, and what looks like paint cans. Been here for weeks. Attracting rats.",
            "Dumpster at {facility} overflowing. Garbage in the street. Strong smell. Health hazard for the neighborhood.",
            "Found barrels of unknown liquid in the vacant lot at {address}. Leaking into the ground. Possibly hazardous. URGENT.",
            "The {facility} is storing waste containers outdoors without covers. Rain is washing chemicals into the storm drain.",
        ],
    },
    "pest": {
        "weight": 0.10,
        "templates": [
            "Rat infestation in the alley behind {address}. Multiple sightings daily. Restaurant garbage not properly contained.",
            "Mosquito breeding in standing water at abandoned property {address}. Overgrown lot with tires and containers.",
            "Cockroach infestation spreading from vacant unit to occupied apartments at {address}. Landlord unresponsive. - {name}, {email}",
        ],
    },
    "other": {
        "weight": 0.05,
        "templates": [
            "Requesting air quality testing near {school}. Parents are concerned about proximity to {facility}.",
            "Can someone test our soil? We want to start a community garden at {address} but the lot was previously industrial.",
            "Is there lead testing available for our home? Built in {year}. We have a toddler. {address}. Contact: {phone}.",
        ],
    },
}

FIRST_NAMES = [
    "Maria", "James", "Patricia", "Robert", "Linda", "Michael", "Barbara", "David",
    "Jennifer", "William", "Susan", "Carlos", "Sarah", "Ahmed", "Priya", "Wei",
    "Olga", "Mohammed", "Fatima", "Kenji", "Aisha", "Ivan", "Elena", "Dmitri",
    "Yuki", "Omar", "Lucia", "Hans", "Amara", "Raj", "Svetlana", "Takeshi",
]
LAST_NAMES = [
    "Johnson", "Williams", "Garcia", "Martinez", "Brown", "Jones", "Miller",
    "Davis", "Rodriguez", "Wilson", "Chen", "Patel", "Kim", "Nguyen", "Ali",
    "Singh", "Okafor", "Petrov", "Mueller", "Tanaka", "Svensson", "Costa",
    "Ibrahim", "Nakamura", "Kowalski", "Volkov", "Johansson", "Fernandez",
]
STREET_NAMES = [
    "Oak St", "Maple Ave", "River Rd", "Industrial Blvd", "Main St",
    "Cedar Ln", "Park Ave", "Mill Rd", "Harbor Dr", "5th Ave",
    "Elm St", "Walnut St", "Pine Rd", "2nd St", "Lake Dr",
    "Commerce Way", "Factory Rd", "School St", "Church Ave", "Bridge St",
]

# ---------------------------------------------------------------------------
# Programs
# ---------------------------------------------------------------------------
PROGRAM_DEFINITIONS = [
    {
        "name": "Clean Air for Kids",
        "type": "asthma_management",
        "description": "School-based asthma management program providing education, inhaler technique training, and air quality monitoring in classrooms. Targets schools in high-pollution zones.",
        "target_zones": HIGH_RISK_ZONES,
        "start_date": "2024-01-15",
        "budget": 185000,
        "capacity": 500,
    },
    {
        "name": "Lead-Safe Homes Initiative",
        "type": "lead_abatement",
        "description": "Free lead paint inspection and abatement for homes built before 1978 in eligible zones. Includes blood lead level screening for children under 6.",
        "target_zones": ["Z08", "Z09", "Z18", "Z25", "Z03"],
        "start_date": "2024-02-01",
        "budget": 420000,
        "capacity": 200,
    },
    {
        "name": "River Watchers",
        "type": "community_monitoring",
        "description": "Citizen science program training volunteers to collect and report water quality data from local streams and the Riverton River. Monthly sampling events.",
        "target_zones": ["Z02", "Z15", "Z22", "Z17"],
        "start_date": "2024-03-01",
        "budget": 35000,
        "capacity": 80,
    },
    {
        "name": "Heat Action Plan",
        "type": "heat_resilience",
        "description": "Summer cooling center operations, wellness checks for elderly residents, and heat-health alert system. Activated when forecast exceeds 95F for 2+ consecutive days.",
        "target_zones": HIGH_RISK_ZONES + ["Z01", "Z07"],
        "start_date": "2024-05-15",
        "budget": 92000,
        "capacity": 1500,
    },
    {
        "name": "Healthy Corner Stores",
        "type": "food_access",
        "description": "Partnership with corner stores in food desert zones to stock fresh produce and meet basic food safety standards. Includes refrigeration equipment grants.",
        "target_zones": ["Z08", "Z09", "Z18", "Z25"],
        "start_date": "2024-01-01",
        "budget": 67000,
        "capacity": 30,
    },
    {
        "name": "Industrial Neighbor Network",
        "type": "community_engagement",
        "description": "Structured dialogue program between industrial facilities and neighboring residents. Quarterly town halls, real-time emission notification system, complaint fast-track.",
        "target_zones": ["Z03", "Z04", "Z13", "Z20", "Z24"],
        "start_date": "2024-02-15",
        "budget": 28000,
        "capacity": 300,
    },
    {
        "name": "Mosquito Surveillance & Control",
        "type": "vector_control",
        "description": "Integrated pest management for mosquito-borne disease prevention. Trap monitoring, larvicide treatment of standing water, and community education.",
        "target_zones": [z[0] for z in ZONES],  # city-wide
        "start_date": "2024-04-01",
        "budget": 155000,
        "capacity": None,  # city-wide, no enrollment cap
    },
    {
        "name": "Air Quality Flag Program",
        "type": "awareness",
        "description": "Daily air quality flag display at schools and parks. Green/Yellow/Orange/Red flags based on AQI. Includes curriculum materials for teachers.",
        "target_zones": [z[0] for z in ZONES],
        "start_date": "2024-01-01",
        "budget": 12000,
        "capacity": None,
    },
    {
        "name": "Brownfield Assessment Pilot",
        "type": "site_remediation",
        "description": "Phase I and Phase II environmental site assessments for 10 priority brownfield parcels. Goal: prepare sites for safe redevelopment as affordable housing or community space.",
        "target_zones": ["Z03", "Z04", "Z13", "Z20"],
        "start_date": "2024-06-01",
        "budget": 350000,
        "capacity": 10,
    },
    {
        "name": "Childhood Asthma Home Visits",
        "type": "home_health",
        "description": "Registered nurses conduct home environmental assessments for children with poorly controlled asthma. Identifies triggers (mold, pests, smoking) and provides remediation supplies.",
        "target_zones": HIGH_RISK_ZONES,
        "start_date": "2024-03-15",
        "budget": 210000,
        "capacity": 150,
    },
]


# ===========================================================================
# GENERATORS
# ===========================================================================

def generate_zones() -> list[dict]:
    """Generate zone records with demographic and environmental risk data."""
    zones = []
    for z in ZONES:
        zone_id, name, ztype, pop, income, pct_u18, pct_o65, lat_off, lon_off, risk = z
        zones.append({
            "zone_id": zone_id,
            "name": name,
            "type": ztype,
            "city": CITY_NAME,
            "state": STATE,
            "population": pop,
            "area_sq_miles": round(random.uniform(0.8, 4.5), 2),
            "median_household_income": income,
            "pct_under_18": pct_u18,
            "pct_over_65": pct_o65,
            "pct_minority": round(random.uniform(0.15, 0.75), 2),
            "pct_below_poverty": round(max(0.03, 1.0 - income / 120000 + random.uniform(-0.05, 0.05)), 2),
            "housing_units": int(pop * random.uniform(0.38, 0.48)),
            "pct_built_before_1978": round(
                0.6 + random.uniform(-0.1, 0.1) if ztype in ("industrial", "residential_low_income") else
                0.3 + random.uniform(-0.1, 0.1) if ztype in ("residential_middle", "mixed_use") else
                0.15 + random.uniform(-0.05, 0.05),
                2
            ),
            "environmental_risk_score": risk,
            "centroid_lat": round(BASE_LAT + lat_off, 6),
            "centroid_lon": round(BASE_LON + lon_off, 6),
        })
    return zones


def generate_sites(zones: list[dict]) -> list[dict]:
    """Generate 500 sites across zones with realistic distribution."""
    sites = []
    site_counter = 0
    used_names: set[str] = set()
    zone_lookup = {z["zone_id"]: z for z in zones}

    for zone in zones:
        zid = zone["zone_id"]
        ztype = zone["type"]
        zname = zone["name"]

        # Determine which site types appear in this zone and how many
        zone_site_types: list[tuple[str, int]] = []

        if ztype in ("industrial",):
            zone_site_types.append(("air_monitoring_station", random.randint(2, 4)))
            zone_site_types.append(("industrial_facility", random.randint(2, 5)))
            zone_site_types.append(("food_establishment", random.randint(0, 2)))
        elif ztype in ("residential_low_income",):
            zone_site_types.append(("air_monitoring_station", random.randint(1, 2)))
            zone_site_types.append(("school", random.randint(1, 3)))
            zone_site_types.append(("clinic", random.randint(1, 2)))
            zone_site_types.append(("food_establishment", random.randint(2, 4)))
            zone_site_types.append(("park", random.randint(0, 1)))
        elif ztype in ("residential_middle", "residential_affluent"):
            zone_site_types.append(("air_monitoring_station", random.randint(0, 1)))
            zone_site_types.append(("school", random.randint(1, 3)))
            zone_site_types.append(("clinic", random.randint(0, 1)))
            zone_site_types.append(("food_establishment", random.randint(2, 5)))
            zone_site_types.append(("park", random.randint(1, 2)))
        elif ztype in ("mixed_use", "urban_commercial"):
            zone_site_types.append(("air_monitoring_station", random.randint(1, 2)))
            zone_site_types.append(("clinic", random.randint(1, 2)))
            zone_site_types.append(("food_establishment", random.randint(3, 6)))
            zone_site_types.append(("school", random.randint(0, 1)))
        elif ztype == "institutional":
            zone_site_types.append(("air_monitoring_station", 1))
            zone_site_types.append(("school", random.randint(1, 2)))
            zone_site_types.append(("clinic", 1))
            zone_site_types.append(("food_establishment", random.randint(3, 5)))
            zone_site_types.append(("park", 1))
        elif ztype == "suburban":
            zone_site_types.append(("school", random.randint(2, 3)))
            zone_site_types.append(("food_establishment", random.randint(2, 4)))
            zone_site_types.append(("park", random.randint(1, 2)))
            zone_site_types.append(("clinic", random.randint(0, 1)))

        # Add water monitoring to zones near the river
        if zid in ("Z02", "Z04", "Z15", "Z17", "Z22", "Z03"):
            zone_site_types.append(("water_monitoring_station", random.randint(1, 2)))

        for stype, count in zone_site_types:
            for i in range(count):
                site_counter += 1
                name_templates = SITE_TYPES[stype]["names"]
                name = random.choice(name_templates).format(n=site_counter, zone=zname.split()[0])
                # Ensure unique
                while name in used_names:
                    name = f"{name} #{random.randint(2,9)}"
                used_names.add(name)

                # Jitter lat/lon within zone
                lat = zone["centroid_lat"] + random.uniform(-0.005, 0.005)
                lon = zone["centroid_lon"] + random.uniform(-0.005, 0.005)

                # Attributes JSON (varies by site type)
                attributes: dict = {}
                if stype == "industrial_facility":
                    attributes = {
                        "sic_code": random.choice(["2911", "3312", "3241", "2821", "4911", "4952", "2011", "5093"]),
                        "permit_number": f"ENV-{YEAR}-{random.randint(1000,9999)}",
                        "employee_count": random.randint(15, 500),
                        "operational_since": random.randint(1955, 2020),
                        "last_epa_inspection": date_only(rand_date(datetime(2022, 1, 1), datetime(2024, 6, 30))),
                        "emissions_tier": random.choice(["major", "minor", "synthetic_minor"]),
                    }
                elif stype == "school":
                    attributes = {
                        "grade_levels": random.choice(["K-5", "6-8", "9-12", "K-8", "PreK-5"]),
                        "enrollment": random.randint(150, 1200),
                        "year_built": random.randint(1925, 2018),
                        "hvac_type": random.choice(["central", "window_units", "rooftop_units", "mixed"]),
                        "has_cafeteria": random.choice([True, True, True, False]),
                        "playground_surface": random.choice(["rubber", "mulch", "asphalt", "grass"]),
                    }
                elif stype == "food_establishment":
                    attributes = {
                        "cuisine_type": random.choice(["american", "mexican", "chinese", "italian", "japanese", "indian", "korean", "vietnamese", "bakery", "deli", "bbq", "seafood"]),
                        "seating_capacity": random.randint(0, 120),
                        "license_number": f"FE-{random.randint(10000,99999)}",
                        "last_inspection_score": random.randint(60, 100),
                        "risk_category": random.choice(["I", "II", "III"]),
                    }
                elif stype == "clinic":
                    attributes = {
                        "facility_type": random.choice(["fqhc", "urgent_care", "primary_care", "pediatric", "community_health"]),
                        "accepts_medicaid": random.choice([True, True, True, False]),
                        "providers": random.randint(2, 15),
                        "annual_visits": random.randint(3000, 45000),
                    }
                elif stype in ("air_monitoring_station", "water_monitoring_station"):
                    attributes = {
                        "network": random.choice(["SLAMS", "NCORE", "local", "community"]),
                        "elevation_ft": random.randint(400, 1200),
                        "installation_date": date_only(rand_date(datetime(2015, 1, 1), datetime(2023, 12, 31))),
                        "data_logger": random.choice(["Campbell CR1000X", "HOBO MX2301", "Envidas Ultimate", "custom_iot"]),
                    }

                site = {
                    "site_id": f"S{site_counter:04d}",
                    "name": name,
                    "site_type": stype,
                    "zone_id": zid,
                    "address": f"{random.randint(100, 9999)} {random.choice(STREET_NAMES)}",
                    "city": CITY_NAME,
                    "state": STATE,
                    "latitude": round(lat, 6),
                    "longitude": round(lon, 6),
                    "status": random.choices(["active", "active", "active", "inactive", "decommissioned"], weights=[70, 10, 10, 7, 3])[0],
                    "commissioned_date": date_only(rand_date(datetime(2010, 1, 1), datetime(2023, 12, 31))),
                    "attributes": attributes,
                }
                sites.append(site)

    # Pad to ~500 if needed with more food establishments and misc
    while len(sites) < 500:
        site_counter += 1
        zone = random.choice(zones)
        stype = random.choice(["food_establishment", "food_establishment", "park", "clinic"])
        name = f"{random.choice(STREET_NAMES).split()[0]} {random.choice(['Grill', 'Cafe', 'Market', 'Diner', 'Pharmacy', 'Fitness'])} #{site_counter}"
        sites.append({
            "site_id": f"S{site_counter:04d}",
            "name": name,
            "site_type": stype,
            "zone_id": zone["zone_id"],
            "address": f"{random.randint(100, 9999)} {random.choice(STREET_NAMES)}",
            "city": CITY_NAME,
            "state": STATE,
            "latitude": round(zone["centroid_lat"] + random.uniform(-0.005, 0.005), 6),
            "longitude": round(zone["centroid_lon"] + random.uniform(-0.005, 0.005), 6),
            "status": "active",
            "commissioned_date": date_only(rand_date(datetime(2015, 1, 1), datetime(2023, 12, 31))),
            "attributes": {},
        })

    return sites[:500]


def generate_sensors(sites: list[dict]) -> list[dict]:
    """Generate ~1200 sensors attached to monitoring stations, schools, facilities, and parks."""
    sensors = []
    sensor_id = 0

    def _add_sensor(site_id: str, stype: str, manufacturer: str, model_prefix: str,
                    serial_prefix: str, interval: int | None = None) -> None:
        nonlocal sensor_id
        sensor_id += 1
        spec = SENSOR_SPECS[stype]
        install_date = rand_date(datetime(2018, 1, 1), datetime(2023, 12, 31))
        cal_status = random.choices(
            ["valid", "valid", "valid", "due", "overdue", "failed"],
            weights=[50, 20, 10, 10, 7, 3]
        )[0]
        sensors.append({
            "sensor_id": f"SEN{sensor_id:05d}",
            "site_id": site_id,
            "sensor_type": stype,
            "measurement": spec["measurement"],
            "unit": spec["unit"],
            "manufacturer": manufacturer,
            "model": f"{model_prefix}-{random.randint(1,9)}",
            "serial_number": f"{serial_prefix}{random.randint(100000,999999)}",
            "install_date": date_only(install_date),
            "last_calibration": date_only(install_date + timedelta(days=random.randint(30, 365))),
            "calibration_status": cal_status,
            "status": "active" if cal_status != "failed" else "maintenance",
            "sampling_interval_minutes": interval or random.choice([1, 5, 15, 60]),
            "precision": spec["precision"],
        })

    for site in sites:
        if site["site_type"] == "air_monitoring_station":
            # Each air station has ALL air sensor types (full suite)
            for stype in AIR_SENSORS:
                _add_sensor(site["site_id"], stype,
                           random.choice(["Teledyne API", "Thermo Fisher", "Met One", "Aeroqual", "Honeywell", "Vaisala"]),
                           random.choice(["T640", "BAM-1022", "49i", "AQY-1", "S500", "HMP155"]),
                           random.choice(["A", "B", "C", "T", "M"]))
            # Some stations also have noise monitors
            if random.random() < 0.6:
                _add_sensor(site["site_id"], "noise",
                           "Cirrus Research", "Optimus", "CR")

        elif site["site_type"] == "water_monitoring_station":
            # All water sensors at each station
            for stype in WATER_SENSORS:
                _add_sensor(site["site_id"], stype,
                           random.choice(["YSI", "Hach", "In-Situ", "Xylem", "Eureka"]),
                           random.choice(["EXO2", "Hydrolab", "AquaTroll", "ProDSS"]),
                           "W",
                           interval=random.choice([5, 15, 30, 60]))

        elif site["site_type"] == "school":
            # Schools have indoor air quality monitors (PM2.5, CO, temperature, humidity)
            for stype in ["pm25", "co", "temperature", "humidity"]:
                _add_sensor(site["site_id"], stype,
                           random.choice(["PurpleAir", "Aeroqual", "uHoo", "Kaiterra"]),
                           random.choice(["PA-II", "AQM-Mini", "Beam", "SE200"]),
                           "SCH",
                           interval=random.choice([5, 15]))
            # Some schools also have outdoor PM2.5 and noise
            if random.random() < 0.5:
                _add_sensor(site["site_id"], "pm25",
                           "PurpleAir", "PA-II-Outdoor", "SPO",
                           interval=5)
            if random.random() < 0.4:
                _add_sensor(site["site_id"], "noise",
                           "Cirrus Research", "Optimus-Green", "SN")

        elif site["site_type"] == "industrial_facility":
            # Industrial facilities have fence-line monitors (multiple pollutant types)
            n_sensors = random.randint(2, 5)
            selected = random.sample(["pm25", "pm10", "no2", "so2", "co", "noise"], k=min(n_sensors, 6))
            for stype in selected:
                _add_sensor(site["site_id"], stype,
                           random.choice(["Aeroqual", "Honeywell", "Cirrus Research", "Teledyne API"]),
                           f"FenceLine-{random.choice(['Pro','Lite','Max'])}",
                           "FL",
                           interval=random.choice([1, 5]))

        elif site["site_type"] == "park":
            # Parks may have noise and air quality monitors
            if random.random() < 0.6:
                _add_sensor(site["site_id"], "noise",
                           random.choice(["Cirrus Research", "Bruel & Kjaer", "NTi Audio"]),
                           "Outdoor-Monitor", "PK",
                           interval=15)
            if random.random() < 0.4:
                _add_sensor(site["site_id"], "pm25",
                           "PurpleAir", "PA-II-Outdoor", "PP",
                           interval=15)
            if random.random() < 0.3:
                _add_sensor(site["site_id"], "temperature",
                           "Vaisala", "WXT536", "PW",
                           interval=15)

        elif site["site_type"] == "clinic":
            # Some clinics have indoor air quality monitors
            if random.random() < 0.3:
                for stype in ["pm25", "temperature", "humidity"]:
                    _add_sensor(site["site_id"], stype,
                               random.choice(["Kaiterra", "uHoo", "Awair"]),
                               "Indoor-AQ", "CL",
                               interval=15)

    return sensors


def _seasonal_factor(month: int, peak_month: int, amplitude: float = 0.5) -> float:
    """Sinusoidal seasonal multiplier. 1.0 at peak, (1-amplitude) at trough."""
    phase = 2 * math.pi * (month - peak_month) / 12
    return 1.0 + amplitude * math.cos(phase)


def _diurnal_factor(hour: int, peak_hour: int = 14, amplitude: float = 0.3) -> float:
    """Sinusoidal diurnal multiplier for readings that vary by time of day."""
    phase = 2 * math.pi * (hour - peak_hour) / 24
    return 1.0 + amplitude * math.cos(phase)


def _zone_pollution_factor(zone_id: str) -> float:
    """Industrial/low-income zones have higher baseline pollution."""
    if zone_id in HIGH_RISK_ZONES:
        return random.uniform(1.4, 2.2)
    elif zone_id in LOW_RISK_ZONES:
        return random.uniform(0.5, 0.8)
    return random.uniform(0.9, 1.2)


def generate_readings(sensors: list[dict], sites: list[dict]) -> list[dict]:
    """Generate ~50K sensor readings with realistic patterns.

    Key patterns:
    - Seasonal: PM2.5/ozone peak in summer, CO peaks in winter
    - Diurnal: ozone peaks afternoon, NO2 peaks rush hours
    - Geographic: industrial zones have higher baselines
    - Data gaps: some sensors go offline for periods
    - Anomalies: occasional spikes (real pollution events)
    """
    readings = []
    site_lookup = {s["site_id"]: s for s in sites}

    # Determine target readings per sensor (~50K total / ~1200 sensors = ~42 per sensor)
    # But vary: some sensors are hourly for a full year, others sampled less frequently
    target_total = 50000
    readings_per_sensor = max(10, target_total // len(sensors))

    for sensor in sensors:
        site = site_lookup.get(sensor["site_id"])
        if not site:
            continue

        zone_id = site.get("zone_id", "Z01")
        zone_factor = _zone_pollution_factor(zone_id)
        spec = SENSOR_SPECS.get(sensor["sensor_type"])
        if not spec:
            continue

        normal_lo, normal_hi = spec["normal_range"]
        normal_mid = (normal_lo + normal_hi) / 2
        normal_spread = (normal_hi - normal_lo) / 2

        # Determine number of readings for this sensor
        n_readings = random.randint(
            max(5, readings_per_sensor - 15),
            readings_per_sensor + 15
        )

        # Generate a gap period (sensor offline) for ~15% of sensors
        has_gap = random.random() < 0.15
        gap_start = rand_date() if has_gap else None
        gap_end = (gap_start + timedelta(days=random.randint(3, 30))) if gap_start else None

        # Determine seasonal peak month for this sensor type
        if sensor["sensor_type"] in ("ozone", "pm25", "pm10"):
            peak_month = 7  # summer
        elif sensor["sensor_type"] in ("co", "no2", "so2"):
            peak_month = 1  # winter (inversions, heating)
        elif sensor["sensor_type"] == "temperature":
            peak_month = 7
        elif sensor["sensor_type"] == "humidity":
            peak_month = 7
        elif sensor["sensor_type"] == "turbidity":
            peak_month = 4  # spring runoff
        else:
            peak_month = None

        # Determine diurnal peak
        if sensor["sensor_type"] == "ozone":
            diurnal_peak = 14  # afternoon
        elif sensor["sensor_type"] in ("no2", "co"):
            diurnal_peak = 8   # morning rush
        elif sensor["sensor_type"] == "noise":
            diurnal_peak = 12  # midday
        else:
            diurnal_peak = None

        for _ in range(n_readings):
            ts = rand_date()

            # Skip if in gap period
            if has_gap and gap_start and gap_end and gap_start <= ts <= gap_end:
                continue

            month = ts.month
            hour = ts.hour

            # Base value around normal midpoint
            base = normal_mid * zone_factor

            # Apply seasonal factor
            if peak_month:
                base *= _seasonal_factor(month, peak_month, amplitude=0.35)

            # Apply diurnal factor
            if diurnal_peak is not None:
                base *= _diurnal_factor(hour, diurnal_peak, amplitude=0.25)

            # Add noise
            noise = random.gauss(0, normal_spread * 0.3)
            value = base + noise

            # Occasional spikes (pollution events, ~3% chance)
            if random.random() < 0.03 and sensor["sensor_type"] in ("pm25", "pm10", "no2", "so2", "turbidity"):
                spike_mult = random.uniform(2.0, 5.0)
                value *= spike_mult

            # Clamp to physical limits
            value = max(spec["min"], min(spec["max"], value))
            value = round(value, spec["precision"])

            # Quality flags
            if random.random() < 0.02:
                qc_flag = random.choice(["suspect", "calibration_drift", "maintenance"])
            elif random.random() < 0.005:
                qc_flag = "invalid"
                value = None  # null reading -- intentional data quality issue
            else:
                qc_flag = "valid"

            reading = {
                "reading_id": uid(),
                "sensor_id": sensor["sensor_id"],
                "site_id": sensor["site_id"],
                "timestamp": iso(ts),
                "value": value,
                "unit": sensor["unit"],
                "sensor_type": sensor["sensor_type"],
                "qc_flag": qc_flag,
            }
            readings.append(reading)

    # Sort by timestamp
    readings.sort(key=lambda r: r["timestamp"])
    return readings


def generate_incidents(zones: list[dict], sites: list[dict]) -> list[dict]:
    """Generate ~5000 health incidents with realistic patterns."""
    incidents = []
    zone_lookup = {z["zone_id"]: z for z in zones}
    clinics = [s for s in sites if s["site_type"] == "clinic"]

    # Weight incident types
    type_weights = {
        "respiratory": 0.40,
        "waterborne": 0.18,
        "heat_related": 0.18,
        "lead_exposure": 0.12,
        "vector_borne": 0.12,
    }

    for _ in range(5000):
        inc_type = random.choices(
            list(type_weights.keys()),
            weights=list(type_weights.values())
        )[0]
        spec = INCIDENT_TYPES[inc_type]

        # Choose zone weighted by risk profile and incident type
        if spec["pollution_correlated"]:
            zone_weights = []
            for z in zones:
                w = z["population"] / 5000
                if z["environmental_risk_score"] == "high":
                    w *= 2.5
                elif z["environmental_risk_score"] == "moderate":
                    w *= 1.3
                zone_weights.append(w)
        else:
            zone_weights = [z["population"] / 5000 for z in zones]

        zone = random.choices(zones, weights=zone_weights)[0]

        # Date with seasonal pattern
        if spec["seasonal_peak"]:
            # Generate date biased toward peak month
            month = spec["seasonal_peak"]
            spread = random.gauss(0, 2.5)
            target_month = int((month + spread - 1) % 12) + 1
            day = random.randint(1, 28)
            dt = datetime(YEAR, target_month, day,
                         random.randint(0, 23), random.randint(0, 59))
        else:
            dt = rand_date()

        # Severity
        severity = random.choices(
            list(spec["severity_weights"].keys()),
            weights=list(spec["severity_weights"].values())
        )[0]
        subtype = random.choice(spec["subtypes"])

        # Reporting clinic
        zone_clinics = [c for c in clinics if c["zone_id"] == zone["zone_id"]]
        if not zone_clinics:
            zone_clinics = clinics
        reporting_facility = random.choice(zone_clinics)

        # Age and demographics
        if inc_type == "lead_exposure":
            age = random.randint(1, 6)
            age_group = "pediatric"
        elif inc_type == "heat_related":
            age = random.choices(
                [random.randint(18, 45), random.randint(65, 90), random.randint(5, 17)],
                weights=[40, 45, 15]
            )[0]
            age_group = "elderly" if age >= 65 else ("pediatric" if age < 18 else "adult")
        else:
            age = random.randint(1, 90)
            if age < 5:
                age_group = "infant"
            elif age < 18:
                age_group = "pediatric"
            elif age < 65:
                age_group = "adult"
            else:
                age_group = "elderly"

        # Generate narrative (realistic free text with some PII for PII detection testing)
        patient_name = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
        narrative = _build_incident_narrative(inc_type, subtype, severity, age_group,
                                               zone["name"], patient_name, dt)

        incident = {
            "incident_id": f"INC-{YEAR}-{len(incidents)+1:05d}",
            "incident_type": inc_type,
            "subtype": subtype,
            "severity": severity,
            "date_reported": date_only(dt),
            "date_onset": date_only(dt - timedelta(days=random.randint(0, 7))),
            "zone_id": zone["zone_id"],
            "reporting_facility_id": reporting_facility["site_id"],
            "patient_age": age,
            "patient_age_group": age_group,
            "patient_sex": random.choice(["M", "F", "M", "F", "X"]),
            "patient_zip": f"3{random.randint(1000, 9999)}",
            "narrative": narrative,
            "outcome": random.choices(
                ["resolved", "ongoing_treatment", "hospitalized", "referred", "deceased"],
                weights=[50, 25, 15, 8, 2]
            )[0],
            "follow_up_required": severity in ("severe", "critical") or random.random() < 0.2,
            "linked_program_id": None,  # filled later for some
        }
        incidents.append(incident)

    # Link some incidents to programs
    for inc in incidents:
        if inc["incident_type"] == "respiratory" and inc["zone_id"] in HIGH_RISK_ZONES and random.random() < 0.3:
            inc["linked_program_id"] = "PRG-001"  # Clean Air for Kids
        elif inc["incident_type"] == "lead_exposure" and random.random() < 0.4:
            inc["linked_program_id"] = "PRG-002"  # Lead-Safe Homes
        elif inc["incident_type"] == "heat_related" and random.random() < 0.25:
            inc["linked_program_id"] = "PRG-004"  # Heat Action Plan

    return incidents


def _build_incident_narrative(inc_type: str, subtype: str, severity: str,
                               age_group: str, zone_name: str,
                               patient_name: str, dt: datetime) -> str:
    """Build realistic clinical narrative text."""
    if inc_type == "respiratory":
        symptom = random.choice(RESPIRATORY_SYMPTOMS)
        onset = random.choice(["approximately 2 days ago", "this morning", "gradually over the past week",
                               "suddenly last night", "3 days ago, worsening"])
        history = random.choice([
            f"Hx of asthma since childhood, uses albuterol PRN",
            "No prior respiratory history",
            "Known COPD, on home O2 2L/min",
            "Seasonal allergies, no prior asthma diagnosis",
            f"Previous ER visit for wheezing {random.randint(2,18)} months ago",
        ])
        treatment = random.choice([
            "Nebulizer treatment in clinic, discharged with oral prednisone burst",
            "Referred to pulmonology for PFTs",
            "Albuterol MDI prescribed, return if worsening",
            "Admitted for IV corticosteroids and monitoring",
            "Prescribed inhaled corticosteroid controller, spacer technique reviewed",
        ])
        exposure = random.choice([
            f"Lives near industrial area in {zone_name}",
            "Reports increased outdoor activity in recent heat wave",
            "Workplace exposure to dust and fumes (construction)",
            f"School near highway; classroom ventilation reported as poor",
            "Home has visible mold in bathroom and basement",
        ])
        return random.choice([
            f"Patient {patient_name} presented with {symptom}. Onset {onset}. {history}. {treatment}.",
            f"{age_group.capitalize()} patient, chief complaint: {symptom}. Duration: {onset}. {exposure}. {treatment}.",
            f"Referred from {zone_name} school nurse. {symptom} worsening over past week. {history}. Plan: {treatment}.",
        ])

    elif inc_type == "waterborne":
        symptom = random.choice(WATERBORNE_SYMPTOMS)
        water_source = random.choice(["municipal tap water", "private well", "recreational swimming",
                                      "Riverton River downstream of discharge point", "unknown"])
        lab_result = random.choice([
            "Stool culture pending", "Positive for Giardia cysts",
            "Cryptosporidium oocysts detected", "Bacterial culture: E. coli O157:H7",
            "No pathogen identified; presumed viral", "Legionella urinary antigen positive",
        ])
        treatment = random.choice([
            "Oral rehydration, symptomatic treatment",
            "Metronidazole 250mg TID x 7 days",
            "IV fluids, hospitalized for monitoring",
            "Azithromycin prescribed, public health notified",
            "Supportive care, stool sample submitted to state lab",
        ])
        return f"Patient {patient_name} reports {symptom} beginning {random.choice(['2 days ago', 'yesterday', 'last week'])}. Water source: {water_source}. {lab_result}. {treatment}."

    elif inc_type == "heat_related":
        symptom = random.choice(HEAT_SYMPTOMS)
        temp = random.randint(100, 106) if severity in ("severe", "critical") else random.randint(99, 102)
        ambient = random.randint(95, 110)
        treatment = random.choice([
            "Active cooling initiated, IV normal saline bolus",
            "Moved to air-conditioned area, oral hydration",
            "Cooling blankets applied, monitoring in ED",
            "Rest, hydration, discharged with heat safety counseling",
        ])
        activity = random.choice(["prolonged outdoor work", "outdoor exercise", "walking to bus stop",
                                  "sitting in non-AC apartment", "working in warehouse without ventilation"])
        location = random.choice([f"{zone_name} construction site", "outdoor market", "apartment without AC",
                                  "school playground", f"{zone_name} park"])
        return f"Patient {patient_name} found at {location}. Core temp: {temp}F. {symptom}. After {activity}. Ambient temp: {ambient}F. {treatment}."

    elif inc_type == "lead_exposure":
        bll = random.choices(
            [random.uniform(3.5, 5.0), random.uniform(5.0, 10.0),
             random.uniform(10.0, 20.0), random.uniform(20.0, 45.0)],
            weights=[30, 40, 20, 10]
        )[0]
        bll = round(bll, 1)
        year_built = random.randint(1920, 1978)
        address = f"{random.randint(100, 9999)} {random.choice(STREET_NAMES)}"
        followup = random.choice([
            "Home inspection ordered", "XRF paint testing scheduled",
            "Nutritional counseling provided, recheck BLL in 3 months",
            "Referred to lead abatement program",
            "Case reported to state lead registry",
            "Chelation therapy initiated per toxicology consult",
        ])
        return f"Screening result: BLL {bll} ug/dL. {age_group.capitalize()} patient ({patient_name}). Residence: {address}, {zone_name}. Built: {year_built}. {followup}."

    else:  # vector_borne
        symptom = random.choice(["fever, headache, and myalgia", "erythema migrans rash",
                                 "fever and altered mental status", "joint pain and fatigue",
                                 "flu-like illness with rash"])
        location = random.choice([f"{zone_name} trail", "backyard garden", "campsite near Riverton River",
                                  f"{zone_name} park", "wooded area behind school"])
        lab_result = random.choice(["WNV IgM positive", "Lyme IgG/IgM pending",
                                    "Ehrlichia PCR positive", "Anaplasma smear positive",
                                    "Serologies pending"])
        treatment = random.choice(["Doxycycline 100mg BID x 21 days", "Supportive care, monitoring",
                                   "IV ceftriaxone for CNS involvement", "Amoxicillin x 14 days"])
        return f"Patient {patient_name} presents with {symptom}. Recent outdoor activity at {location}. {lab_result}. {treatment}."


def generate_inspections(sites: list[dict]) -> list[dict]:
    """Generate ~2000 inspections for food establishments, schools, and industrial facilities."""
    inspections = []
    inspectable = [s for s in sites if s["site_type"] in (
        "food_establishment", "industrial_facility", "school", "clinic", "park"
    )]

    # Inspection frequency varies by site type:
    # food_establishment: 2-6/year (high risk get more)
    # industrial_facility: 3-8/year (permits require frequent checks)
    # school: 1-3/year (annual + follow-ups)
    # clinic: 1-2/year (health dept compliance)
    # park: 1-2/year (environmental/safety)
    for site in inspectable:
        if site["site_type"] == "food_establishment":
            n_inspections = random.choices([2, 3, 4, 5, 6], weights=[15, 25, 30, 20, 10])[0]
        elif site["site_type"] == "industrial_facility":
            n_inspections = random.choices([3, 4, 5, 6, 7, 8], weights=[10, 15, 25, 25, 15, 10])[0]
        elif site["site_type"] == "clinic":
            n_inspections = random.choices([1, 2, 3], weights=[40, 45, 15])[0]
        elif site["site_type"] == "park":
            n_inspections = random.choices([1, 2], weights=[60, 40])[0]
        else:  # school
            n_inspections = random.choices([1, 2, 3], weights=[30, 50, 20])[0]

        for i in range(n_inspections):
            dt = rand_date()
            inspector = random.choice(INSPECTOR_NAMES)

            if site["site_type"] == "food_establishment":
                inspection_type = random.choice(["routine", "routine", "routine", "follow_up", "complaint_driven"])
                score = random.choices(
                    [random.randint(90, 100), random.randint(75, 89), random.randint(60, 74), random.randint(40, 59)],
                    weights=[50, 30, 15, 5]
                )[0]
                violation_cat = "food_safety"
            elif site["site_type"] == "industrial_facility":
                inspection_type = random.choice(["scheduled", "unannounced", "follow_up", "complaint_driven", "permit_renewal"])
                score = random.choices(
                    [random.randint(85, 100), random.randint(70, 84), random.randint(50, 69), random.randint(30, 49)],
                    weights=[40, 35, 18, 7]
                )[0]
                violation_cat = "environmental"
            elif site["site_type"] == "clinic":
                inspection_type = random.choice(["compliance_audit", "routine", "follow_up"])
                score = random.choices(
                    [random.randint(85, 100), random.randint(70, 84), random.randint(55, 69)],
                    weights=[60, 30, 10]
                )[0]
                violation_cat = random.choice(["safety", "environmental"])
            elif site["site_type"] == "park":
                inspection_type = random.choice(["seasonal", "routine", "complaint_driven"])
                score = random.choices(
                    [random.randint(80, 100), random.randint(65, 79), random.randint(50, 64)],
                    weights=[55, 35, 10]
                )[0]
                violation_cat = "environmental"
            else:  # school
                inspection_type = random.choice(["annual", "follow_up", "complaint_driven"])
                score = random.choices(
                    [random.randint(80, 100), random.randint(65, 79), random.randint(50, 64)],
                    weights=[55, 35, 10]
                )[0]
                violation_cat = random.choice(["environmental", "safety"])

            # Generate violations
            n_violations = 0 if score >= 95 else (
                random.randint(1, 2) if score >= 85 else (
                    random.randint(2, 4) if score >= 70 else random.randint(3, 7)
                )
            )
            violations = []
            templates = VIOLATION_TYPES[violation_cat]
            for _ in range(n_violations):
                tmpl = random.choice(templates)
                violation_text = tmpl.format(
                    temp=random.randint(45, 65),
                    items=random.choice(["yogurt (exp 2024-01-15), deli meat (exp 2024-02-03)", "milk, eggs, sliced tomatoes"]),
                    ppm=random.randint(50, 150),
                    room=random.randint(101, 320),
                    location=random.choice(["Room 205 ceiling", "gymnasium wall", "basement storage", "kitchen exhaust duct"]),
                    opacity=random.randint(25, 55),
                    ph=round(random.uniform(4.5, 5.8), 1),
                    noise=random.randint(70, 95),
                    date=f"{random.randint(1,12)}/{YEAR-random.randint(1,3)}",
                )
                violations.append({
                    "code": f"V{random.randint(100,999)}",
                    "description": violation_text,
                    "severity": random.choice(["critical", "major", "minor"]),
                    "corrected_on_site": random.random() < 0.3,
                })

            # Inspector notes (free text)
            notes_templates = [
                f"Facility generally {'well-maintained' if score >= 80 else 'in need of attention'}. {f'{n_violations} violations noted.' if n_violations else 'No violations observed.'} {'Follow-up inspection scheduled.' if score < 75 else ''}",
                f"{'Cooperative management, corrections initiated during inspection.' if random.random() < 0.5 else 'Management not present during inspection; violations documented with on-site staff.'}",
                f"Third inspection this year. {'Improvement noted from previous visit.' if random.random() < 0.6 else 'Recurring violations from prior inspection remain uncorrected. Enforcement action recommended.'}",
            ]

            # Photos metadata (some inspections have photos)
            n_photos = random.choices([0, 0, 1, 2, 3], weights=[40, 20, 20, 15, 5])[0]
            photos = []
            for p in range(n_photos):
                photos.append({
                    "photo_id": uid(),
                    "filename": f"insp_{len(inspections)+1:04d}_photo_{p+1}.jpg",
                    "caption": random.choice([
                        "Violation documented", "Condition of food storage area",
                        "Corrective action in progress", "Overall facility condition",
                        "Equipment requiring maintenance", "Signage deficiency",
                    ]),
                    "taken_at": iso(dt + timedelta(minutes=random.randint(10, 90))),
                })

            inspection = {
                "inspection_id": f"INSP-{YEAR}-{len(inspections)+1:05d}",
                "site_id": site["site_id"],
                "inspection_type": inspection_type,
                "inspection_date": date_only(dt),
                "inspector_name": inspector,
                "score": score,
                "result": "pass" if score >= 70 else ("conditional_pass" if score >= 50 else "fail"),
                "violations": violations,
                "violation_count": n_violations,
                "critical_violations": sum(1 for v in violations if v["severity"] == "critical"),
                "notes": random.choice(notes_templates),
                "photos": photos,
                "follow_up_required": score < 75 or any(v["severity"] == "critical" for v in violations),
                "follow_up_date": date_only(dt + timedelta(days=random.randint(14, 45))) if score < 75 else None,
                "duration_minutes": random.randint(30, 180),
            }
            inspections.append(inspection)

    # Sort by date and trim to target
    inspections.sort(key=lambda x: x["inspection_date"])
    return inspections[:2500]


def generate_complaints(zones: list[dict], sites: list[dict]) -> list[dict]:
    """Generate ~3000 complaints with realistic PII-containing text."""
    complaints = []
    zone_lookup = {z["zone_id"]: z for z in zones}
    industrial_sites = [s for s in sites if s["site_type"] == "industrial_facility"]

    for _ in range(3000):
        # Choose category
        cat = random.choices(
            list(COMPLAINT_CATEGORIES.keys()),
            weights=[c["weight"] for c in COMPLAINT_CATEGORIES.values()]
        )[0]
        cat_spec = COMPLAINT_CATEGORIES[cat]

        # Bias toward high-risk zones
        zone_weights = []
        for z in zones:
            w = z["population"] / 5000
            if z["environmental_risk_score"] == "high":
                w *= 2.5
            zone_weights.append(w)
        zone = random.choices(zones, weights=zone_weights)[0]

        dt = rand_date()

        # Fill template
        name = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
        phone = f"({random.randint(200,999)}) {random.randint(200,999)}-{random.randint(1000,9999)}"
        email = f"{name.split()[0].lower()}.{name.split()[1].lower()}{random.randint(1,99)}@{random.choice(['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com'])}"
        address = f"{random.randint(100, 9999)} {random.choice(STREET_NAMES)}, {CITY_NAME}"
        street = random.choice(STREET_NAMES)
        facility = random.choice(industrial_sites)["name"] if industrial_sites else "the facility"
        school = f"{zone['name'].split()[0]} Elementary"
        time_str = f"{random.randint(5,11)}:{random.choice(['00','15','30','45'])} {'AM' if random.random()<0.5 else 'PM'}"
        location = random.choice([f"{zone['name']} park", "the riverbank near downtown", f"the creek behind {street}"])
        neighbor_address = f"{random.randint(100,9999)} {random.choice(STREET_NAMES)}, {CITY_NAME}"
        noise_db = random.randint(70, 95)
        year = random.randint(1935, 1975)

        template = random.choice(cat_spec["templates"])
        description = template.format(
            name=name, phone=phone, email=email, address=address,
            street=street, facility=facility, school=school,
            time=time_str, location=location, noise=noise_db,
            neighbor_address=neighbor_address, year=year,
        )

        # Response timeline
        response_days = random.choices(
            [random.randint(1, 3), random.randint(3, 7), random.randint(7, 21), random.randint(21, 60)],
            weights=[35, 35, 20, 10]
        )[0]

        status = random.choices(
            ["open", "investigating", "resolved", "closed_no_action", "referred"],
            weights=[10, 20, 45, 15, 10]
        )[0]

        # Intentional data quality issue: ~5% of complaints reference a non-existent zone
        reported_zone_id = zone["zone_id"]
        if random.random() < 0.05:
            reported_zone_id = f"Z{random.randint(26, 35)}"  # orphaned reference

        complaint = {
            "complaint_id": f"CMP-{YEAR}-{len(complaints)+1:05d}",
            "category": cat,
            "date_filed": date_only(dt),
            "date_acknowledged": date_only(dt + timedelta(days=random.randint(0, 2))),
            "zone_id": reported_zone_id,
            "reported_address": address if random.random() < 0.7 else None,  # some have no address
            "latitude": round(zone["centroid_lat"] + random.uniform(-0.008, 0.008), 6) if random.random() < 0.8 else None,
            "longitude": round(zone["centroid_lon"] + random.uniform(-0.008, 0.008), 6) if random.random() < 0.8 else None,
            "description": description,
            "source": random.choice(["phone", "online_form", "email", "in_person", "311_app"]),
            "priority": random.choice(["low", "medium", "medium", "high", "urgent"]),
            "assigned_to": random.choice(INSPECTOR_NAMES),
            "status": status,
            "resolution_date": date_only(dt + timedelta(days=response_days)) if status in ("resolved", "closed_no_action") else None,
            "resolution_notes": _build_resolution_notes(cat, status) if status in ("resolved", "closed_no_action") else None,
            "related_site_id": facility if cat in ("air_quality", "noise", "waste") and random.random() < 0.4 else None,
        }
        # Fix: related_site_id should be a site_id, not a name
        if complaint["related_site_id"] and not complaint["related_site_id"].startswith("S"):
            matching = [s for s in sites if s["name"] == complaint["related_site_id"]]
            complaint["related_site_id"] = matching[0]["site_id"] if matching else None

        complaints.append(complaint)

    return complaints


def _build_resolution_notes(category: str, status: str) -> str:
    if status == "closed_no_action":
        return random.choice([
            "Unable to verify reported condition upon site visit.",
            "Complaint withdrawn by reporter.",
            "Conditions within permitted limits at time of inspection.",
            "Duplicate complaint; merged with existing case.",
            "Outside department jurisdiction; referred to state EPA.",
        ])
    # resolved
    notes = {
        "air_quality": [
            "Facility issued notice of violation. Emission controls adjusted. Re-inspection confirmed compliance.",
            "Source identified as permitted agricultural burn. Within allowable dates and conditions.",
            "Dust suppression measures implemented at construction site. Water truck deployed.",
            "Stack test conducted; emissions within permit limits. Odor mitigation plan required within 30 days.",
        ],
        "water_quality": [
            "Water main flushing resolved discoloration. Follow-up testing confirmed safe levels.",
            "Discharge traced to illicit connection; disconnected. Penalty issued.",
            "Algae bloom treated; swimming advisory lifted after testing.",
            "Lead service line identified; resident enrolled in replacement program.",
        ],
        "noise": [
            "Construction hours violation confirmed. Citation issued. Contractor adjusted schedule.",
            "Noise monitoring conducted; levels exceed ordinance at property line. Abatement order issued.",
            "Mediation between facility and residents. Sound barrier installation agreed upon.",
        ],
        "waste": [
            "Illegal dump site cleaned by city crew. Property owner cited.",
            "Hazardous materials team dispatched; containers removed for proper disposal.",
            "Dumpster service increased to 3x/week. Enclosure installed.",
        ],
        "pest": [
            "Bait stations installed in alley. Property owners notified of sanitation requirements.",
            "Standing water sources eliminated. Larvicide applied to remaining areas.",
            "Health department issued notice to landlord; pest treatment completed.",
        ],
        "other": [
            "Environmental testing completed; results within acceptable limits.",
            "Referred to appropriate program for follow-up services.",
        ],
    }
    return random.choice(notes.get(category, notes["other"]))


def generate_programs() -> list[dict]:
    """Generate program records with enrollment and outcome data."""
    programs = []
    for i, pdef in enumerate(PROGRAM_DEFINITIONS):
        prog = {
            "program_id": f"PRG-{i+1:03d}",
            "name": pdef["name"],
            "type": pdef["type"],
            "description": pdef["description"],
            "target_zones": pdef["target_zones"],
            "start_date": pdef["start_date"],
            "end_date": None if pdef["type"] in ("awareness", "vector_control", "community_monitoring") else f"{YEAR}-12-31",
            "budget_usd": pdef["budget"],
            "funding_source": random.choice([
                "CDC Environmental Health Grant",
                "EPA Community Air Monitoring Grant",
                "State Department of Health",
                "HUD Lead Hazard Reduction Grant",
                "City General Fund",
                "FEMA Pre-Disaster Mitigation",
                "Private Foundation Grant",
            ]),
            "program_manager": f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}",
            "contact_email": f"{pdef['name'].lower().replace(' ', '_')[:20]}@riverton.gov",
            "capacity": pdef["capacity"],
            "enrolled": int(pdef["capacity"] * random.uniform(0.4, 0.95)) if pdef["capacity"] else None,
            "status": "active",
            "outcomes": _generate_program_outcomes(pdef),
        }
        programs.append(prog)
    return programs


def _generate_program_outcomes(pdef: dict) -> dict:
    """Generate realistic outcome metrics for a program."""
    ptype = pdef["type"]
    capacity = pdef["capacity"] or 1000

    if ptype == "asthma_management":
        return {
            "children_screened": int(capacity * random.uniform(0.7, 1.0)),
            "action_plans_created": int(capacity * random.uniform(0.5, 0.8)),
            "er_visits_avoided_estimate": random.randint(15, 45),
            "schools_participating": random.randint(8, 18),
            "air_monitors_installed_in_classrooms": random.randint(12, 30),
        }
    elif ptype == "lead_abatement":
        return {
            "homes_inspected": int(capacity * random.uniform(0.6, 0.9)),
            "homes_with_lead_found": int(capacity * random.uniform(0.3, 0.6)),
            "abatements_completed": int(capacity * random.uniform(0.2, 0.4)),
            "children_screened": int(capacity * random.uniform(0.8, 1.2)),
            "elevated_bll_cases": random.randint(8, 25),
        }
    elif ptype == "community_monitoring":
        return {
            "volunteers_trained": int(capacity * random.uniform(0.5, 0.9)),
            "sampling_events": random.randint(8, 12),
            "samples_collected": random.randint(200, 500),
            "exceedances_reported": random.randint(3, 12),
        }
    elif ptype == "heat_resilience":
        return {
            "cooling_center_visits": random.randint(800, 3000),
            "wellness_checks_conducted": random.randint(200, 600),
            "heat_alerts_issued": random.randint(8, 20),
            "hospitalizations_prevented_estimate": random.randint(10, 30),
        }
    elif ptype == "food_access":
        return {
            "stores_enrolled": int(capacity * random.uniform(0.6, 1.0)),
            "refrigeration_units_provided": random.randint(10, 25),
            "produce_variety_score_avg": round(random.uniform(3.5, 8.0), 1),
        }
    elif ptype == "community_engagement":
        return {
            "town_halls_held": random.randint(3, 6),
            "attendees_total": random.randint(150, 500),
            "complaints_fast_tracked": random.randint(20, 60),
            "facility_commitments": random.randint(5, 15),
        }
    elif ptype == "vector_control":
        return {
            "traps_monitored": random.randint(80, 200),
            "positive_pools": random.randint(2, 12),
            "acres_treated": random.randint(500, 2000),
            "wnv_cases_city": random.randint(0, 5),
        }
    elif ptype == "site_remediation":
        return {
            "phase_i_completed": random.randint(6, 10),
            "phase_ii_completed": random.randint(2, 6),
            "contaminants_found": random.choice([
                ["lead", "arsenic", "PAHs"],
                ["petroleum hydrocarbons", "heavy metals"],
                ["chlorinated solvents", "lead"],
                ["asbestos", "PCBs", "lead"],
            ]),
            "remediation_plans_approved": random.randint(1, 4),
        }
    else:
        return {
            "participants": int(capacity * random.uniform(0.5, 0.9)) if capacity else random.randint(50, 200),
            "sessions_completed": random.randint(20, 100),
        }


# ===========================================================================
# MAIN
# ===========================================================================

def write_ndjson(data: list[dict], filename: str) -> None:
    filepath = OUTPUT_DIR / filename
    with open(filepath, "w") as f:
        for record in data:
            f.write(json.dumps(record, default=str) + "\n")
    print(f"  {filename}: {len(data):,} records ({filepath.stat().st_size / 1024:.1f} KB)")


def main():
    print("=" * 60)
    print("ADM Synthetic Data Generator")
    print("Domain: Community Environmental Health - City of Riverton")
    print("=" * 60)
    print()

    print("Generating zones...")
    zones = generate_zones()

    print("Generating sites...")
    sites = generate_sites(zones)

    print("Generating sensors...")
    sensors = generate_sensors(sites)

    print("Generating readings (this may take a moment)...")
    readings = generate_readings(sensors, sites)

    print("Generating health incidents...")
    incidents = generate_incidents(zones, sites)

    print("Generating inspections...")
    inspections = generate_inspections(sites)

    print("Generating complaints...")
    complaints = generate_complaints(zones, sites)

    print("Generating programs...")
    programs = generate_programs()

    print()
    print("Writing NDJSON files...")
    write_ndjson(zones, "zones.json")
    write_ndjson(sites, "sites.json")
    write_ndjson(sensors, "sensors.json")
    write_ndjson(readings, "readings.json")
    write_ndjson(incidents, "incidents.json")
    write_ndjson(inspections, "inspections.json")
    write_ndjson(complaints, "complaints.json")
    write_ndjson(programs, "programs.json")

    print()
    print("=" * 60)
    print("Data generation complete!")
    print()
    print("Entity summary:")
    print(f"  Zones:        {len(zones):>6,}")
    print(f"  Sites:        {len(sites):>6,}")
    print(f"  Sensors:      {len(sensors):>6,}")
    print(f"  Readings:     {len(readings):>6,}")
    print(f"  Incidents:    {len(incidents):>6,}")
    print(f"  Inspections:  {len(inspections):>6,}")
    print(f"  Complaints:   {len(complaints):>6,}")
    print(f"  Programs:     {len(programs):>6,}")
    print()
    print("Relationships:")
    print("  sites.zone_id        -> zones.zone_id")
    print("  sensors.site_id      -> sites.site_id")
    print("  readings.sensor_id   -> sensors.sensor_id")
    print("  readings.site_id     -> sites.site_id")
    print("  incidents.zone_id    -> zones.zone_id")
    print("  incidents.reporting_facility_id -> sites.site_id")
    print("  inspections.site_id  -> sites.site_id")
    print("  complaints.zone_id   -> zones.zone_id  (5% orphaned!)")
    print("  complaints.related_site_id -> sites.site_id")
    print("  incidents.linked_program_id -> programs.program_id")
    print()
    print("Intentional data quality issues:")
    print("  - ~5% of complaints reference non-existent zone_ids (orphaned FKs)")
    print("  - ~0.5% of sensor readings have null values (sensor failures)")
    print("  - ~15% of sensors have data gaps (offline periods)")
    print("  - ~2% of readings flagged as suspect/calibration_drift")
    print("  - Some complaints missing lat/lon (~20%) or address (~30%)")
    print("  - PII present in complaint descriptions and incident narratives")
    print("  - Seasonal patterns: air quality degrades summer, respiratory peaks winter")
    print("  - Geographic correlation: industrial zones have worse readings + more incidents")
    print("=" * 60)


if __name__ == "__main__":
    main()

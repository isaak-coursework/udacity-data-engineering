CREATE TABLE "temperatures" (
    "state_code" TEXT,
    "city" TEXT,
    "month" INTEGER,
    "avg_temperature" REAL,
    "avg_temperature_uncertainty" REAL
);

CREATE TABLE "demographics" (
    "city" TEXT,
    "state" TEXT,
    "median_age" REAL,
    "male_population" REAL,
    "female_population" REAL,
    "total_population" INTEGER,
    "no_veterans" REAL,
    "is_foreign_born" REAL,
    "avg_household_size" REAL,
    "state_code" TEXT,
    "race" TEXT,
    "count" INTEGER
);

CREATE TABLE "airports" (
    "state_code" TEXT,
    "city" TEXT,
    "no_airports" INTEGER,
    "avg_elevation" REAL,
    "most_common_type" TEXT
);

CREATE TABLE "immigration_base" (
    "immigrant_id" INTEGER,
    "year" INTEGER,
    "month" INTEGER,
    "country_of_citizenship_code" INTEGER,
    "country_of_citizenship" TEXT,
    "country_of_residence_code" INTEGER,
    "country_of_residence" TEXT,
    "arrival_port_code" TEXT,
    "arrival_port" TEXT,
    "arrival_port_city" TEXT,
    "arrival_port_state" TEXT,
    "arrival_date" TIMESTAMP,
    "arrival_mode_code" INTEGER,
    "arrival_mode" TEXT,
    "departure_date" TIMESTAMP,
    "airline" TEXT,
    "flight_number" TEXT,
    "state_settled_code" TEXT,
    "state_settled" TEXT,
    "age" INTEGER,
    "birth_year" INTEGER,
    "visa_code" INTEGER,
    "visa_reason" TEXT,
    "visa_type" TEXT,
    "gender" TEXT
);
WITH summary_table AS (
  SELECT
    "Email",
    TO_DATE("Start Date", 'DD/MM/YYYY') AS "Start Date",
    "Duration (h)",
    "Workspace Id",
    CAST(SUBSTRING("Duration (h)", 1, POSITION(':' IN "Duration (h)") - 1) AS INT) AS hours,
    CAST(SUBSTRING("Duration (h)", POSITION(':' IN "Duration (h)") + 1, 2) AS INT) AS minutes
  FROM
    {{ref('src_hr_landing_clockify')}}
),
landing_table AS (
  SELECT
    "employee_email",
    "Start Date",
    "hours",
    "minutes",
    "hours" + ("minutes" / 60.0) AS total_hours,
    "Workspace Id"
  FROM (
    SELECT
      "Email" AS "employee_email",
      "Start Date",
      "hours",
      "minutes",
      "Workspace Id"
    FROM
      summary_table
  ) AS subquery
),
landing_fin AS (
  SELECT
    EXTRACT(YEAR FROM "Start Date") AS year,
    EXTRACT(MONTH FROM "Start Date") AS month,
    "employee_email",
    SUM("total_hours") AS total_hours_clock,
    "Workspace Id",
    'Clockify' AS source
  FROM
    landing_table
  GROUP BY
    EXTRACT(YEAR FROM "Start Date"),
    EXTRACT(MONTH FROM "Start Date"),
    "employee_email",
    "Workspace Id"
),
active_track_summary AS (
  SELECT
    "Email",
    CAST("Date" AS DATE) AS "Date",
    "Total (h:mm:ss)",
    CAST(SUBSTRING("Total (h:mm:ss)", 1, POSITION(':' IN "Total (h:mm:ss)") - 1) AS INT) AS hours,
    CAST(SUBSTRING("Total (h:mm:ss)", POSITION(':' IN "Total (h:mm:ss)") + 1, 2) AS INT) AS minutes
  FROM
    {{ref ('activetrack_hours') }}
),
active_track AS (
  SELECT
    "Email",
    "Date",
    "hours",
    "minutes",
    "hours" + ("minutes" / 60.0) AS "total_hours"
  FROM (
    SELECT
      "Email",
      "Date",
      "hours",
      "minutes"
    FROM
      active_track_summary
  ) AS subquery
),
active_fin AS (
  SELECT
    EXTRACT(YEAR FROM "Date") AS year,
    EXTRACT(MONTH FROM "Date") AS month,
    "Email" as Email ,
    SUM("total_hours") AS total_hours_active,
    'active' AS source
  FROM
    active_track
  GROUP BY
    EXTRACT(YEAR FROM "Date"),
    EXTRACT(MONTH FROM "Date"),
    "Email"
),
combine_table AS (
  SELECT
    lf.year,
    lf.month,
    lf.employee_email,
    lf.total_hours_clock,
    lf."Workspace Id",
    af.total_hours_active
  FROM
    landing_fin AS lf
  LEFT JOIN
    active_fin af ON lf.year = af.year AND lf.month = af.month AND lf.employee_email = af.Email
),
em_name AS (
    SELECT 
      lf.year,
      lf.month,
      lf.employee_email,
      lf.total_hours_clock,
      lf.total_hours_active,
      lf."Workspace Id",
      em."Name" ,
      em."Department",
      em."Status " 
    FROM
      combine_table AS lf
    LEFT JOIN 
      {{ ref('employeemaster') }}  AS em ON lf.employee_email = em."Email"
),
reporting_table AS (
  SELECT 
    lf.year,
    lf.month,
    lf.employee_email AS email,
    lf."Name" ,
    lf."Status " ,
    lf."Department",
    lf.total_hours_clock AS "clockify hours",
    lf.total_hours_active AS "activetrack hours",
    lf."Workspace Id"   
  FROM 
    em_name AS lf
) 
SELECT  * FROM reporting_table





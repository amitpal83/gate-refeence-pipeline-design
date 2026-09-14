{{
  config(
    materialized='table',
    tags=['silver', 'project_monitoring']
  )
}}

select * from (
    values
        ('NCR', 'National Capital Region'),
        ('CAR', 'Cordillera Administrative Region'),
        ('R1', 'Ilocos Region'),
        ('R2', 'Cagayan Valley'),
        ('R3', 'Central Luzon'),
        ('R4A', 'CALABARZON'),
        ('R4B', 'MIMAROPA'),
        ('R5', 'Bicol Region'),
        ('R6', 'Western Visayas'),
        ('R7', 'Central Visayas'),
        ('R8', 'Eastern Visayas'),
        ('R9', 'Zamboanga Peninsula'),
        ('R10', 'Northern Mindanao'),
        ('R11', 'Davao Region'),
        ('R12', 'SOCCSKSARGEN'),
        ('R13', 'Caraga'),
        ('NIR', 'Negros Island Region')
) as regions(region_code, region_name)

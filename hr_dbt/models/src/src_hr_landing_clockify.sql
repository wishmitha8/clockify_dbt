with

{{ this.name }} as (

                    select * from {{ source('ldg_hr_clockify','landing_clockify') }}
)

select * from {{ this.name}}
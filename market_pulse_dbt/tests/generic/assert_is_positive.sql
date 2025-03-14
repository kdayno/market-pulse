{% test assert_is_positive(model, column_name) %}

with validation as (

    select
        {{ column_name }} as positive_field

    from {{ model }}

),

validation_errors as (

    select
        positive_field

    from validation
    where positive_field < 0  -- if this is true, then positive_field is actually negative

)

select *
from validation_errors

{% endtest %}
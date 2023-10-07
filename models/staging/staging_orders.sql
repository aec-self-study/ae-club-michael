with final as (
    
    select * 
    from {{ source('analytics-engineers-club','orders') }}

)

select * from final
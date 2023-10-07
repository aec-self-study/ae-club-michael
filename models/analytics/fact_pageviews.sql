with final as (

    select *
    from {{ref('staging_pageviews')}}
)

select * from final
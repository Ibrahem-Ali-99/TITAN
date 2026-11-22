select 'blocked' as failure
where '{{ env_var("TITAN_DBT_GATE", "pass") }}' = 'fail'

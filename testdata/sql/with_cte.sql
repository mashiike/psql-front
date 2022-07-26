with users as (
    select date_trunc('day', access_at) as ymd, user_id
    from "access"."log"
    group by ymd, user_id
), weeks as (
    select week_start_at, ymd
    from "calender"
)

select week_start_at --, count(distinct user_id)
from weeks
join users using(ymd)
group by 1

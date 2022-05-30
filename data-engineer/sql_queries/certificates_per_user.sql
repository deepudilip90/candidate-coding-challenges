select cert.user as user_id, usr.email as user_email, count(distinct course) as distinct_certificates
from certificates cert left join users usr on usr.id = cert.user
group by 1,2
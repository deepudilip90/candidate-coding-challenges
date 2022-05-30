select cert.user as user_id, usr.email as user_email, cert.course as course_id, cou.title as course_title, datediff(completedDate, startDate) as days_to_complete
from certificates cert left join courses cou on cou.id = cert.course 
left join users usr on usr.id = cert.user
order by days_to_complete desc
limit 5 ;
select cou.id as course_id, cou.title as course_title, avg(datediff(completedDate, startDate)) as avg_days_to_complete
from certificates cert left join courses cou on cou.id = cert.course 
group by cou.id, cou.title;
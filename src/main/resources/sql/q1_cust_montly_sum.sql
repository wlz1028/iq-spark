select cust_id, substring(trans_date,1,6) as year_month, sum(trans_amt) as amount
from trans
group by cust_id, substring(trans_date,1,6)

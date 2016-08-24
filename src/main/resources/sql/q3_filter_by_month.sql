SELECT *
FROM trans
where substring(trans_date, 1, 6)=${yearmonth}
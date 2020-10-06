#!/usr/bin/env python
# coding: utf-8

# In[36]:


#!/usr/bin/env python
# coding: utf-8

# In[40]:


#Simple python script for creating a table in postgreSQL

import pandas as pd
from pandas import DataFrame
import os as os
import json
import gspread
import oauth2client
import csv
import datetime
import pymysql
from psycopg2 import IntegrityError
import psycopg2
from oauth2client.service_account import ServiceAccountCredentials

os.chdir(r"C:\Users\Admin\Desktop\My Tree\BackUp_Data")
user_ = input("User name :")
password_ = input("password :")
port_ = input("Port : ")
database_ = input("Database :")
host_ = input("Host name: ")

conn2_ = psycopg2.connect(user = user_, 
                         password = password_,
                         host = host_,
                         port = port_,
                         database = databse_
                        )
x1 = conn2.cursor()

sql1 = """
-- the Master Query
-- Need to Add :
/*
Select 'Product_Growth_Job_Posts' as Data_cut, jp.city as City_, "Today"  as Data_As_Of ,date(ed.created_at) as Created_at,
count(distinct(jp.job_post_id)) as Value
from api_jobpost jp
join api_employerdetail as ed on jp.user_id = ed.user_id
where jp.user_id in (select distinct wp.employer_detail_id from api_employerdetailwalletpassbook wp
where wp.transaction_source like '%recipient_wal_share_earn_1000%'
and date(wp.created_at) between @Start and @End	)
*/

select 'Overall_Job_Posts' as Data_cut,case
    when jp.city = 'mumbai' then 'Mumbai'
    when jp.city = 'delhi' then 'Delhi'
    when jp.city = 'pune' then 'Pune'
    when jp.city = 'bengaluru' then 'Bengaluru'
    when jp.city = 'hyderabad' then 'Hyderabad'
    when jp.city = 'ahmedabad' then 'Ahmedabad'
    when jp.city = 'kolkata' then 'Kolkata'
    when jp.city = 'chennai' then 'Chennai'
    when jp.city = 'lucknow' then 'Lucknow'
    when jp.city = 'jaipur' then 'Jaipur'
    when jp.city = 'surat' then 'Surat'
    when jp.city = 'indore' then 'Indore'
    when jp.city = 'nagpur' then 'Nagpur'
    when jp.city = 'chandigarh' then 'Chandigarh'
    when jp.city = 'vadodara' then 'Vadodara'
    when jp.city = 'patna' then 'Patna'
    when jp.city = 'bhopal' then 'Bhopal'
    when jp.city = 'nashik' then 'Nashik'
    when jp.city = 'kanpur' then 'Kanpur'
    when jp.city = 'varanasi' then 'Varanasi'
    when jp.city = 'bhubaneswar' then 'Bhubaneswar'
    when jp.city = 'dehradun' then 'Dehradun'
    when jp.city = 'coimbatore' then 'Coimbatore'
    when jp.city = 'ludhiana' then 'Ludhiana'
    when jp.city = 'mohali' then 'Mohali'
    else 'other'
end City_, 'Today'  as Data_As_Of ,date(jp.timestamp) as Created_at,
count(distinct(jp.job_post_id)) as Value_
from wi_employer_registration as ed
join wi_job_posting as jp on ed.employer_id = jp.user_id
where (jp.timestamp)  between date(getdate()) ||' 00:00:00' and date(getdate()) ||' 23:59:59'
group by Data_cut,City_,Data_As_Of,Created_at

union

select 'FB_Job_Posts' as Data_cut,case
    when jp.city = 'mumbai' then 'Mumbai'
    when jp.city = 'delhi' then 'Delhi'
    when jp.city = 'pune' then 'Pune'
    when jp.city = 'bengaluru' then 'Bengaluru'
    when jp.city = 'hyderabad' then 'Hyderabad'
    when jp.city = 'ahmedabad' then 'Ahmedabad'
    when jp.city = 'kolkata' then 'Kolkata'
    when jp.city = 'chennai' then 'Chennai'
    when jp.city = 'lucknow' then 'Lucknow'
    when jp.city = 'jaipur' then 'Jaipur'
    when jp.city = 'surat' then 'Surat'
    when jp.city = 'indore' then 'Indore'
    when jp.city = 'nagpur' then 'Nagpur'
    when jp.city = 'chandigarh' then 'Chandigarh'
    when jp.city = 'vadodara' then 'Vadodara'
    when jp.city = 'patna' then 'Patna'
    when jp.city = 'bhopal' then 'Bhopal'
    when jp.city = 'nashik' then 'Nashik'
    when jp.city = 'kanpur' then 'Kanpur'
    when jp.city = 'varanasi' then 'Varanasi'
    when jp.city = 'bhubaneswar' then 'Bhubaneswar'
    when jp.city = 'dehradun' then 'Dehradun'
    when jp.city = 'coimbatore' then 'Coimbatore'
    when jp.city = 'ludhiana' then 'Ludhiana'
    when jp.city = 'mohali' then 'Mohali'
    else 'other'
end City_, 'Today'  as Data_As_Of ,date(jp.timestamp) as Created_at,
count(distinct(jp.job_post_id)) as Value_
from wi_employer_registration as ed
join wi_job_posting as jp on ed.employer_id = jp.user_id
where (referrer like '%fb %' OR referrer like '%=fb%' OR referrer like '%=Fb%')
and (jp.timestamp)  between date(getdate()) ||' 00:00:00' and date(getdate()) ||' 23:59:59'
group by Data_cut,City_,Data_As_Of,Created_at

union

select 'Google_Job_Posts' as Data_cut,case
    when jp.city = 'mumbai' then 'Mumbai'
    when jp.city = 'delhi' then 'Delhi'
    when jp.city = 'pune' then 'Pune'
    when jp.city = 'bengaluru' then 'Bengaluru'
    when jp.city = 'hyderabad' then 'Hyderabad'
    when jp.city = 'ahmedabad' then 'Ahmedabad'
    when jp.city = 'kolkata' then 'Kolkata'
    when jp.city = 'chennai' then 'Chennai'
    when jp.city = 'lucknow' then 'Lucknow'
    when jp.city = 'jaipur' then 'Jaipur'
    when jp.city = 'surat' then 'Surat'
    when jp.city = 'indore' then 'Indore'
    when jp.city = 'nagpur' then 'Nagpur'
    when jp.city = 'chandigarh' then 'Chandigarh'
    when jp.city = 'vadodara' then 'Vadodara'
    when jp.city = 'patna' then 'Patna'
    when jp.city = 'bhopal' then 'Bhopal'
    when jp.city = 'nashik' then 'Nashik'
    when jp.city = 'kanpur' then 'Kanpur'
    when jp.city = 'varanasi' then 'Varanasi'
    when jp.city = 'bhubaneswar' then 'Bhubaneswar'
    when jp.city = 'dehradun' then 'Dehradun'
    when jp.city = 'coimbatore' then 'Coimbatore'
    when jp.city = 'ludhiana' then 'Ludhiana'
    when jp.city = 'mohali' then 'Mohali'
    else 'other'
end City_, 'Today'  as Data_As_Of ,date(jp.timestamp) as Created_at,
count(distinct(jp.job_post_id)) as Value_
from wi_employer_registration as ed
join wi_job_posting as jp on ed.employer_id = jp.user_id
where (referrer like '%SEM%' OR referrer like '%gclid%' OR referrer like '%Google%' OR referrer like '%google_ads%' OR referrer like '%google%')
and referrer not like '%youtube%'
and referrer not like '%google.com%'
and referrer not like '%google.co.in%'
and (jp.timestamp)  between date(getdate()) ||' 00:00:00' and date(getdate()) ||' 23:59:59'
group by Data_cut,City_,Data_As_Of,Created_at

union

select 'Product_Growth_Job_Posts' as Data_cut,case
    when jp.city = 'mumbai' then 'Mumbai'
    when jp.city = 'delhi' then 'Delhi'
    when jp.city = 'pune' then 'Pune'
    when jp.city = 'bengaluru' then 'Bengaluru'
    when jp.city = 'hyderabad' then 'Hyderabad'
    when jp.city = 'ahmedabad' then 'Ahmedabad'
    when jp.city = 'kolkata' then 'Kolkata'
    when jp.city = 'chennai' then 'Chennai'
    when jp.city = 'lucknow' then 'Lucknow'
    when jp.city = 'jaipur' then 'Jaipur'
    when jp.city = 'surat' then 'Surat'
    when jp.city = 'indore' then 'Indore'
    when jp.city = 'nagpur' then 'Nagpur'
    when jp.city = 'chandigarh' then 'Chandigarh'
    when jp.city = 'vadodara' then 'Vadodara'
    when jp.city = 'patna' then 'Patna'
    when jp.city = 'bhopal' then 'Bhopal'
    when jp.city = 'nashik' then 'Nashik'
    when jp.city = 'kanpur' then 'Kanpur'
    when jp.city = 'varanasi' then 'Varanasi'
    when jp.city = 'bhubaneswar' then 'Bhubaneswar'
    when jp.city = 'dehradun' then 'Dehradun'
    when jp.city = 'coimbatore' then 'Coimbatore'
    when jp.city = 'ludhiana' then 'Ludhiana'
    when jp.city = 'mohali' then 'Mohali'
    else 'other'
end City_, 'Today'  as Data_As_Of ,date(jp.timestamp) as Created_at,
count(distinct(jp.job_post_id)) as Value_
from wi_employer_registration as ed
join wi_job_posting as jp on ed.employer_id = jp.user_id
where (jp.referrer like '%form_lead_retarget%'
or jp.referrer like '%sms_exp_1%'
or jp.referrer like '%sms_exp_2%'
or jp.referrer like '%sms_exp_3%'
or jp.referrer like '%sms_exp_4%'
or jp.referrer like '%sms_exp%'
or jp.referrer like '%sms_exp_5%'
or jp.referrer like '%sms_exp_6%'
or jp.referrer like '%sms_exp_7%'
or jp.referrer like '%sms_exp_8%'
or jp.referrer like '%sms_exp_9%'
or jp.referrer like '%sms_exp_10%'
or jp.referrer like '%sms_exp_11%'
or jp.referrer like '%MR_employer_track%'
or jp.referrer like '%drip_employer_track%'
or jp.referrer like '%drip_drip%'
or jp.referrer like '%vibe%'
or jp.referrer like '%vmayo%'
or jp.referrer like '%form_lead_retarget%'
or jp.referrer like '%Form_lead_past_retarget%'
or jp.referrer like '%cand_app_login%'
or jp.referrer like '%Mag-EMP11%'
or jp.referrer like '%pg_emp_dh%'
or jp.referrer like '%pg_emp_uc%'
or jp.referrer like '%pg_emp_quora%'
or jp.referrer like '%pg_emp_colombia%'
or jp.referrer like '%pg_emp_ta%'
or jp.referrer like '%pg_emp_ag%'
or jp.referrer like '%pg_emp_pm%'
or jp.referrer like '%pg_emp_vl1%'
or jp.referrer like '%pg_emp_vl2%'
or jp.referrer like '%pg_emp_fb%'
or jp.referrer like '%pg_emp_insta%'
or jp.referrer like '%pg_emp_quora%'
or jp.referrer like '%pg_emp_twitter%'
or jp.referrer like '%pg_emp_linkedin%'
or jp.referrer like '%graphic%'
or jp.referrer like '%pg_emp_mail1%'
or jp.referrer like '%pg_emp_mail2%'
or jp.referrer like '%pg_emp_linkedin2%'
or jp.referrer like '%pg_emp_hs%'
or jp.referrer like '%pg_emp_exp%'
)
and (jp.timestamp)  between date(getdate()) ||' 00:00:00' and date(getdate()) ||' 23:59:59'
group by Data_cut,City_,Data_As_Of,Created_at

union

select 'Tick_Tok_Job_Posts' as Data_cut,case
    when jp.city = 'mumbai' then 'Mumbai'
    when jp.city = 'delhi' then 'Delhi'
    when jp.city = 'pune' then 'Pune'
    when jp.city = 'bengaluru' then 'Bengaluru'
    when jp.city = 'hyderabad' then 'Hyderabad'
    when jp.city = 'ahmedabad' then 'Ahmedabad'
    when jp.city = 'kolkata' then 'Kolkata'
    when jp.city = 'chennai' then 'Chennai'
    when jp.city = 'lucknow' then 'Lucknow'
    when jp.city = 'jaipur' then 'Jaipur'
    when jp.city = 'surat' then 'Surat'
    when jp.city = 'indore' then 'Indore'
    when jp.city = 'nagpur' then 'Nagpur'
    when jp.city = 'chandigarh' then 'Chandigarh'
    when jp.city = 'vadodara' then 'Vadodara'
    when jp.city = 'patna' then 'Patna'
    when jp.city = 'bhopal' then 'Bhopal'
    when jp.city = 'nashik' then 'Nashik'
    when jp.city = 'kanpur' then 'Kanpur'
    when jp.city = 'varanasi' then 'Varanasi'
    when jp.city = 'bhubaneswar' then 'Bhubaneswar'
    when jp.city = 'dehradun' then 'Dehradun'
    when jp.city = 'coimbatore' then 'Coimbatore'
    when jp.city = 'ludhiana' then 'Ludhiana'
    when jp.city = 'mohali' then 'Mohali'
    else 'other'
end City_, 'Today'  as Data_As_Of, date(jp.timestamp) as Created_at,
count(distinct(jp.job_post_id)) as Value_
from wi_employer_registration as ed
join wi_job_posting as jp on ed.employer_id = jp.user_id
where (jp.referrer like '%pg_emp_tt%')
and (jp.timestamp)  between date(getdate()) ||' 00:00:00' and date(getdate()) ||' 23:59:59'
group by Data_cut,City_,Data_As_Of,Created_at

union

select 'Overall_New_Users' as Data_cut,case
    when jp.city = 'mumbai' then 'Mumbai'
    when jp.city = 'delhi' then 'Delhi'
    when jp.city = 'pune' then 'Pune'
    when jp.city = 'bengaluru' then 'Bengaluru'
    when jp.city = 'hyderabad' then 'Hyderabad'
    when jp.city = 'ahmedabad' then 'Ahmedabad'
    when jp.city = 'kolkata' then 'Kolkata'
    when jp.city = 'chennai' then 'Chennai'
    when jp.city = 'lucknow' then 'Lucknow'
    when jp.city = 'jaipur' then 'Jaipur'
    when jp.city = 'surat' then 'Surat'
    when jp.city = 'indore' then 'Indore'
    when jp.city = 'nagpur' then 'Nagpur'
    when jp.city = 'chandigarh' then 'Chandigarh'
    when jp.city = 'vadodara' then 'Vadodara'
    when jp.city = 'patna' then 'Patna'
    when jp.city = 'bhopal' then 'Bhopal'
    when jp.city = 'nashik' then 'Nashik'
    when jp.city = 'kanpur' then 'Kanpur'
    when jp.city = 'varanasi' then 'Varanasi'
    when jp.city = 'bhubaneswar' then 'Bhubaneswar'
    when jp.city = 'dehradun' then 'Dehradun'
    when jp.city = 'coimbatore' then 'Coimbatore'
    when jp.city = 'ludhiana' then 'Ludhiana'
    when jp.city = 'mohali' then 'Mohali'
    else 'other'
end City_, 'Today'  as Data_As_Of ,date(ed.timestamp) as Created_at,
count(distinct(jp.user_id)) as Value_
from wi_employer_registration as ed
join wi_job_posting as jp on ed.employer_id = jp.user_id
where (ed.timestamp) between date(getdate()) ||' 00:00:00' and date(getdate()) ||' 23:59:59'
and (jp.timestamp)  between date(getdate()) ||' 00:00:00' and date(getdate()) ||' 23:59:59'
group by Data_cut,City_,Data_As_Of,Created_at

union

select 'FB_New_Users' as Data_cut,case
    when jp.city = 'mumbai' then 'Mumbai'
    when jp.city = 'delhi' then 'Delhi'
    when jp.city = 'pune' then 'Pune'
    when jp.city = 'bengaluru' then 'Bengaluru'
    when jp.city = 'hyderabad' then 'Hyderabad'
    when jp.city = 'ahmedabad' then 'Ahmedabad'
    when jp.city = 'kolkata' then 'Kolkata'
    when jp.city = 'chennai' then 'Chennai'
    when jp.city = 'lucknow' then 'Lucknow'
    when jp.city = 'jaipur' then 'Jaipur'
    when jp.city = 'surat' then 'Surat'
    when jp.city = 'indore' then 'Indore'
    when jp.city = 'nagpur' then 'Nagpur'
    when jp.city = 'chandigarh' then 'Chandigarh'
    when jp.city = 'vadodara' then 'Vadodara'
    when jp.city = 'patna' then 'Patna'
    when jp.city = 'bhopal' then 'Bhopal'
    when jp.city = 'nashik' then 'Nashik'
    when jp.city = 'kanpur' then 'Kanpur'
    when jp.city = 'varanasi' then 'Varanasi'
    when jp.city = 'bhubaneswar' then 'Bhubaneswar'
    when jp.city = 'dehradun' then 'Dehradun'
    when jp.city = 'coimbatore' then 'Coimbatore'
    when jp.city = 'ludhiana' then 'Ludhiana'
    when jp.city = 'mohali' then 'Mohali'
    else 'other'
end City_, 'Today'  as Data_As_Of ,date(ed.timestamp) as Created_at,
count(distinct(jp.user_id)) as Value_
from wi_employer_registration as ed
join wi_job_posting as jp on ed.employer_id = jp.user_id
where (referrer like '%fb %' OR referrer like '%=fb%' OR referrer like '%=Fb%')
and (ed.timestamp) between date(getdate()) ||' 00:00:00' and date(getdate()) ||' 23:59:59'
and (jp.timestamp)  between date(getdate()) ||' 00:00:00' and date(getdate()) ||' 23:59:59'
group by Data_cut,City_,Data_As_Of,Created_at

union

select 'Google_New_Users' as Data_cut,case
    when jp.city = 'mumbai' then 'Mumbai'
    when jp.city = 'delhi' then 'Delhi'
    when jp.city = 'pune' then 'Pune'
    when jp.city = 'bengaluru' then 'Bengaluru'
    when jp.city = 'hyderabad' then 'Hyderabad'
    when jp.city = 'ahmedabad' then 'Ahmedabad'
    when jp.city = 'kolkata' then 'Kolkata'
    when jp.city = 'chennai' then 'Chennai'
    when jp.city = 'lucknow' then 'Lucknow'
    when jp.city = 'jaipur' then 'Jaipur'
    when jp.city = 'surat' then 'Surat'
    when jp.city = 'indore' then 'Indore'
    when jp.city = 'nagpur' then 'Nagpur'
    when jp.city = 'chandigarh' then 'Chandigarh'
    when jp.city = 'vadodara' then 'Vadodara'
    when jp.city = 'patna' then 'Patna'
    when jp.city = 'bhopal' then 'Bhopal'
    when jp.city = 'nashik' then 'Nashik'
    when jp.city = 'kanpur' then 'Kanpur'
    when jp.city = 'varanasi' then 'Varanasi'
    when jp.city = 'bhubaneswar' then 'Bhubaneswar'
    when jp.city = 'dehradun' then 'Dehradun'
    when jp.city = 'coimbatore' then 'Coimbatore'
    when jp.city = 'ludhiana' then 'Ludhiana'
    when jp.city = 'mohali' then 'Mohali'
    else 'other'
end City_, 'Today'  as Data_As_Of ,date(ed.timestamp) as Created_at,
count(distinct(jp.user_id)) as Value_
from wi_employer_registration as ed
join wi_job_posting as jp on ed.employer_id = jp.user_id
where (referrer like '%SEM%' OR referrer like '%gclid%' OR referrer like '%Google%' OR referrer like '%google_ads%' OR referrer like '%google%')
and referrer not like '%youtube%'
and referrer not like '%google.com%'
and referrer not like '%google.co.in%'
and (ed.timestamp) between date(getdate()) ||' 00:00:00' and date(getdate()) ||' 23:59:59'
and (jp.timestamp)  between date(getdate()) ||' 00:00:00' and date(getdate()) ||' 23:59:59'
group by Data_cut,City_,Data_As_Of,Created_at

union

select 'Product_Growth_New_Users' as Data_cut,case
    when jp.city = 'mumbai' then 'Mumbai'
    when jp.city = 'delhi' then 'Delhi'
    when jp.city = 'pune' then 'Pune'
    when jp.city = 'bengaluru' then 'Bengaluru'
    when jp.city = 'hyderabad' then 'Hyderabad'
    when jp.city = 'ahmedabad' then 'Ahmedabad'
    when jp.city = 'kolkata' then 'Kolkata'
    when jp.city = 'chennai' then 'Chennai'
    when jp.city = 'lucknow' then 'Lucknow'
    when jp.city = 'jaipur' then 'Jaipur'
    when jp.city = 'surat' then 'Surat'
    when jp.city = 'indore' then 'Indore'
    when jp.city = 'nagpur' then 'Nagpur'
    when jp.city = 'chandigarh' then 'Chandigarh'
    when jp.city = 'vadodara' then 'Vadodara'
    when jp.city = 'patna' then 'Patna'
    when jp.city = 'bhopal' then 'Bhopal'
    when jp.city = 'nashik' then 'Nashik'
    when jp.city = 'kanpur' then 'Kanpur'
    when jp.city = 'varanasi' then 'Varanasi'
    when jp.city = 'bhubaneswar' then 'Bhubaneswar'
    when jp.city = 'dehradun' then 'Dehradun'
    when jp.city = 'coimbatore' then 'Coimbatore'
    when jp.city = 'ludhiana' then 'Ludhiana'
    when jp.city = 'mohali' then 'Mohali'
    else 'other'
end City_, 'Today'  as Data_As_Of ,date(ed.timestamp) as Created_at,
count(distinct(jp.user_id)) as Value_
from wi_employer_registration as ed
join wi_job_posting as jp on ed.employer_id = jp.user_id
where (jp.referrer like '%form_lead_retarget%'
or jp.referrer like '%sms_exp_1%'
or jp.referrer like '%sms_exp_2%'
or jp.referrer like '%sms_exp_3%'
or jp.referrer like '%sms_exp_4%'
or jp.referrer like '%sms_exp%'
or jp.referrer like '%sms_exp_5%'
or jp.referrer like '%sms_exp_6%'
or jp.referrer like '%sms_exp_7%'
or jp.referrer like '%sms_exp_8%'
or jp.referrer like '%sms_exp_9%'
or jp.referrer like '%sms_exp_10%'
or jp.referrer like '%sms_exp_11%'
or jp.referrer like '%MR_employer_track%'
or jp.referrer like '%drip_employer_track%'
or jp.referrer like '%drip_drip%'
or jp.referrer like '%vibe%'
or jp.referrer like '%vmayo%'
or jp.referrer like '%form_lead_retarget%'
or jp.referrer like '%Form_lead_past_retarget%'
or jp.referrer like '%cand_app_login%'
or jp.referrer like '%Mag-EMP11%'
or jp.referrer like '%pg_emp_dh%'
or jp.referrer like '%pg_emp_uc%'
or jp.referrer like '%pg_emp_quora%'
or jp.referrer like '%pg_emp_colombia%'
or jp.referrer like '%pg_emp_ta%'
or jp.referrer like '%pg_emp_ag%'
or jp.referrer like '%pg_emp_pm%'
or jp.referrer like '%pg_emp_vl1%'
or jp.referrer like '%pg_emp_vl2%'
or jp.referrer like '%pg_emp_fb%'
or jp.referrer like '%pg_emp_insta%'
or jp.referrer like '%pg_emp_quora%'
or jp.referrer like '%pg_emp_twitter%'
or jp.referrer like '%pg_emp_linkedin%'
or jp.referrer like '%graphic%'
or jp.referrer like '%pg_emp_mail1%'
or jp.referrer like '%pg_emp_mail2%'
or jp.referrer like '%pg_emp_linkedin2%'
or jp.referrer like '%pg_emp_hs%'
or jp.referrer like '%pg_emp_exp%'
)
and (ed.timestamp) between date(getdate()) ||' 00:00:00' and date(getdate()) ||' 23:59:59'
and (jp.timestamp)  between date(getdate()) ||' 00:00:00' and date(getdate()) ||' 23:59:59'
group by Data_cut,City_,Data_As_Of,Created_at

union

select 'Tick_Tok_New_Users' as Data_cut,case
    when jp.city = 'mumbai' then 'Mumbai'
    when jp.city = 'delhi' then 'Delhi'
    when jp.city = 'pune' then 'Pune'
    when jp.city = 'bengaluru' then 'Bengaluru'
    when jp.city = 'hyderabad' then 'Hyderabad'
    when jp.city = 'ahmedabad' then 'Ahmedabad'
    when jp.city = 'kolkata' then 'Kolkata'
    when jp.city = 'chennai' then 'Chennai'
    when jp.city = 'lucknow' then 'Lucknow'
    when jp.city = 'jaipur' then 'Jaipur'
    when jp.city = 'surat' then 'Surat'
    when jp.city = 'indore' then 'Indore'
    when jp.city = 'nagpur' then 'Nagpur'
    when jp.city = 'chandigarh' then 'Chandigarh'
    when jp.city = 'vadodara' then 'Vadodara'
    when jp.city = 'patna' then 'Patna'
    when jp.city = 'bhopal' then 'Bhopal'
    when jp.city = 'nashik' then 'Nashik'
    when jp.city = 'kanpur' then 'Kanpur'
    when jp.city = 'varanasi' then 'Varanasi'
    when jp.city = 'bhubaneswar' then 'Bhubaneswar'
    when jp.city = 'dehradun' then 'Dehradun'
    when jp.city = 'coimbatore' then 'Coimbatore'
    when jp.city = 'ludhiana' then 'Ludhiana'
    when jp.city = 'mohali' then 'Mohali'
    else 'other'
end City_, 'Today'  as Data_As_Of ,date(ed.timestamp) as Created_at,
count(distinct(jp.user_id)) as Value_
from wi_employer_registration as ed
join wi_job_posting as jp on ed.employer_id = jp.user_id
where (jp.referrer like '%pg_emp_tt%')
and (ed.timestamp) between date(getdate()) ||' 00:00:00' and date(getdate()) ||' 23:59:59'
and (jp.timestamp)  between date(getdate()) ||' 00:00:00' and date(getdate()) ||' 23:59:59'
group by Data_cut,City_,Data_As_Of,Created_at


"""
print("<-------------------Start--------------->")
x1.execute(sql1)
conn2.commit()
#print("The Data Type is :" , type(data1))
df = DataFrame(x1.fetchall())
df.columns = ['data_cut','city_','data_as_of','created_at','value_']
df.to_csv('Output_Final_MMDB.csv', index=False, header=True)
print("Data Fetched From SQL:")
#print(df)
from datetime import datetime, date, time

scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('workindia-dashboards-721a8c43109c.json', scope)
client = gspread.authorize(creds)
print("Connected to Google sheets")
sheet = client.open("Marketing_MasterDashboard_v1")
worksheet = sheet.worksheet('New_Users_Job_Post_Raw_Data')

########################################## Read The master Dump File
df2 = pd.read_csv("Output_Final_MMDB.csv")
df = pd.read_csv("The_Data_Dump_MMDB.csv")
print("Master & Output, both File Read")
########################################## Read The master Dump File

today = datetime.combine(date.today(), time())
print("Before changing the formate")
#print(df)

print("Converting string dates from csv to datetime for comparison")
try:
    df['created_at'] = pd.to_datetime(df['created_at'], format="%d-%m-%Y")
except ValueError:
    df['created_at'] = pd.to_datetime(df['created_at'])

print("After changing the formate")
#print(df)

df_old_date =  df[df['created_at'] < today]

df_old_date['created_at'] = df_old_date['created_at'].dt.strftime('%Y-%m-%d')

print("Marketing Dump, after date format adjustment")
out = df_old_date.append(df2)
#out = pd.concat([df_old_date, df2], sort=False)
out.to_csv("appended_data_MMDB.csv", index=False)
#print(out)

print("Index Removed")
data_list = []
print("Data List Created")
df3 = pd.read_csv("appended_data_MMDB.csv")
headers = list(out)
data_with_headers = [headers] + out.values.tolist()
#print(data_with_headers[:10])
df3.to_csv("The_Data_Dump_MMDB.csv", index = False)
worksheet.clear()
sheet.values_append("'New_Users_Job_Post_Raw_Data'!A:E", {'valueInputOption': 'USER_ENTERED'}, {"values": data_with_headers})
sheet.values_append("'New_Users_Job_Post_Raw_Data'!K:K", {'valueInputOption': 'USER_ENTERED'}, {"values": [[str(datetime.now())]]})
os.remove("appended_data_MMDB.csv")
os.remove("Output_Final_MMDB.csv")
print(today)
print("Data Pasted")
print("<-------------------End--------------->")


# In[ ]:





# In[32]:





# In[34]:





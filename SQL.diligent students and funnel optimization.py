#!/usr/bin/env python
# coding: utf-8

# # 1. Очень усердные ученики

# # Образовательные курсы состоят из различных уроков, каждый из которых состоит из нескольких маленьких заданий. Каждое такое маленькое задание называется "горошиной".
# 
# Назовём очень усердным учеником того пользователя, который хотя бы раз за текущий месяц правильно решил 20 горошин.

# Дана таблица default.peas
# Необходимо написать оптимальный запрос, который даст информацию о количестве очень усердных студентов.NB! Под усердным студентом мы понимаем студента, который правильно решил 20 задач за текущий месяц.

# # Необходимо написать оптимальный запрос, который даст информацию о количестве очень усердных студентов.NB! Под усердным студентом мы понимаем студента, который правильно решил 20 задач за текущий месяц.

# In[1]:


import pandahouse as ph

#импорт библиотеки


# In[2]:


connection_default = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'default',
                      'user':'student', 
                      'password':'dpo_python_2020'}
#создаю словарь с нужными параметрами


# In[3]:


q_test = """
SELECT 
    count(*) as count
FROM 
    peas
"""

q_test = ph.read_clickhouse(query=q_test, connection=connection_default)
q_test.head()

#проверю правильность подключения к ClickHouse через pandahouse, отправив простой запрос.


# In[4]:


q = """
SELECT
    st_id, 
    toDateTime(timest) as timest,
    correct, 
    subject
FROM 
    peas
"""

q = ph.read_clickhouse(query=q, connection=connection_default)
q.head(11)

#вывожу таблицу peas, чтобы посмотреть на данные


# In[5]:


print(f'st_id:           {q.st_id.nunique()}')
print(f'time_max:        {q.timest.max()}')
print(f'time_min:        {q.timest.min()}')
print(f'unique correct:  {q.correct.unique()}')
print(f'subject:         {q.subject.unique()}')

#разведывательный анализ наполнения таблицы


# In[6]:


q_answer = """
SELECT COUNT(st_id) AS count_diligent_student
FROM 
(SELECT toStartOfMonth(timest) AS current_month, st_id, SUM(correct) AS count_pea FROM peas 
WHERE correct == 1 GROUP BY current_month, st_id) as join_tab
WHERE count_pea >= 20
"""

q_answer = ph.read_clickhouse(query=q_answer, connection=connection_default)
q_answer

#запрос с ответом на поставленную задачу


# Образовательная платформа предлагает пройти студентам курсы по модели trial: студент может решить бесплатно лишь 30 горошин в день. Для неограниченного количества заданий в определенной дисциплине студенту необходимо приобрести полный доступ. Команда провела эксперимент, где был протестирован новый экран оплаты.

# # 2. Оптимизация воронки

# Образовательная платформа предлагает пройти студентам курсы по модели trial: студент может решить бесплатно лишь 30 горошин в день. Для неограниченного количества заданий в определенной дисциплине студенту необходимо приобрести полный доступ. Команда провела эксперимент, где был протестирован новый экран оплаты.

# # Необходимо в одном запросе выгрузить следующую информацию о группах пользователей:
# 
# ARPU 
# ARPAU 
# CR в покупку 
# СR активного пользователя в покупку 
# CR пользователя из активности по математике (subject = ’math’) в покупку курса по математике
# ARPU считается относительно всех пользователей, попавших в группы.
# 
# Активным считается пользователь, за все время решивший больше 10 задач правильно в любых дисциплинах.
# 
# Активным по математике считается пользователь, за все время решивший 2 или больше задач правильно по математике.

# In[7]:


q_funnel = """
SELECT test_grp AS group,
    SUM(sum_money) / COUNT(DISTINCT st_id) AS ARPU,
    SUMIf(sum_money, sum_correct > 10) / COUNTIf(sum_correct > 10) AS ARPAU,
    COUNTIf(sum_money > 0) / COUNT(DISTINCT st_id) * 100 AS CR,
    COUNTIf(sum_correct > 10 AND sum_money > 0) / COUNTIf(sum_correct > 10) * 100 AS CR_active,
    SUMIf(count_math, sum_math >= 2) / COUNTIf(st_id, sum_math >= 2) * 100 AS CR_math
FROM (SELECT * FROM studs 
    LEFT JOIN 
    (SELECT DISTINCT st_id, SUM(correct) AS sum_correct, SUMIf(correct, subject = 'Math') AS sum_math FROM peas 
    GROUP BY st_id) AS join_3 USING st_id) AS join_2
    LEFT JOIN 
    (SELECT DISTINCT st_id, SUM(money) AS sum_money, COUNTIf(money, subject='Math') AS count_math 
    FROM final_project_check GROUP BY st_id) AS join_1 USING st_id
GROUP BY test_grp
"""

q_funnel = ph.read_clickhouse(query=q_funnel, connection=connection_default)
q_funnel

#запрос с ответом на поставленную задачу


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





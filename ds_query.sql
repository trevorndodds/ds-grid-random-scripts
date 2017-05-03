--BrokerBusyIdleMAX
SELECT     bs.time_stamp_h, b.broker_name, MAX(bs.num_busy_engines) AS max_busy_engines, MAX(bs.num_idle_engines) AS max_idle_engines, 
                      MAX(bs.num_tasks_pending) AS max_num_tasks_pending, MAX(bs.num_jobs_running) AS max_jobs_running, MAX(bs.num_total_engines) AS max_total_engines, 
                      AVG(bs.num_busy_engines) AS avg_busy_engines, AVG(bs.num_idle_engines) AS avg_idle_engines
FROM         broker_stats AS bs INNER JOIN
                      brokers AS b ON bs.broker_id = b.broker_id
WHERE     (bs.time_stamp >= @StartDate) AND (bs.time_stamp <= @EndDate)
GROUP BY bs.time_stamp_h, b.broker_name

--BrokerBusy
SELECT b.broker_name
      ,AVG([num_busy_engines]) AS avg_busy_engines
	  ,(dateadd(hour, datediff(hour, 0, time_stamp), 0)) AS time_stamp_h
  FROM [dbo].[broker_stats]  AS j WITH(NOLOCK) INNER JOIN
			dbo.brokers AS b WITH(NOLOCK) ON j.broker_id = b.broker_id
  WHERE    (time_stamp >= dateadd(day,datediff(day,1,GETDATE()),0) AND time_stamp <= dateadd(day,datediff(day,0,GETDATE()),0))
  GROUP BY  (dateadd(hour, datediff(hour, 0, time_stamp), 0)),b.broker_name

--BrokerBusyIdle
SELECT     bs.time_stamp_h, b.broker_name, AVG(bs.num_busy_engines) AS avg_busy_engines, AVG(bs.num_idle_engines) AS avg_idle_engines
FROM         broker_stats AS bs INNER JOIN
                      brokers AS b ON bs.broker_id = b.broker_id
WHERE     (bs.time_stamp >= @StartDate) AND (bs.time_stamp <= @EndDate)
GROUP BY bs.time_stamp_h, b.broker_name

--BrokerBusyPend
SELECT     bs.time_stamp_h, b.broker_name, AVG(bs.num_busy_engines) AS avg_busy_engines, AVG(bs.num_idle_engines) AS avg_idle_engines, AVG(bs.num_tasks_pending) 
                      AS avg_num_tasks_pending
FROM         broker_stats AS bs INNER JOIN
                      brokers AS b ON bs.broker_id = b.broker_id
WHERE     (bs.time_stamp >= @StartDate) AND (bs.time_stamp <= @EndDate)
GROUP BY bs.time_stamp_h, b.broker_name

--BrokerJobs
SELECT     jobs.job_id, jobs.service_type_name, t.name AS Status, jobs.start_time, jobs.end_time, DATEDIFF(second, jobs.start_time, jobs.end_time) AS Duration, 
                      jobs.num_tasks, jobs.task_time_avg, jobs.priority, jobs.driver_username, jobs.driver_hostname, jobs.gridlibrary, jobs.gridlibrary_version, jobs.app_name
FROM         jobs INNER JOIN
                      job_status_codes AS t ON jobs.job_status = t.code INNER JOIN
                      brokers AS b ON jobs.broker_id = b.broker_id
WHERE     (jobs.start_time >= @StartDate) AND (jobs.start_time <= @EndDate) AND (b.broker_name = @BrokerName)
ORDER BY jobs.start_time DESC

--UserEngineHour
SELECT     j.driver_username, j.service_type_name, SUM(j.task_time_avg * j.num_tasks / 3600) AS duration, SUM(j.num_tasks) AS "Total Tasks", b.broker_name
FROM         dbo.jobs AS j INNER JOIN
                      dbo.brokers AS b ON j.broker_id = b.broker_id
WHERE    (j.start_time >= @StartDate AND j.start_time <= @EndDate) AND j.task_time_avg IS NOT NULL
GROUP BY j.driver_username, j.service_type_name, b.broker_name
HAVING      (SUM(j.task_time_avg * j.num_tasks / 3600) IS NOT NULL)
ORDER BY duration desc

--UserJobs
SELECT     jobs.job_id, jobs.service_type_name, t.name AS Status, jobs.start_time, jobs.end_time, DATEDIFF(second, jobs.start_time, jobs.end_time) AS Duration, 
                      jobs.num_tasks, jobs.task_time_avg, jobs.priority, jobs.driver_username, jobs.driver_hostname, jobs.gridlibrary, jobs.gridlibrary_version, jobs.app_name
FROM         jobs INNER JOIN
                      job_status_codes AS t ON jobs.job_status = t.code
WHERE     (jobs.start_time >= @StartDate) AND (jobs.start_time <= @EndDate) AND (jobs.driver_username = @UserName)
ORDER BY jobs.start_time DESC

--AppNameJobs
SELECT     j.gridlibrary, j.gridlibrary_version, j.app_name, j.service_type_name, SUM(task_time_avg * num_tasks / 3600) AS duration, SUM(j.num_tasks) AS [Total Tasks], b.broker_name
FROM         jobs AS j INNER JOIN
                      brokers AS b ON j.broker_id = b.broker_id
WHERE     (j.start_time >= @StartDate) AND (j.start_time <= @EndDate)
GROUP BY j.app_name, b.broker_name, j.gridlibrary, j.gridlibrary_version, j.app_name, j.service_type_name
HAVING      (SUM(j.task_time_avg * j.num_tasks / 3600) IS NOT NULL


--TaskErrors
     ei.username AS Engine, ei.engine_id AS EngineId, ei.IP, j.service_type_name AS Service, j.job_id, sc.name AS Code, t.task_id, t.start_time, t.end_time, 
                      t.end_time_h AS Time, t.engine_instance AS EngineInstance, b.broker_name
FROM         jobs AS j INNER JOIN
                      tasks AS t ON j.job_id = t.job_id INNER JOIN
                      task_status_codes AS sc ON t.task_status = sc.code INNER JOIN
                      engine_info AS ei ON t.engine_id = ei.engine_id INNER JOIN
                      brokers AS b ON j.broker_id = b.broker_id
WHERE     (t.task_status = - 4 OR
                      t.task_status = - 2 OR
                      t.task_status = - 1) AND (t.start_time >= @StartDate) AND (t.end_time <= @EndDate)

--TaskDetails
SELECT     tasks.task_id, tasks.job_id, t.name AS Status, ei.username, tasks.engine_instance, tasks.start_time, tasks.end_time
FROM         tasks INNER JOIN
                      engine_info AS ei ON tasks.engine_id = ei.engine_id INNER JOIN
                      task_status_codes AS t ON tasks.task_status = t.code
WHERE     (tasks.job_id = @JobID)
ORDER BY tasks.task_id

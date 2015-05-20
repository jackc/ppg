select pg_sleep(1);

insert into jobs(parallel, job_number)
values({{.Parallel}}, {{.JobNumber}});

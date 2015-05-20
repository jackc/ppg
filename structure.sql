drop table if exists jobs;

create table jobs(
  id serial primary key,
  parallel int not null,
  job_number int not null
);

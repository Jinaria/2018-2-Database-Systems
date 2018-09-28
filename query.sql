use Pokemon

select t.name from Trainer as t, CatchedPokemon as c where t.id = c.owner_id group by c.owner_id having count(*) >= 3 order by count(*) desc;

select name from Pokemon where type in(select * from(select type from Pokemon group by type order by count(*) desc limit 2) as twotype) order by name;

select name from Pokemon where name like '_o%' order by name;

select nickname from CatchedPokemon where level >= 50 order by nickname;

select name from Pokemon where char_length(name) = 6 order by name;

select name from Trainer where hometown = 'Blue City' order by name;

select distinct hometown from Trainer order by hometown;

select avg(level) from CatchedPokemon, Trainer where Trainer.name = 'Red' and Trainer.id = owner_id;

select nickname from CatchedPokemon where not nickname like 'T%' order by nickname;

select nickname from CatchedPokemon where level >= 50 and owner_id >= 6 order by nickname; 

select id, name from Pokemon order by id;

select nickname from CatchedPokemon where level <= 50 order by level;

select distinct P.name, P.id from Pokemon as P, CatchedPokemon as C, Trainer as T where T.hometown = 'Sangnok City' and C.pid = P.id and T.id = C.owner_id order by P.id;

select nickname from CatchedPokemon, Gym, Pokemon where Gym.leader_id = CatchedPokemon.owner_id and CatchedPokemon.pid = Pokemon.id and Pokemon.type = 'Water' order by nickname;

select count(*) from CatchedPokemon as c, Evolution as e where c.pid = e.before_id;

select count(*) from Pokemon where type = 'Water' or type = 'Electric' or type = 'Psychic';

select count(distinct p.id) from Pokemon as p, Trainer as t, CatchedPokemon as c where c.pid = p.id and t.hometown = "Sangnok City" and t.id = c.owner_id;

select max(c.level) from CatchedPokemon as c, Trainer as t where t.hometown = 'Sangnok City';

select count(distinct p.type) from Gym as g, Pokemon as p, CatchedPokemon as c where g.city = 'Sangnok City' and g.leader_id = c.owner_id and c.pid = p.id;

select t.name, count(c.owner_id) from Trainer as t, CatchedPokemon as c where t.hometown = 'Sangnok City' and t.id = c.owner_id group by c.owner_id order by count(c.owner_id);

select name from Pokemon where name like 'a%' or name like 'e%' or name like 'i%' or name like 'o%' or name like 'u%';

select type, count(type) from Pokemon group by type order by count(type), type;

select distinct t.name from Trainer as t, CatchedPokemon as c where t.id = c.owner_id and c.level <= 10 order by t.name;

select ct.name, avg(cp.level) from City as ct, CatchedPokemon as cp, Trainer as t where t.id = cp.owner_id and ct.name = t.hometown group by ct.name order by avg(cp.level);  

select distinct p.name from (select c.pid as id from Trainer as t, CatchedPokemon as c where t.id = c.owner_id and t.hometown = 'Sangnok City') as s, Pokemon as p, CatchedPokemon as c, Trainer as t where t.hometown = 'Brown City' and t.id = c.owner_id and s.id = c.pid and c.pid = p.id order by p.name;

select p.name from CatchedPokemon as c, Pokemon as p where p.id = c.pid and c.nickname like '% %' order by name desc;

select t.name, max(c.level) from Trainer as t, CatchedPokemon as c where t.id = c.owner_id group by owner_id having count(*) >= 4 order by t.name;

select t.name, avg(c.level) from CatchedPokemon as c, Trainer as t, Pokemon as p where t.id = c.owner_id and p.id = c.pid and (p.type = 'Normal' or p.type = 'Electric') group by t.name order by avg(c.level);

select p.name, t.name, ct.description from Pokemon as p, Trainer as t, CatchedPokemon as cp, City as ct where p.id = 152 and cp.pid = p.id and cp.owner_id = t.id and t.hometown = ct.name order by cp.level desc;

select p1.id, p1.name, p2.name, p3.name from Evolution as e1, Evolution as e2, Pokemon as p1, Pokemon as p2, Pokemon as p3 where p1.id = e1.before_id and e1.after_id = e2.before_id and p2.id = e1.after_id and p3.id = e2.after_id order by p1.id;

select name from Pokemon where id between 10 and 100 order by name;

select distinct p.name from Pokemon as p left join CatchedPokemon as c on p.id = c.pid where c.owner_id = null order by p.name;

select sum(c.level) from Trainer as t, CatchedPokemon as c where t.name = "Matis" and t.id = c.owner_id;

select t.name from Trainer as t, Gym as g where t.id = g.leader_id order by t.name;

select s.type, s.c * 100 / count(p.type) from (select type, count(*) as c from Pokemon as maxp group by type order by count(*) desc limit 1) as s, Pokemon as p;

select t.name from CatchedPokemon as c, Trainer as t where c.owner_id = t.id and c.nickname like 'A%' order by t.name;

select s.name, max(s.sum) from (select t.name as name, sum(c.level) as sum from CatchedPokemon as c, Trainer as t where t.id = c.owner_id group by t.name order by sum(c.level) desc) as s;

select p.name from Pokemon as p, Evolution as e where p.id = e.after_id and e.after_id not in (select e1.after_id from Evolution as e1, Evolution as e2 where e1.before_id = e2.after_id) order by p.name;

select t.name from Trainer as t, CatchedPokemon as c where t.id = c.owner_id group by t.id having count(c.pid) > count(distinct c.pid) order by t.name;

select t.hometown, cp.nickname from Trainer as t, CatchedPokemon as cp, (select hometown, max(level) as maxlevel from Trainer as t, CatchedPokemon as c where t.id = c.owner_id group by t.hometown) as s where t.id = owner_id and t.hometown = s.hometown and cp.level = s.maxlevel order by t.hometown;
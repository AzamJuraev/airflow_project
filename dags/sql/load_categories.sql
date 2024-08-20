with cte as (
			select unnest(categories_tags_en) as categories
			from public.food_info_tmp
			group by 1)

insert into public.food_categories
select categories from cte
where categories not in (select distinct categories from public.food_categories)
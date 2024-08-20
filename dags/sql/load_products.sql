with cte as (
				select brands, code, product_name, image_url, nutrition_grades, unnest(categories_tags_en) as categories
				from public.food_info_tmp),

		cte1 as(
				select ct.brands, ct.code, ct.product_name, ct.image_url, ct.nutrition_grades, fc.category_id from cte ct
				left join public.food_categories fc on fc.category_name = ct.categories)

insert into public.food_info
select brands, code, product_name, image_url, nutrition_grades, array_agg(category_id order by category_id)
from cte1
group by brands, code, product_name, image_url, nutrition_grades


-- 1. вывести количество фильмов в каждой категории, отсортировать по убыванию.
    SELECT
        c.name, count(*) as cnt
    FROM
        film f, film_category fc, category c
    WHERE
        fc.film_id=f.film_id
        and c.category_id=fc.category_id
    GROUP BY
        c.category_id
    ORDER BY
        cnt DESC;

-- 2. вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.
    SELECT
        a.actor_id, a.first_name, a.last_name, sum(f.rental_duration) duration
    FROM
        actor a, film_actor fa, film f
    WHERE
        fa.actor_id=a.actor_id
        and f.film_id=fa.film_id
    GROUP BY
        a.actor_id
    ORDER BY
        duration DESC
    LIMIT 10;

-- 3. вывести категорию фильмов, на которую потратили больше всего денег.
    SELECT
        c.name, sum(p.amount) as amount
    FROM
        film f, film_category fc ,category c,
        inventory i, rental r, payment p
    WHERE
        fc.film_id=f.film_id
        and c.category_id=fc.category_id
        and i.film_id=f.film_id
        and r.inventory_id=i.inventory_id
        and p.rental_id=r.rental_id
    GROUP BY
        c.name
    ORDER BY 
        amount DESC
    LIMIT 1;

-- 4. вывести названия фильмов, которых нет в inventory. Написать запрос без использования оператора IN.
    --  4.1 
        SELECT
            f.title
        FROM
            film f
        WHERE
            f.film_id NOT IN (select film_id from inventory);
    --  4.2
        SELECT
            f.title
        FROM
            film f
        LEFT JOIN inventory i ON f.film_id=i.film_id
        WHERE
            i.film_id is NULL;

-- 5. вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”. Если у нескольких актеров одинаковое кол-во фильмов, вывести всех.
    -- 5.1 Без оконных функций
    WITH
        actors_list
    AS
    (
        SELECT
            a.actor_id, a.first_name, a.last_name, count(*) cnt
        FROM
            category c, film_category fc, film_actor fa, actor a
        WHERE
            c.name='Children'
            and fc.category_id=c.category_id
            and fa.film_id=fc.film_id
            and a.actor_id=fa.actor_id
        GROUP BY
            a.actor_id
    )
    SELECT
        *
    FROM
        actors_list
    WHERE
        cnt in
        (
            SELECT
                distinct cnt
            FROM
                actors_list
            ORDER BY
                cnt DESC
            LIMIT 3

        )
    ORDER BY cnt DESC, actor_id;
    -- 5.2 С оконными функциями
    SELECT
        *
    FROM
       (
           SELECT
               a.actor_id, a.first_name, a.last_name, count(*) cnt,
               dense_rank() over (order by count(*) desc) as rank
           FROM
               category c, film_category fc, film_actor fa, actor a
           WHERE
               c.name='Children'
               and fc.category_id=c.category_id
               and fa.film_id=fc.film_id
               and a.actor_id=fa.actor_id
           GROUP BY
               a.actor_id
       ) as T
    WHERE
        rank <= 3
    ORDER BY cnt DESC, actor_id;
        
-- 6. вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1). Отсортировать по количеству неактивных клиентов по убыванию.
    SELECT
        ci.city,
        sum(case when cu.active=1 then 1 else 0 end) active_users,
        sum(case when cu.active=0 then 1 else 0 end) inactive_users
    FROM
        city ci, address a, customer cu
    WHERE
        a.city_id=ci.city_id
        and cu.address_id=a.address_id
    GROUP BY
        ci.city_id
    ORDER BY
        inactive_users DESC;

-- 7. вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах (customer.address_id в этом city), и которые начинаются на букву “a”. То же самое сделать для городов в которых есть символ “-”. Написать все в одном запросе.

    WITH categories_list AS
    (
        SELECT
            c.name,
            sum(case when lower(ci.city) like 'a%' then f.rental_duration else 0 end) a_cities_amount,
            sum(case when ci.city like '%-%' then f.rental_duration else 0 end) dash_cities_amount
        FROM category c
        JOIN film_category fc on fc.category_id=c.category_id
        JOIN film f on f.film_id=fc.film_id
        JOIN inventory i on i.film_id=f.film_id
        JOIN rental r on r.inventory_id=i.inventory_id
        JOIN customer cu on cu.customer_id=r.customer_id
        JOIN address a on a.address_id=cu.address_id
        JOIN city ci on ci.city_id=a.city_id
        WHERE
            lower(ci.city) like 'a%'
            or ci.city like '%-%'
        GROUP BY
            c.category_id
    )
    (
        SELECT
            name, a_cities_amount as amount, 'cities with "a"' as details
        FROM
            categories_list
        ORDER BY
            amount DESC
        LIMIT 1
    )
    UNION
    (
        SELECT
            name, dash_cities_amount as amount, 'cities with "-"' as details
        FROM
            categories_list
        ORDER BY
            amount DESC
        LIMIT 1
    );

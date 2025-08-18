WITH raw_payments AS (  -- 0) Нормалізація типів з вихідної таблиці
    SELECT
        user_id,
        -- на випадок якщо payment_date вже date і привід до text і назад в 'YYYY-MM-DD' не зашкодить
        to_date(payment_date::text, 'YYYY-MM-DD')::date AS payment_date,
        CAST(revenue_amount_usd AS numeric)            AS revenue
    FROM project.games_payments
),

user_month_revenue AS (  -- 1) Сума revenue по кожному user_id за календарний місяць
    SELECT
        rp.user_id,
        DATE_TRUNC('month', rp.payment_date::timestamp)::date AS payment_month,
        SUM(rp.revenue)                                      AS total_revenue
    FROM raw_payments rp
    GROUP BY 1,2
),

user_month_flags AS (  -- 2) Віконні функції + календарні сусіди + рядкові метрики
    SELECT
        umr.*,

        -- сусідні платні місяці юзера (віконні)
        LAG(umr.payment_month)  OVER (PARTITION BY umr.user_id ORDER BY umr.payment_month) AS previous_paid_month,
        LAG(umr.total_revenue)  OVER (PARTITION BY umr.user_id ORDER BY umr.payment_month) AS previous_paid_month_revenue,
        LEAD(umr.payment_month) OVER (PARTITION BY umr.user_id ORDER BY umr.payment_month) AS next_paid_month,

        -- очікувані календарні сусіди
        (umr.payment_month - INTERVAL '1 month')::date AS previous_calendar_month,
        (umr.payment_month + INTERVAL '1 month')::date AS next_calendar_month,

        /* ----- Рядкові метрики (на user×month), як просив ментор ----- */

        -- Перша оплата юзера → "новий" MRR у цей місяць
        CASE
            WHEN LAG(umr.payment_month) OVER (PARTITION BY umr.user_id ORDER BY umr.payment_month) IS NULL
            THEN umr.total_revenue
        END AS new_mrr,

        -- Зростання: попередній платний місяць існує і це саме попередній календарний місяць,
        -- а сума в поточному місяці більша. Результат — позитивна дельта.
        CASE
            WHEN LAG(umr.payment_month) OVER (PARTITION BY umr.user_id ORDER BY umr.payment_month)
                   = (umr.payment_month - INTERVAL '1 month')::date
             AND umr.total_revenue > LAG(umr.total_revenue) OVER (PARTITION BY umr.user_id ORDER BY umr.payment_month)
            THEN umr.total_revenue - LAG(umr.total_revenue) OVER (PARTITION BY umr.user_id ORDER BY umr.payment_month)
        END AS expansion_revenue,

        -- Падіння: умови аналогічні, але сума менша → повертаємо ВІД’ЄМНУ дельту (як у формулі ментора)
        CASE
            WHEN LAG(umr.payment_month) OVER (PARTITION BY umr.user_id ORDER BY umr.payment_month)
                   = (umr.payment_month - INTERVAL '1 month')::date
             AND umr.total_revenue < LAG(umr.total_revenue) OVER (PARTITION BY umr.user_id ORDER BY umr.payment_month)
            THEN umr.total_revenue - LAG(umr.total_revenue) OVER (PARTITION BY umr.user_id ORDER BY umr.payment_month)
        END AS contraction_revenue,

        -- Відвалене revenue: якщо наступного платного місяця немає АБО він не дорівнює наступному календарному
        CASE
            WHEN LEAD(umr.payment_month) OVER (PARTITION BY umr.user_id ORDER BY umr.payment_month) IS NULL
              OR LEAD(umr.payment_month) OVER (PARTITION BY umr.user_id ORDER BY umr.payment_month)
                 <> (umr.payment_month + INTERVAL '1 month')::date
            THEN umr.total_revenue
        END AS churned_revenue,

        -- Прапорець відваленого юзера (для підрахунку churned_users)
        CASE
            WHEN LEAD(umr.payment_month) OVER (PARTITION BY umr.user_id ORDER BY umr.payment_month) IS NULL
              OR LEAD(umr.payment_month) OVER (PARTITION BY umr.user_id ORDER BY umr.payment_month)
                 <> (umr.payment_month + INTERVAL '1 month')::date
            THEN 1
        END AS churned_user_flag
    FROM user_month_revenue umr
),

monthly_base AS (  -- 3) Базові метрики по місяцю
    SELECT
        payment_month AS month,
        COUNT(DISTINCT user_id) AS paid_users,
        SUM(total_revenue)      AS total_revenue,
        ROUND(SUM(total_revenue) / NULLIF(COUNT(DISTINCT user_id),0), 2) AS arppu
    FROM user_month_revenue
    GROUP BY 1
),

monthly_win_agg AS (  -- 4) Агрегація рядкових метрик до місяця
    SELECT
        payment_month AS month,

        -- нові платники у місяці
        COUNT(DISTINCT CASE WHEN previous_paid_month IS NULL THEN user_id END) AS new_paid_users,

        -- суми факторів
        SUM(COALESCE(new_mrr,0))               AS new_mrr,
        SUM(COALESCE(expansion_revenue,0))     AS expansion_mrr,
        SUM(COALESCE(contraction_revenue,0))   AS contraction_mrr,   -- це сума від’ємних дельт
        SUM(COALESCE(churned_revenue,0))       AS churned_revenue,

        -- кількість відвалених юзерів
        COUNT(DISTINCT CASE WHEN churned_user_flag = 1 THEN user_id END) AS churned_users,

        -- “істинний” MRR — повторні платежі (є платіж і в попередньому календарному місяці)
        SUM(CASE
                WHEN previous_paid_month = (payment_month - INTERVAL '1 month')::date
                THEN total_revenue
            END) AS mrr
    FROM user_month_flags
    GROUP BY 1
),

churn_rates AS (  -- 5) Коефіцієнти відвалу (потрібна база попереднього місяця)
    SELECT
        mwa.month,
        ROUND(mwa.churned_users::numeric   / NULLIF(mb_prev.paid_users,0),    4) AS churn_rate,
        ROUND(mwa.churned_revenue::numeric / NULLIF(mb_prev.total_revenue,0), 4) AS revenue_churn_rate
    FROM monthly_win_agg mwa
    JOIN monthly_base mb_prev
      ON mb_prev.month = mwa.month - INTERVAL '1 month'
)

-- 6) Фінальний вивід (по місяцях) — все, що потрібно для Tableau
SELECT
    mb.month,
    mb.paid_users,
    mb.total_revenue,
    mb.arppu,
    mwa.new_paid_users,
    mwa.new_mrr,
    mwa.mrr,
    mwa.expansion_mrr,
    mwa.contraction_mrr,
    mwa.churned_users,
    mwa.churned_revenue,
    cr.churn_rate,
    cr.revenue_churn_rate
FROM monthly_base mb
LEFT JOIN monthly_win_agg mwa ON mb.month = mwa.month
LEFT JOIN churn_rates     cr  ON mb.month = cr.month
ORDER BY mb.month;

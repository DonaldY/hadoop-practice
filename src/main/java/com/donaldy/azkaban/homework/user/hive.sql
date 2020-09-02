USE default;

INSERT INTO TABLE user_info
SELECT COUNT(DISTINCT id) active_num, TO_DATE(click_time) `date`
FROM user_clicks
WHERE TO_DATE(click_time) = CURRENT_DATE
GROUP BY TO_DATE(click_time);
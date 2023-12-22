SELECT
    e1.employee_id,
    (
        SELECT
            AVG(salary)
        FROM
            (
                SELECT
                    salary
                FROM
                    employees e2
                WHERE
                    e2.manager_id = e1.manager_id
                    AND e2.department_id IN (
                        SELECT
                            department_id
                        FROM
                            departments d1
                        WHERE
                            EXISTS (
                                SELECT
                                    department_name
                                FROM
                                    departments d2
                                WHERE
                                    d1.parent_department_id = d2.department_id
                                    AND d2.location_id = (
                                        SELECT
                                            location_id
                                        FROM
                                            locations
                                        WHERE
                                            country_id = 'US'
                                    )
                            )
                    )
            ) AS DepartmentSalary
    ) AS AverageDepartmentSalary
FROM
    employees e1
WHERE
    e1.employee_id IN (
        SELECT
            employee_id
        FROM
            job_history
        WHERE
            start_date > (
                SELECT
                    MIN(start_date)
                FROM
                    job_history
                WHERE
                    department_id = (
                        SELECT
                            department_id
                        FROM
                            employees
                        WHERE
                            employee_id = e1.manager_id
                    )
            )
    )
    AND e1.salary > (
        SELECT
            AVG(salary)
        FROM
            employees
        WHERE
            department_id = e1.department_id
    )
ORDER BY
    e1.employee_id;